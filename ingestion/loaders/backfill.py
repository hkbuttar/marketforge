"""Immutable historical backfill from a bounded source extract to Parquet."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Iterable, Mapping
from typing import Callable

import duckdb

from ingestion.contracts import CONTRACTS
from ingestion.contracts.base import (
    finite_float,
    integer,
    iso_date,
    text,
    upper_text,
    utc_datetime,
    write_quarantine,
)


EVENT_FIELDS = {
    "prices": "date",
    "fundamentals": "period_end",
    "earnings": "event_timestamp",
    "macro": "observation_date",
    "news": "event_timestamp",
}
DUCKDB_TYPES = {
    text: "VARCHAR",
    upper_text: "VARCHAR",
    finite_float: "DOUBLE",
    integer: "BIGINT",
    iso_date: "DATE",
    utc_datetime: "TIMESTAMPTZ",
}


@dataclass(frozen=True)
class BackfillResult:
    run_id: str
    dataset: str
    status: str
    input_rows: int
    accepted_rows: int
    quarantined_rows: int
    duplicate_rows: int
    files_written: int
    input_bytes: int
    output_bytes: int
    wall_clock_seconds: float
    started_at: str
    completed_at: str
    max_event_date: str | None
    run_type: str
    requested_start: str | None
    requested_end: str | None
    late_arriving_rows: int = 0
    earliest_late_event_date: str | None = None
    arrival_time: str | None = None
    prior_event_watermark: str | None = None
    contract_version: int = 1


class IdempotencyConflictError(RuntimeError):
    """An existing logical key was replayed with different canonical values."""


FailureHook = Callable[[str, Path], None]
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _stable_record_id(row: Mapping[str, Any]) -> str:
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _size(paths: Iterable[Path]) -> int:
    return sum(path.stat().st_size for path in paths if path.is_file())


def _existing_rows(dataset_root: Path, contract) -> dict[tuple[Any, ...], tuple[Any, ...]]:
    files = list(dataset_root.glob("year=*/month=*/*.parquet"))
    if not files:
        return {}
    value_fields = tuple(
        field for field in contract.fields if field not in {"ingested_at", "source_record_id"}
        or field in contract.idempotency_by
    )
    selected = tuple(dict.fromkeys((*contract.idempotency_by, *value_fields)))
    quoted = ", ".join(f'"{field}"' for field in selected)
    with duckdb.connect() as connection:
        placeholders = ", ".join("?" for _ in files)
        query = f"SELECT {quoted} FROM read_parquet([{placeholders}])"
        rows = connection.execute(query, [str(path) for path in files]).fetchall()
    positions = {field: index for index, field in enumerate(selected)}
    existing = {}
    for row in rows:
        key = tuple(row[positions[field]] for field in contract.idempotency_by)
        values = tuple(row[positions[field]] for field in value_fields)
        if key in existing and existing[key] != values:
            raise IdempotencyConflictError(f"retained data already conflicts for key {key!r}")
        existing[key] = values
    return existing


def _validate_parquet(contract, rows: list[dict[str, Any]], target: Path) -> None:
    with duckdb.connect() as connection:
        description = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(target)]
        ).fetchall()
        actual_columns = [column[0] for column in description]
        row_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(target)]
        ).fetchone()[0]
    if actual_columns != list(contract.fields):
        raise ValueError(f"staged Parquet schema mismatch: {actual_columns!r}")
    if row_count != len(rows):
        raise ValueError(f"staged Parquet row count {row_count} != expected {len(rows)}")


def _stage_partition(
    contract,
    rows: list[dict[str, Any]],
    staging_target: Path,
    final_target: Path,
    failure_hook: FailureHook | None,
) -> None:
    staging_target.parent.mkdir(parents=True, exist_ok=True)
    if final_target.exists():
        raise FileExistsError(f"immutable raw artifact already exists: {final_target}")
    writing_target = staging_target.with_suffix(".writing")
    # A .writing file is never canonical and may be left by a killed writer.
    if writing_target.exists():
        writing_target.unlink()
    if staging_target.exists():
        staging_target.unlink()
    if failure_hook:
        failure_hook("before_temp_write", writing_target)
    columns = list(contract.fields)
    definitions = ", ".join(
        f'"{name}" {DUCKDB_TYPES.get(spec.normalizer, "VARCHAR")}'
        for name, spec in contract.fields.items()
    )
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    escaped_target = str(writing_target).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(f"CREATE TEMP TABLE payload ({definitions})")
        connection.executemany(
            f"INSERT INTO payload ({quoted_columns}) VALUES ({placeholders})",
            [[row[column] for column in columns] for row in rows],
        )
        connection.execute(
            f"COPY (SELECT {quoted_columns} FROM payload) TO '{escaped_target}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    if failure_hook:
        failure_hook("after_temp_write", writing_target)
    _validate_parquet(contract, rows, writing_target)
    os.replace(writing_target, staging_target)
    if failure_hook:
        failure_hook("after_temp_validation", staging_target)


def _promote_partition(staging_target: Path, final_target: Path, failure_hook: FailureHook | None) -> None:
    final_target.parent.mkdir(parents=True, exist_ok=True)
    if final_target.exists():
        raise FileExistsError(f"immutable raw artifact already exists: {final_target}")
    os.replace(staging_target, final_target)
    if failure_hook:
        failure_hook("after_final_promotion", final_target)


def _write_manifest(result: BackfillResult, metadata_root: Path) -> Path:
    metadata_root.mkdir(parents=True, exist_ok=True)
    target = metadata_root / f"{result.run_id}.json"
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(asdict(result), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)
    return target


def run_backfill(
    dataset: str,
    records: Iterable[Mapping[str, Any]],
    *,
    source: str,
    raw_root: Path = Path("data/raw"),
    quarantine_root: Path = Path("data/quarantine"),
    metadata_root: Path = Path("warehouse/metadata/ingestion_runs"),
    run_id: str | None = None,
    now: datetime | None = None,
    input_bytes: int = 0,
    failure_hook: FailureHook | None = None,
    run_type: str = "historical_backfill",
    requested_start: date | None = None,
    requested_end: date | None = None,
    late_event_cutoff: date | None = None,
) -> BackfillResult:
    if dataset not in CONTRACTS:
        raise ValueError(f"unknown dataset {dataset!r}; choose from {sorted(CONTRACTS)}")
    contract = CONTRACTS[dataset]
    run_id = run_id or str(uuid.uuid4())
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("run_id must contain only letters, numbers, dot, underscore, or hyphen")
    if (
        requested_start and requested_end and requested_end < requested_start
        and run_type != "incremental"
    ):
        raise ValueError("requested_end must not precede requested_start")
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    started_at = now.astimezone(timezone.utc)
    start = monotonic()
    supplied = [dict(row) for row in records]
    enriched = []
    for row in supplied:
        row.setdefault("source", source)
        row.setdefault("source_record_id", _stable_record_id(row))
        row.setdefault("ingested_at", started_at.isoformat())
        enriched.append(row)

    validation = contract.validate(
        enriched, source=source, ingestion_run_id=run_id, received_at=started_at
    )
    write_quarantine(validation.rejected, quarantine_root)
    existing = _existing_rows(raw_root / dataset, contract)
    value_fields = tuple(
        field for field in contract.fields if field not in {"ingested_at", "source_record_id"}
        or field in contract.idempotency_by
    )
    new_rows = []
    duplicate_rows = 0
    for row in validation.accepted:
        key = tuple(row[field] for field in contract.idempotency_by)
        values = tuple(row[field] for field in value_fields)
        if key not in existing:
            new_rows.append(row)
            existing[key] = values
        elif existing[key] == values:
            duplicate_rows += 1
        else:
            raise IdempotencyConflictError(
                f"{dataset} replay changed canonical values for idempotency key {key!r}"
            )
    groups: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in new_rows:
        event_value = row[EVENT_FIELDS[dataset]]
        event_date = event_value.date() if isinstance(event_value, datetime) else event_value
        groups[(event_date.year, event_date.month)].append(row)

    staged: list[tuple[Path, Path]] = []
    for (year, month), partition_rows in sorted(groups.items()):
        final_target = (
            raw_root
            / dataset
            / f"year={year:04d}"
            / f"month={month:02d}"
            / f"part-{run_id}.parquet"
        )
        staging_target = (
            raw_root / ".tmp" / run_id / dataset / f"year={year:04d}" / f"month={month:02d}"
            / f"part-{run_id}.parquet"
        )
        _stage_partition(contract, partition_rows, staging_target, final_target, failure_hook)
        staged.append((staging_target, final_target))

    written: list[Path] = []
    for staging_target, final_target in staged:
        _promote_partition(staging_target, final_target, failure_hook)
        written.append(final_target)

    if failure_hook:
        failure_hook("before_manifest", metadata_root / f"{run_id}.json")

    completed_at = datetime.now(timezone.utc)
    accepted_event_dates = []
    for row in validation.accepted:
        value = row[EVENT_FIELDS[dataset]]
        accepted_event_dates.append(value.date() if isinstance(value, datetime) else value)
    new_event_dates = []
    for row in new_rows:
        value = row[EVENT_FIELDS[dataset]]
        new_event_dates.append(value.date() if isinstance(value, datetime) else value)
    late_event_dates = (
        [value for value in new_event_dates if value <= late_event_cutoff]
        if late_event_cutoff else []
    )
    result = BackfillResult(
        run_id=run_id,
        dataset=dataset,
        status="degraded" if validation.rejected else "success",
        input_rows=len(supplied),
        accepted_rows=len(new_rows),
        quarantined_rows=len(validation.rejected),
        duplicate_rows=duplicate_rows,
        files_written=len(written),
        input_bytes=input_bytes,
        output_bytes=_size(written),
        wall_clock_seconds=round(monotonic() - start, 6),
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        max_event_date=max(accepted_event_dates).isoformat() if accepted_event_dates else None,
        run_type=run_type,
        requested_start=requested_start.isoformat() if requested_start else None,
        requested_end=requested_end.isoformat() if requested_end else None,
        late_arriving_rows=len(late_event_dates),
        earliest_late_event_date=(min(late_event_dates).isoformat() if late_event_dates else None),
        arrival_time=started_at.isoformat(),
        prior_event_watermark=late_event_cutoff.isoformat() if late_event_cutoff else None,
        contract_version=contract.version,
    )
    _write_manifest(result, metadata_root)
    return result
