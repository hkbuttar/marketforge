"""Immutable historical backfill from a bounded source extract to Parquet."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Iterable, Mapping

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


def _stable_record_id(row: Mapping[str, Any]) -> str:
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _size(paths: Iterable[Path]) -> int:
    return sum(path.stat().st_size for path in paths if path.is_file())


def _existing_ids(dataset_root: Path) -> set[tuple[str, str]]:
    files = list(dataset_root.glob("year=*/month=*/*.parquet"))
    if not files:
        return set()
    with duckdb.connect() as connection:
        placeholders = ", ".join("?" for _ in files)
        query = f"SELECT DISTINCT source, source_record_id FROM read_parquet([{placeholders}])"
        return set(connection.execute(query, [str(path) for path in files]).fetchall())


def _write_partition(contract, rows: list[dict[str, Any]], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"immutable raw artifact already exists: {target}")
    columns = list(contract.fields)
    definitions = ", ".join(
        f'"{name}" {DUCKDB_TYPES.get(spec.normalizer, "VARCHAR")}'
        for name, spec in contract.fields.items()
    )
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    escaped_target = str(target).replace("'", "''")
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
) -> BackfillResult:
    if dataset not in CONTRACTS:
        raise ValueError(f"unknown dataset {dataset!r}; choose from {sorted(CONTRACTS)}")
    contract = CONTRACTS[dataset]
    run_id = run_id or str(uuid.uuid4())
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
    existing = _existing_ids(raw_root / dataset)
    new_rows = [
        row for row in validation.accepted if (row["source"], row["source_record_id"]) not in existing
    ]
    duplicate_rows = len(validation.accepted) - len(new_rows)
    groups: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in new_rows:
        event_value = row[EVENT_FIELDS[dataset]]
        event_date = event_value.date() if isinstance(event_value, datetime) else event_value
        groups[(event_date.year, event_date.month)].append(row)

    written: list[Path] = []
    for (year, month), partition_rows in sorted(groups.items()):
        target = (
            raw_root
            / dataset
            / f"year={year:04d}"
            / f"month={month:02d}"
            / f"part-{run_id}.parquet"
        )
        _write_partition(contract, partition_rows, target)
        written.append(target)

    completed_at = datetime.now(timezone.utc)
    result = BackfillResult(
        run_id=run_id,
        dataset=dataset,
        status="success",
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
    )
    _write_manifest(result, metadata_root)
    return result
