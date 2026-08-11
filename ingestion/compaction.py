"""Validated whole-partition compaction with recoverable directory promotion."""

from __future__ import annotations

import json
import os
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

from ingestion.contracts import CONTRACTS


class CompactionError(RuntimeError):
    pass


@dataclass(frozen=True)
class CompactionResult:
    compaction_id: str
    dataset: str
    partition: str
    status: str
    files_before: int
    files_after: int
    rows_before: int
    rows_after: int
    duplicate_rows_removed: int
    bytes_before: int
    bytes_after: int
    query_latency_before_ms: float
    query_latency_after_ms: float
    archive: str
    completed_at: str


def _latency(pattern: str, iterations: int = 5) -> float:
    samples = []
    with duckdb.connect() as connection:
        for _ in range(iterations):
            started = time.perf_counter()
            connection.execute("SELECT count(*) FROM read_parquet(?, union_by_name=true, hive_partitioning=false)", [pattern]).fetchone()
            samples.append((time.perf_counter() - started) * 1000)
    return round(statistics.median(samples), 3)


def compact_partition(
    dataset: str, year: int, month: int, *, raw_root: Path = Path("data/raw"),
    archive_root: Path = Path("data/archive/compaction"),
    metadata_root: Path = Path("warehouse/metadata/compactions"),
    min_files: int = 2, max_file_bytes: int = 16_000_000,
    compaction_id: str | None = None,
    failure_hook: Callable[[str, Path], None] | None = None,
) -> CompactionResult:
    if dataset not in CONTRACTS:
        raise ValueError(f"unknown dataset {dataset!r}")
    if year < 1900 or not 1 <= month <= 12 or min_files < 2 or max_file_bytes < 1:
        raise ValueError("invalid compaction bounds")
    compaction_id = compaction_id or str(uuid.uuid4())
    partition = raw_root / dataset / f"year={year:04d}" / f"month={month:02d}"
    files = sorted(partition.glob("*.parquet"))
    if len(files) < min_files or sum(path.stat().st_size <= max_file_bytes for path in files) < min_files:
        raise CompactionError("partition does not meet the configured small-file threshold")
    lock = partition.with_name(partition.name + ".compaction.lock")
    lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise CompactionError(f"partition is already locked: {partition}") from exc
    os.write(descriptor, compaction_id.encode())
    os.close(descriptor)
    staged_partition = raw_root / ".tmp" / f"compact-{compaction_id}" / dataset / f"year={year:04d}" / f"month={month:02d}"
    archive = archive_root / dataset / f"year={year:04d}" / f"month={month:02d}" / compaction_id
    moved_to_archive = False
    try:
        pattern = str(partition / "*.parquet")
        latency_before = _latency(pattern)
        bytes_before = sum(path.stat().st_size for path in files)
        contract = CONTRACTS[dataset]
        columns = list(contract.fields)
        quoted = ", ".join(f'"{column}"' for column in columns)
        with duckdb.connect() as connection:
            description = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true, hive_partitioning=false)", [pattern]
            ).fetchall()
            if [row[0] for row in description] != columns:
                raise CompactionError("source partition does not match the canonical contract schema")
            rows_before = connection.execute("SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [pattern]).fetchone()[0]
            key = ", ".join(f'"{name}"' for name in contract.idempotency_by)
            unique_before = connection.execute(
                f"SELECT count(*) FROM (SELECT {key} FROM read_parquet(?, hive_partitioning=false) GROUP BY {key})",
                [pattern],
            ).fetchone()[0]
            value_fields = [name for name in columns if name not in {"ingested_at", "source_record_id"}]
            values = ", ".join(f'"{name}"' for name in value_fields)
            conflicts = connection.execute(f"""SELECT count(*) FROM (
                SELECT {key} FROM read_parquet(?, hive_partitioning=false) GROUP BY {key}
                HAVING count(DISTINCT hash({values})) > 1)""", [pattern]).fetchone()[0]
            if conflicts:
                raise CompactionError(f"{conflicts} idempotency keys contain conflicting canonical values")
            staged_partition.mkdir(parents=True, exist_ok=False)
            target = staged_partition / f"compact-{compaction_id}.parquet"
            escaped = str(target).replace("'", "''")
            connection.execute(f"""COPY (
                SELECT {quoted} FROM (
                    SELECT {quoted}, row_number() OVER (
                        PARTITION BY {key} ORDER BY ingested_at DESC, source_record_id DESC
                    ) record_rank FROM read_parquet(?, union_by_name=true, hive_partitioning=false)
                ) WHERE record_rank = 1
            ) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)""", [pattern])
        with duckdb.connect() as connection:
            after_description = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(target)]
            ).fetchall()
            rows_after = connection.execute(
                "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(target)]
            ).fetchone()[0]
        if [row[0] for row in after_description] != columns:
            raise CompactionError("compacted schema differs from the canonical contract")
        if rows_after != unique_before:
            raise CompactionError("compacted row-count equivalence failed")
        if failure_hook:
            failure_hook("after_validation_before_swap", target)
        archive.parent.mkdir(parents=True, exist_ok=True)
        os.replace(partition, archive)
        moved_to_archive = True
        if failure_hook:
            failure_hook("after_archive_before_promotion", archive)
        try:
            os.replace(staged_partition, partition)
        except BaseException:
            os.replace(archive, partition)
            moved_to_archive = False
            raise
        latency_after = _latency(str(partition / "*.parquet"))
        final = next(partition.glob("*.parquet"))
        result = CompactionResult(
            compaction_id, dataset, f"year={year:04d}/month={month:02d}", "success",
            len(files), 1, rows_before, rows_after, rows_before - rows_after,
            bytes_before, final.stat().st_size, latency_before, latency_after,
            str(archive), datetime.now(timezone.utc).isoformat(),
        )
        metadata_root.mkdir(parents=True, exist_ok=True)
        metadata = metadata_root / f"{compaction_id}.json"
        temporary = metadata.with_suffix(".tmp")
        temporary.write_text(json.dumps(asdict(result), indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, metadata)
        return result
    except BaseException:
        if moved_to_archive and not partition.exists() and archive.exists():
            os.replace(archive, partition)
        raise
    finally:
        lock.unlink(missing_ok=True)
