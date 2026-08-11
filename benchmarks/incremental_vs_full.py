#!/usr/bin/env python3
"""Benchmark one daily incremental ingestion against a full historical rebuild."""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_backfill, run_incremental
from ingestion.sources.files import read_records


def _size(root: Path) -> int:
    return sum(path.stat().st_size for path in root.glob("**/*") if path.is_file()) if root.exists() else 0


def _rss_bytes(value: int) -> int:
    return value if sys.platform == "darwin" else value * 1024


def _worker(mode: str, input_path: Path, root: Path, through: date) -> dict[str, Any]:
    usage_before = resource.getrusage(resource.RUSAGE_SELF)
    output_before = _size(root)
    started = time.perf_counter()
    records = read_records(str(input_path), "jsonl")
    options = {
        "source": "tiingo", "raw_root": root / "raw",
        "quarantine_root": root / "quarantine", "metadata_root": root / "metadata/runs",
    }
    if mode == "full":
        result = run_backfill("prices", records, run_id="full-refresh", **options)
    elif mode == "incremental":
        result = run_incremental(
            "prices", records, checkpoint_store=CheckpointStore(root / "metadata/checkpoints.sqlite"),
            through=through, run_id="daily-incremental", **options,
        ).backfill
    else:
        raise ValueError(mode)
    elapsed = time.perf_counter() - started
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "mode": mode,
        "wall_clock_seconds": round(elapsed, 6),
        "cpu_user_seconds": round(max(0, usage.ru_utime - usage_before.ru_utime), 6),
        "cpu_system_seconds": round(max(0, usage.ru_stime - usage_before.ru_stime), 6),
        "peak_ram_bytes": _rss_bytes(usage.ru_maxrss),
        "rows_processed": result.input_rows,
        "bytes_read": input_path.stat().st_size,
        "bytes_written": max(0, _size(root) - output_before),
        "files_written": result.files_written,
    }


def _run_worker(mode: str, input_path: Path, root: Path, through: date) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, "-m", "benchmarks.incremental_vs_full", "--worker", mode,
         "--input", str(input_path), "--root", str(root), "--through", through.isoformat()],
        text=True, capture_output=True, check=False,
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr or completed.stdout)
    return json.loads(completed.stdout)


def _fingerprint(root: Path) -> tuple:
    pattern = str(root / "raw/prices/**/*.parquet")
    with duckdb.connect() as connection:
        return connection.execute(
            "SELECT count(*), sum(hash(symbol, date, open, high, low, close, volume, source)), "
            "sum(volume), min(date), max(date) "
            "FROM read_parquet(?, hive_partitioning=false)", [pattern]
        ).fetchone()


def benchmark_incremental_vs_full(raw_root: Path, work_root: Path) -> dict[str, Any]:
    work_root.mkdir(parents=True, exist_ok=True)
    files = sorted((raw_root / "prices").glob("year=*/month=*/*.parquet"))
    if not files:
        raise ValueError("no price Parquet files found")
    source = str(raw_root / "prices/year=*/month=*/*.parquet").replace("'", "''")
    full_input = work_root / "full.jsonl"
    latest_input = work_root / "latest.jsonl"
    baseline_input = work_root / "baseline.jsonl"
    with duckdb.connect() as connection:
        latest = connection.execute(
            f"SELECT max(date)::DATE FROM read_parquet('{source}', hive_partitioning=false) WHERE source='tiingo'"
        ).fetchone()[0]
        if latest is None:
            raise ValueError("no Tiingo price rows found")
        for target, predicate in (
            (full_input, "true"), (latest_input, f"date::DATE=DATE '{latest}'"),
            (baseline_input, f"date::DATE<DATE '{latest}'"),
        ):
            escaped = str(target).replace("'", "''")
            connection.execute(
                f"COPY (SELECT * FROM read_parquet('{source}', hive_partitioning=false) "
                f"WHERE source='tiingo' AND {predicate}) TO '{escaped}' (FORMAT JSON, ARRAY false)"
            )
        baseline_through = connection.execute(
            f"SELECT max(date)::DATE FROM read_parquet('{source}', hive_partitioning=false) "
            f"WHERE source='tiingo' AND date::DATE<DATE '{latest}'"
        ).fetchone()[0]

    full_root = work_root / "full"
    incremental_root = work_root / "incremental"
    seed_records = read_records(str(baseline_input), "jsonl")
    run_backfill(
        "prices", seed_records, source="tiingo", raw_root=incremental_root / "raw",
        quarantine_root=incremental_root / "quarantine",
        metadata_root=incremental_root / "metadata/runs", run_id="incremental-baseline",
    )
    CheckpointStore(incremental_root / "metadata/checkpoints.sqlite").advance(
        "prices", "tiingo", baseline_through, "incremental-baseline"
    )
    full = _run_worker("full", full_input, full_root, latest)
    incremental = _run_worker("incremental", latest_input, incremental_root, latest)
    fingerprints_match = _fingerprint(full_root) == _fingerprint(incremental_root)
    if not fingerprints_match:
        raise RuntimeError("incremental and full-refresh outputs differ")
    row_fraction = incremental["rows_processed"] / full["rows_processed"]
    return {
        "dataset": "prices", "source": "tiingo", "through": latest.isoformat(),
        "canonical_outputs_match": True,
        "full_refresh": full, "daily_incremental": incremental,
        "incremental_row_percent": round(row_fraction * 100, 4),
        "runtime_speedup": round(full["wall_clock_seconds"] / incremental["wall_clock_seconds"], 2),
        "write_reduction_percent": round((1 - incremental["bytes_written"] / full["bytes_written"]) * 100, 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--results", type=Path)
    parser.add_argument("--worker", choices=("full", "incremental"), help=argparse.SUPPRESS)
    parser.add_argument("--input", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--through", type=date.fromisoformat, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(_worker(args.worker, args.input, args.root, args.through)))
        return 0
    with tempfile.TemporaryDirectory(prefix="marketforge-refresh-") as directory:
        result = benchmark_incremental_vs_full(args.raw_root, Path(directory))
    output = json.dumps(result, indent=2)
    if args.results:
        args.results.parent.mkdir(parents=True, exist_ok=True)
        args.results.write_text(output + "\n")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
