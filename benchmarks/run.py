#!/usr/bin/env python3
"""Run the complete MarketForge benchmark suite and persist portable reports."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import statistics
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from benchmarks.incremental_vs_full import benchmark_incremental_vs_full
from benchmarks.storage_efficiency import benchmark_storage
from ingestion.compaction import compact_partition


def _bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.glob("**/*") if path.is_file()) if root.exists() else 0


def _measure(connection: duckdb.DuckDBPyConnection, sql: str, iterations: int) -> dict[str, Any]:
    connection.execute(sql).fetchall()  # warm DuckDB and the filesystem cache
    samples = []
    result = []
    for _ in range(iterations):
        started = time.perf_counter()
        result = connection.execute(sql).fetchall()
        samples.append((time.perf_counter() - started) * 1000)
    return {"median_ms": round(statistics.median(samples), 3), "result_rows": len(result)}


def benchmark_queries(raw_root: Path, iterations: int) -> dict[str, Any]:
    pattern = str(raw_root / "prices/year=*/month=*/*.parquet").replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TEMP VIEW prices AS SELECT * FROM "
            f"read_parquet('{pattern}', hive_partitioning=false, union_by_name=true)"
        )
        symbol, latest = connection.execute("SELECT min(symbol), max(date)::DATE FROM prices").fetchone()
        safe_symbol = symbol.replace("'", "''")
        queries = {
            "security_lookup": (
                f"SELECT * FROM prices WHERE symbol='{safe_symbol}' AND date=DATE '{latest}'"
            ),
            "one_year_history": (
                f"SELECT * FROM prices WHERE symbol='{safe_symbol}' "
                f"AND date > DATE '{latest}' - INTERVAL 1 YEAR AND date <= DATE '{latest}' ORDER BY date"
            ),
            # The bounded price lake has no sector dimension yet. A stable symbol hash
            # supplies representative cross-sectional groups without changing source data.
            "sector_aggregate": (
                "SELECT hash(symbol) % 11 sector_bucket, avg(close), sum(volume) FROM prices "
                f"WHERE date=DATE '{latest}' GROUP BY sector_bucket"
            ),
            "full_history_aggregate": (
                "SELECT symbol, avg(close), min(low), max(high), sum(volume) FROM prices GROUP BY symbol"
            ),
        }
        return {
            "iterations": iterations,
            "query_symbol": symbol,
            "query_date": latest.isoformat(),
            "sector_basis": "stable symbol hash (11 representative buckets)",
            **{name: _measure(connection, sql, iterations) for name, sql in queries.items()},
        }


def benchmark_compaction(raw_root: Path, work_root: Path) -> dict[str, Any]:
    candidates = []
    for partition in (raw_root / "prices").glob("year=*/month=*"):
        files = list(partition.glob("*.parquet"))
        if len(files) >= 2:
            candidates.append((len(files), partition, files))
    if not candidates:
        return {"status": "skipped", "reason": "no price partition contains at least two files"}
    _, partition, files = max(candidates, key=lambda item: item[0])
    year = int(partition.parent.name.split("=", 1)[1])
    month = int(partition.name.split("=", 1)[1])
    isolated = work_root / "compaction/raw/prices" / partition.parent.name / partition.name
    isolated.mkdir(parents=True)
    for source in files:
        shutil.copy2(source, isolated / source.name)
    result = compact_partition(
        "prices", year, month,
        raw_root=work_root / "compaction/raw",
        archive_root=work_root / "compaction/archive",
        metadata_root=work_root / "compaction/metadata",
        compaction_id="benchmark",
    )
    return {
        "status": result.status,
        "partition": result.partition,
        "file_count_before": result.files_before,
        "file_count_after": result.files_after,
        "latency_before_ms": result.query_latency_before_ms,
        "latency_after_ms": result.query_latency_after_ms,
        "bytes_before": result.bytes_before,
        "bytes_after": result.bytes_after,
    }


def run_suite(raw_root: Path, iterations: int = 3) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    files = sorted((raw_root / "prices").glob("year=*/month=*/*.parquet"))
    if not files:
        raise ValueError(f"no price Parquet files found below {raw_root}")
    pattern = str(raw_root / "prices/year=*/month=*/*.parquet")
    with duckdb.connect() as connection:
        rows = connection.execute(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning=false, union_by_name=true)", [pattern]
        ).fetchone()[0]
    with tempfile.TemporaryDirectory(prefix="marketforge-suite-") as directory:
        work_root = Path(directory)
        storage = benchmark_storage(raw_root, work_root / "storage", iterations)
        refresh = benchmark_incremental_vs_full(raw_root, work_root / "refresh")
        queries = benchmark_queries(raw_root, iterations)
        compaction = benchmark_compaction(raw_root, work_root)
    zstd = storage["formats"]["parquet_zstd"]
    return {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "historical_rows": rows,
        "disk_footprint_bytes": _bytes(raw_root),
        "parquet_compression_ratio": zstd["size_ratio_to_csv"],
        "storage": storage,
        "full_refresh": refresh["full_refresh"],
        "incremental": refresh["daily_incremental"],
        "incremental_comparison": {
            key: refresh[key] for key in (
                "canonical_outputs_match", "incremental_row_percent", "runtime_speedup",
                "write_reduction_percent",
            )
        },
        "queries": queries,
        "compaction": compaction,
    }


def _flatten(value: Any, prefix: str = ""):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _flatten(child, f"{prefix}.{key}" if prefix else key)
    elif isinstance(value, list):
        yield prefix, json.dumps(value, separators=(",", ":"))
    else:
        yield prefix, value


def markdown_report(result: dict[str, Any]) -> str:
    query_rows = "\n".join(
        f"| {name.replace('_', ' ')} | {metrics['median_ms']:.3f} | {metrics['result_rows']} |"
        for name, metrics in result["queries"].items() if isinstance(metrics, dict)
    )
    compact = result["compaction"]
    if compact["status"] == "skipped":
        compact_rows = f"| status | skipped ({compact['reason']}) |"
    else:
        compact_rows = "\n".join((
            f"| files | {compact['file_count_before']} → {compact['file_count_after']} |",
            f"| median latency | {compact['latency_before_ms']:.3f} → {compact['latency_after_ms']:.3f} ms |",
        ))
    return f"""# MarketForge benchmark report

Generated: {result['created_at']}

| Dataset summary | Value |
| --- | ---: |
| Historical rows | {result['historical_rows']:,} |
| Disk footprint | {result['disk_footprint_bytes']:,} bytes |
| Parquet ZSTD / CSV ratio | {result['parquet_compression_ratio']:.4f} |

| Pipeline | Runtime (s) | Peak RAM (bytes) | Bytes read | Bytes written | Rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full refresh | {result['full_refresh']['wall_clock_seconds']:.6f} | {result['full_refresh']['peak_ram_bytes']:,} | {result['full_refresh']['bytes_read']:,} | {result['full_refresh']['bytes_written']:,} | {result['full_refresh']['rows_processed']:,} |
| Incremental | {result['incremental']['wall_clock_seconds']:.6f} | {result['incremental']['peak_ram_bytes']:,} | {result['incremental']['bytes_read']:,} | {result['incremental']['bytes_written']:,} | {result['incremental']['rows_processed']:,} |

| Query | Median latency (ms) | Result rows |
| --- | ---: | ---: |
{query_rows}

Sector aggregation uses {result['queries']['sector_basis']} because the current price lake has no sector dimension.

| Compaction | Before → after |
| --- | ---: |
{compact_rows}
"""


def write_reports(result: dict[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "json": output_dir / "latest.json",
        "csv": output_dir / "latest.csv",
        "markdown": output_dir / "latest.md",
    }
    paths["json"].write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    with paths["csv"].open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("metric", "value"))
        writer.writerows(_flatten(result))
    paths["markdown"].write_text(markdown_report(result), encoding="utf-8")
    return {kind: str(path) for kind, path in paths.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=Path("benchmarks/results"))
    args = parser.parse_args()
    result = run_suite(args.raw_root, args.iterations)
    reports = write_reports(result, args.output_dir)
    print(markdown_report(result))
    print("Reports:", json.dumps(reports, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
