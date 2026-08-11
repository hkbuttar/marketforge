#!/usr/bin/env python3
"""Benchmark three Parquet layouts against representative price workloads."""

from __future__ import annotations

import argparse
import json
import re
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb


LAYOUTS = {
    "single_file": None,
    "year_month": '"year", "month"',
    "year_month_symbol": '"year", "month", symbol',
}


def _size(root: Path) -> int:
    return sum(path.stat().st_size for path in root.glob("**/*.parquet"))


def _scan_count(plan: str, fallback: int) -> int:
    match = re.search(r"Scanning Files:\s*(\d+)/(\d+)", plan)
    if match:
        return int(match.group(1))
    match = re.search(r"Total Files Read:\s*(\d+)", plan)
    return int(match.group(1)) if match else fallback


def _measure(connection, query: str, iterations: int, total_files: int) -> dict[str, Any]:
    connection.execute(query).fetchall()
    samples = []
    for _ in range(iterations):
        started = time.perf_counter()
        result = connection.execute(query).fetchall()
        samples.append((time.perf_counter() - started) * 1000)
    plan = connection.execute(f"EXPLAIN ANALYZE {query}").fetchone()[1]
    return {
        "median_ms": round(statistics.median(samples), 3),
        "result_rows": len(result),
        "files_scanned": _scan_count(plan, total_files),
        "files_total": total_files,
    }


def benchmark_layouts(raw_root: Path, output_root: Path, iterations: int = 10) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be positive")
    source_files = sorted((raw_root / "prices").glob("year=*/month=*/*.parquet"))
    if not source_files:
        raise ValueError("no price Parquet files found")
    source_pattern = str(raw_root / "prices/year=*/month=*/*.parquet").replace("'", "''")
    output_root.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as connection:
        connection.execute(
            'CREATE TEMP TABLE bars AS SELECT *, year(date)::INTEGER AS "year", '
            f'month(date)::INTEGER AS "month" FROM read_parquet(\'{source_pattern}\', '
            "hive_partitioning=false, union_by_name=true)"
        )
        rows = connection.execute("SELECT count(*) FROM bars").fetchone()[0]
        latest, symbol = connection.execute(
            "SELECT max(date)::DATE, min(symbol) FROM bars"
        ).fetchone()
        year, month = latest.year, latest.month
        results = {}
        for name, partition_by in LAYOUTS.items():
            target = output_root / name
            target.mkdir()
            started = time.perf_counter()
            if partition_by is None:
                file = target / "prices.parquet"
                connection.execute(
                    f"COPY bars TO '{file}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                relation = f"read_parquet('{file}', hive_partitioning=false)"
            else:
                connection.execute(
                    f"COPY bars TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, "
                    f"PARTITION_BY ({partition_by}))"
                )
                relation = f"read_parquet('{target}/**/*.parquet', hive_partitioning=true)"
            write_ms = (time.perf_counter() - started) * 1000
            files = list(target.glob("**/*.parquet"))
            safe_symbol = symbol.replace("'", "''")
            queries = {
                "one_day_all_securities": (
                    f'SELECT symbol, close FROM {relation} WHERE "year"={year} '
                    f'AND "month"={month} AND date=DATE \'{latest}\''
                ),
                "one_month_one_security": (
                    f'SELECT * FROM {relation} WHERE "year"={year} AND "month"={month} '
                    f"AND symbol='{safe_symbol}'"
                ),
                "one_year_all_securities": f'SELECT symbol, avg("close") FROM {relation} WHERE "year"={year} GROUP BY symbol',
                "full_history_aggregation": f"SELECT symbol, avg(close), sum(volume) FROM {relation} GROUP BY symbol",
            }
            results[name] = {
                "files": len(files), "bytes": _size(target),
                "write_ms": round(write_ms, 3),
                "queries": {
                    query_name: _measure(connection, query, iterations, len(files))
                    for query_name, query in queries.items()
                },
            }
    baseline = results["single_file"]["bytes"]
    for result in results.values():
        result["disk_overhead_vs_single_percent"] = round((result["bytes"] / baseline - 1) * 100, 2)
    return {
        "dataset": "prices", "rows": rows, "source_files": len(source_files),
        "iterations": iterations, "query_date": str(latest), "query_symbol": symbol,
        "layouts": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="marketforge-partitions-") as directory:
        result = benchmark_layouts(args.raw_root, Path(directory), args.iterations)
    output = json.dumps(result, indent=2)
    if args.results:
        args.results.parent.mkdir(parents=True, exist_ok=True)
        args.results.write_text(output + "\n")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
