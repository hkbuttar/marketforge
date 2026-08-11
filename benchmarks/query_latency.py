#!/usr/bin/env python3
"""Benchmark representative DuckDB queries over a synthetic partitioned price lake."""

from __future__ import annotations

import json
import re
import statistics
import tempfile
import time
from pathlib import Path

import duckdb

from warehouse.duckdb import install_raw_views


QUERIES = {
    "one_symbol_point": "SELECT * FROM raw_prices WHERE year='2024' AND month='06' AND symbol='SYM042' AND date=DATE '2024-06-17'",
    "thirty_day_slice": "SELECT * FROM raw_prices WHERE year='2024' AND month='06' AND symbol='SYM042' AND date BETWEEN DATE '2024-06-01' AND DATE '2024-06-30'",
    "one_year_slice": "SELECT * FROM raw_prices WHERE year='2024' AND symbol='SYM042' AND date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'",
    "cross_sectional_day": "SELECT symbol, close FROM raw_prices WHERE year='2024' AND month='06' AND date=DATE '2024-06-17'",
    "full_history_aggregation": "SELECT symbol, avg(close), sum(volume) FROM raw_prices GROUP BY symbol",
}


def create_lake(root: Path) -> None:
    target = root / "prices"
    target.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE bars AS SELECT
                'SYM' || lpad(symbol::VARCHAR, 3, '0') AS symbol,
                day::DATE AS date,
                100.0 + symbol + row_number() OVER () * 0.00001 AS open,
                open + 2 AS high,
                open - 1 AS low,
                open + 1 AS close,
                (100000 + symbol * 100)::BIGINT AS volume,
                'synthetic'::VARCHAR AS source,
                symbol::VARCHAR || ':' || day::DATE::VARCHAR AS source_record_id,
                TIMESTAMPTZ '2026-08-11 00:00:00+00' AS ingested_at,
                year(day)::INTEGER AS year,
                month(day)::INTEGER AS month
            FROM range(100) symbols(symbol)
            CROSS JOIN generate_series(DATE '2021-01-01', DATE '2025-12-31', INTERVAL 1 DAY) dates(day)
            WHERE dayofweek(day) BETWEEN 1 AND 5
        """)
        connection.execute(
            f"COPY bars TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (year, month))"
        )


def execute_once(database: Path, raw_root: Path, query: str) -> tuple[float, int, str]:
    with duckdb.connect(str(database)) as connection:
        install_raw_views(connection, raw_root)
        started = time.perf_counter()
        rows = connection.execute(query).fetchall()
        elapsed_ms = (time.perf_counter() - started) * 1000
        plan = connection.execute(f"EXPLAIN ANALYZE {query}").fetchone()[1]
    return elapsed_ms, len(rows), plan


def scan_metrics(plan: str) -> dict[str, int | None]:
    file_match = re.search(r"Scanning Files:\s*(\d+)/(\d+)", plan)
    total_match = re.search(r"Total Files Read:\s*(\d+)", plan)
    row_matches = re.findall(r"([\d,]+) rows?", plan)
    files_scanned = int(file_match.group(1)) if file_match else (
        int(total_match.group(1)) if total_match else None
    )
    files_total = int(file_match.group(2)) if file_match else files_scanned
    return {
        "files_scanned": files_scanned,
        "files_total": files_total,
        "plan_rows_max": max((int(value.replace(",", "")) for value in row_matches), default=None),
    }


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        raw_root = root / "raw"
        database = root / "analytics.duckdb"
        create_lake(raw_root)
        total_rows = duckdb.connect().execute(
            "SELECT count(*) FROM read_parquet(?)", [str(raw_root / "prices/**/*.parquet")]
        ).fetchone()[0]
        file_count = len(list((raw_root / "prices").glob("**/*.parquet")))
        results = {}
        for name, query in QUERIES.items():
            cold_ms, rows, plan = execute_once(database, raw_root, query)
            with duckdb.connect(str(database)) as connection:
                install_raw_views(connection, raw_root)
                connection.execute(query).fetchall()
                samples = []
                for _ in range(10):
                    started = time.perf_counter()
                    connection.execute(query).fetchall()
                    samples.append((time.perf_counter() - started) * 1000)
            results[name] = {
                "cold_ms": round(cold_ms, 3),
                "warm_median_ms": round(statistics.median(samples), 3),
                "result_rows": rows,
                **scan_metrics(plan),
            }
        print(json.dumps({"lake_rows": total_rows, "lake_files": file_count, "queries": results}, indent=2))


if __name__ == "__main__":
    main()
