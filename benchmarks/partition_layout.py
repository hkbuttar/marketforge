#!/usr/bin/env python3
"""Compare monthly and daily Hive partition layouts with synthetic daily bars."""

from __future__ import annotations

import json
import statistics
import tempfile
import time
from pathlib import Path

import duckdb


def size(root: Path) -> int:
    return sum(path.stat().st_size for path in root.glob("**/*.parquet"))


def latency(connection, glob: str, iterations: int = 20) -> float:
    samples = []
    query = "SELECT avg(close) FROM read_parquet(?, hive_partitioning=true) WHERE year=2025 AND month=6"
    for _ in range(iterations):
        started = time.perf_counter()
        connection.execute(query, [glob]).fetchone()
        samples.append((time.perf_counter() - started) * 1000)
    return statistics.median(samples)


def main() -> None:
    with tempfile.TemporaryDirectory() as directory, duckdb.connect() as connection:
        root = Path(directory)
        connection.execute("""
            CREATE TABLE bars AS
            SELECT
                'SYM' || lpad(symbol::VARCHAR, 3, '0') AS symbol,
                day::DATE AS trade_date,
                100.0 + symbol AS close,
                year(day)::INTEGER AS year,
                month(day)::INTEGER AS month,
                day(day)::INTEGER AS day
            FROM range(100) symbols(symbol)
            CROSS JOIN generate_series(DATE '2025-01-01', DATE '2025-12-31', INTERVAL 1 DAY) dates(day)
            WHERE dayofweek(day) BETWEEN 1 AND 5
        """)
        results = {}
        for name, partition_by in (("monthly", "year, month"), ("daily", "year, month, day")):
            target = root / name
            connection.execute(
                f"COPY bars TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY ({partition_by}))"
            )
            files = list(target.glob("**/*.parquet"))
            results[name] = {
                "rows": connection.execute("SELECT count(*) FROM bars").fetchone()[0],
                "files": len(files),
                "bytes": size(target),
                "june_query_median_ms": round(latency(connection, str(target / "**/*.parquet")), 3),
            }
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
