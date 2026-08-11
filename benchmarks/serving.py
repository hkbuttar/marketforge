#!/usr/bin/env python3
"""Benchmark cold and warm FastAPI endpoints over the retained price lake."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb
from fastapi.testclient import TestClient

from backend.main import create_app


ENDPOINTS = (
    "/api/securities?limit=100",
    "/api/securities/AAPL",
    "/api/securities/AAPL/history?source=tiingo&limit=252",
    "/api/pipeline/health",
    "/api/datasets",
    "/api/datasets/prices/lineage",
)


def _percentile(samples: list[float], percentile: float) -> float:
    ordered = sorted(samples)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * percentile + 0.999999) - 1))
    return ordered[index]


def _database(raw_root: Path, target: Path) -> None:
    pattern = str(raw_root / "prices/year=*/month=*/*.parquet").replace("'", "''")
    if not list((raw_root / "prices").glob("year=*/month=*/*.parquet")):
        raise ValueError("no price Parquet files found")
    with duckdb.connect(str(target)) as connection:
        connection.execute("CREATE SCHEMA main_marts")
        connection.execute(f"""CREATE TABLE main_marts.mart_security_daily AS
            WITH bars AS (
                SELECT symbol, lower("source") AS "source", date::DATE trade_date, close, volume,
                       lag(close) OVER (PARTITION BY symbol, "source" ORDER BY date) previous_close
                FROM read_parquet('{pattern}', hive_partitioning=false, union_by_name=true)
                WHERE source='tiingo'
            ) SELECT trade_date, symbol, "source", close,
                close / nullif(previous_close, 0) - 1.0 daily_return,
                NULL::DOUBLE rolling_20d_return, NULL::DOUBLE rolling_20d_volatility, volume
            FROM bars""")
        connection.execute("""CREATE TABLE main_marts.mart_company_snapshot AS
            SELECT symbol, source, arg_max(trade_date, trade_date) latest_price_date,
                arg_max(close, trade_date) latest_price,
                NULL::DATE latest_fundamental_period_end,
                NULL::TIMESTAMPTZ latest_fundamental_filed_at,
                0::BIGINT available_fundamental_metrics,
                NULL::TIMESTAMPTZ latest_earnings_timestamp,
                NULL::DOUBLE latest_eps_actual, NULL::DOUBLE latest_eps_surprise
            FROM main_marts.mart_security_daily GROUP BY symbol, "source"
        """)
        connection.execute("""CREATE TABLE main_marts.mart_pipeline_dataset_health AS
            SELECT 'prices' dataset, 'healthy' status, count(*)::BIGINT row_count,
                0.0::DOUBLE null_rate, 0::BIGINT duplicate_count, 0::BIGINT quarantine_count,
                max(trade_date)::TIMESTAMP latest_event_time, 'benchmark' last_successful_run,
                current_timestamp last_successful_run_at
            FROM main_marts.mart_security_daily""")


def benchmark_serving(
    raw_root: Path, lineage_path: Path, work_root: Path, iterations: int = 20
) -> dict[str, Any]:
    if iterations < 2:
        raise ValueError("iterations must be at least 2")
    database = work_root / "serving.duckdb"
    _database(raw_root, database)
    endpoints = {}
    for endpoint in ENDPOINTS:
        cold, warm = [], []
        for _ in range(iterations):
            with TestClient(create_app(database=database, lineage_path=lineage_path)) as client:
                started = time.perf_counter()
                response = client.get(endpoint)
                cold.append((time.perf_counter() - started) * 1000)
                if response.status_code != 200:
                    raise RuntimeError(f"{endpoint} returned {response.status_code}: {response.text}")
        with TestClient(create_app(database=database, lineage_path=lineage_path)) as client:
            client.get(endpoint)
            for _ in range(iterations):
                started = time.perf_counter()
                response = client.get(endpoint)
                warm.append((time.perf_counter() - started) * 1000)
                if response.status_code != 200:
                    raise RuntimeError(f"{endpoint} returned {response.status_code}: {response.text}")
        endpoints[endpoint] = {
            "cold_median_ms": round(statistics.median(cold), 3),
            "cold_p95_ms": round(_percentile(cold, 0.95), 3),
            "warm_median_ms": round(statistics.median(warm), 3),
            "warm_p95_ms": round(_percentile(warm, 0.95), 3),
            "iterations": iterations,
        }
    return {"dataset": "retained Tiingo prices", "endpoints": endpoints}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--lineage", type=Path, default=Path("warehouse/metadata/lineage.json"))
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--results", type=Path, default=Path("benchmarks/results/serving.json"))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="marketforge-serving-") as directory:
        result = benchmark_serving(args.raw_root, args.lineage, Path(directory), args.iterations)
    args.results.parent.mkdir(parents=True, exist_ok=True)
    args.results.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
