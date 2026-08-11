#!/usr/bin/env python3
"""Build the small deterministic read-only dataset used by the hosted demo."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from observability.audit_store import SCHEMA


def build_snapshot(root: Path) -> dict[str, str | int]:
    root.mkdir(parents=True, exist_ok=True)
    raw = root / "raw/prices/year=2026/month=07"
    raw.mkdir(parents=True, exist_ok=True)
    parquet = raw / "curated-demo.parquet"
    database = root / "marketforge-demo.duckdb"
    metadata = root / "operational.sqlite"
    lineage = root / "lineage.json"
    benchmarks = root / "benchmarks.json"
    for target in (parquet, database, metadata):
        target.unlink(missing_ok=True)

    with duckdb.connect() as connection:
        connection.execute("""CREATE TABLE bars AS
            SELECT symbol, day::DATE date, 100.0 + symbol_index * 20 + day_index AS "open",
                "open" + 2 high, "open" - 1 low, "open" + 1 AS "close",
                (100000 + symbol_index * 10000 + day_index * 100)::BIGINT volume,
                'demo' AS "source", symbol || ':' || day::DATE::VARCHAR source_record_id,
                TIMESTAMPTZ '2026-08-11 00:00:00+00' ingested_at
            FROM (VALUES ('AAPL', 0), ('MSFT', 1), ('XOM', 2)) symbols(symbol, symbol_index)
            CROSS JOIN (
                SELECT day, row_number() OVER (ORDER BY day) day_index
                FROM generate_series(DATE '2026-06-22', DATE '2026-07-31', INTERVAL 1 DAY) dates(day)
                WHERE dayofweek(day) BETWEEN 1 AND 5
            ) dates""")
        connection.execute(
            f"COPY bars TO '{parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE SCHEMA main_marts")
        connection.execute(f"""CREATE TABLE main_marts.mart_security_daily AS
            WITH returns AS (
                SELECT date trade_date, symbol, source, close, volume,
                    close / nullif(lag(close) OVER (PARTITION BY symbol ORDER BY date), 0) - 1 daily_return
                FROM read_parquet('{parquet}')
            ) SELECT *, NULL::DOUBLE rolling_20d_return,
                stddev_samp(daily_return) OVER (
                    PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) rolling_20d_volatility FROM returns""")
        connection.execute("""CREATE TABLE main_marts.mart_company_snapshot AS
            SELECT symbol, source, arg_max(trade_date, trade_date) latest_price_date,
                arg_max(close, trade_date) latest_price,
                NULL::DATE latest_fundamental_period_end,
                NULL::TIMESTAMPTZ latest_fundamental_filed_at,
                0::BIGINT available_fundamental_metrics,
                NULL::TIMESTAMPTZ latest_earnings_timestamp,
                NULL::DOUBLE latest_eps_actual, NULL::DOUBLE latest_eps_surprise
            FROM main_marts.mart_security_daily GROUP BY symbol, source""")
        connection.execute("""CREATE TABLE main_marts.mart_market_daily AS
            SELECT trade_date, avg(daily_return) mean_return, median(daily_return) median_return,
                stddev_samp(daily_return) cross_sectional_volatility,
                count(*) FILTER (WHERE daily_return > 0)::BIGINT advancers,
                count(*) FILTER (WHERE daily_return < 0)::BIGINT decliners,
                count(*) FILTER (WHERE daily_return = 0)::BIGINT unchanged,
                (advancers-decliners)::DOUBLE/nullif(count(daily_return),0) market_breadth,
                count(daily_return)::BIGINT securities_with_returns
            FROM main_marts.mart_security_daily GROUP BY trade_date""")
        connection.execute("""CREATE TABLE main_marts.mart_sector_daily AS
            SELECT daily.trade_date, sectors.sector, avg(daily.daily_return) sector_average_return,
                count(daily.daily_return)::BIGINT securities_with_returns
            FROM main_marts.mart_security_daily daily
            JOIN (VALUES ('AAPL','Information Technology'), ('MSFT','Information Technology'),
                         ('XOM','Energy')) sectors(symbol,sector) USING(symbol)
            GROUP BY daily.trade_date, sectors.sector""")
        connection.execute("""CREATE TABLE main_marts.mart_pipeline_dataset_health AS
            SELECT 'prices' dataset, 'healthy' status, count(*)::BIGINT row_count,
                0.0::DOUBLE null_rate, 0::BIGINT duplicate_count, 0::BIGINT quarantine_count,
                max(trade_date)::TIMESTAMP latest_event_time, 'hosted-demo-build' last_successful_run,
                TIMESTAMPTZ '2026-08-11 00:00:00+00' last_successful_run_at
            FROM main_marts.mart_security_daily""")

    with sqlite3.connect(metadata) as connection:
        connection.executescript(SCHEMA)
        connection.execute(
            "INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("hosted-demo-build", "build_demo_snapshot", "prices", "curated_snapshot",
             "2026-08-11T00:00:00+00:00", "2026-08-11T00:00:01+00:00", "success",
             90, 90, 0, None, "generated"),
        )
        connection.execute(
            "INSERT INTO quality_results VALUES (?,?,?,?,?,?,?,?,?)",
            ("hosted-demo-quality", "hosted-demo-build", "prices", "fixture_integrity",
             "HEALTHY", 90, "90 rows", "Curated snapshot row count verified",
             "2026-08-11T00:00:01+00:00"),
        )

    lineage.write_text(json.dumps({
        "generated_at": "2026-08-11T00:00:01+00:00",
        "nodes": [
            {"id": "source.marketforge.raw.prices", "name": "prices", "type": "source"},
            {"id": "model.marketforge.mart_security_daily", "name": "mart_security_daily", "type": "model"},
            {"id": "model.marketforge.mart_company_snapshot", "name": "mart_company_snapshot", "type": "model"},
        ],
        "edges": [
            {"from": "source.marketforge.raw.prices", "to": "model.marketforge.mart_security_daily"},
            {"from": "model.marketforge.mart_security_daily", "to": "model.marketforge.mart_company_snapshot"},
        ],
    }, indent=2) + "\n", encoding="utf-8")
    benchmarks.write_text(json.dumps({
        "schema_version": 1, "environment": "full-local-lake-measurement",
        "historical_rows": 68897, "disk_footprint_bytes": 1972960,
        "parquet_compression_ratio": 0.1796,
        "full_refresh": {"wall_clock_seconds": 34.474819, "peak_ram_bytes": 275136512,
                         "bytes_read": 14484982, "bytes_written": 1471524},
        "incremental": {"wall_clock_seconds": 0.225584, "peak_ram_bytes": 155107328,
                        "bytes_read": 10328, "bytes_written": 8862},
        "incremental_comparison": {"runtime_speedup": 152.82, "write_reduction_percent": 99.4},
        "compaction": {"file_count_before": 4, "file_count_after": 1,
                       "latency_before_ms": 0.692, "latency_after_ms": 0.273},
    }, indent=2) + "\n", encoding="utf-8")
    return {"rows": 90, "database": str(database), "metadata": str(metadata),
            "raw_root": str(root / "raw"), "lineage": str(lineage), "benchmarks": str(benchmarks)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("deploy/generated"))
    args = parser.parse_args()
    result = build_snapshot(args.output)
    print(json.dumps({**result, "built_at": datetime.now(timezone.utc).isoformat()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
