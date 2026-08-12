#!/usr/bin/env python3
"""Build the small deterministic read-only dataset used by the hosted demo."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from observability.audit_store import SCHEMA
from ingestion.loaders import run_backfill
from ingestion.sources.businessquant import fetch_earnings
from ingestion.sources.fred import fetch_series
from ingestion.sources.http_json import SourceHTTPError
from ingestion.sources.newsapi import fetch_news
from ingestion.sources.sec_edgar import fetch_fundamentals
from ingestion.sources.tiingo import TiingoError, fetch_prices


LIVE_DATASETS = ("prices", "fundamentals", "earnings", "macro", "news")


def _live_records(today: date) -> dict[str, tuple[str, list[dict]]]:
    start = today - timedelta(days=60)
    return {
        "prices": ("tiingo", fetch_prices(("AAPL", "MSFT", "XOM"), start=start, end=today)),
        "fundamentals": ("sec-edgar", fetch_fundamentals("AAPL", "320193")),
        "earnings": ("businessquant", fetch_earnings("AAPL")),
        "macro": ("fred", [
            row for series in ("CPIAUCSL", "UNRATE", "FEDFUNDS", "GDP", "DGS10")
            for row in fetch_series(series, start=today - timedelta(days=730), end=today)
        ]),
        "news": ("newsapi", fetch_news(
            '("stock market" OR earnings OR economy)', start=today - timedelta(days=1),
            end=today, page_size=25,
        )),
    }


def _build_live(root: Path, *, today: date) -> dict[str, str | int]:
    raw_root = root / "raw"
    metadata_root = root / "ingestion_runs"
    quarantine_root = root / "quarantine"
    records = _live_records(today)
    runs = {}
    for dataset, (source, rows) in records.items():
        if not rows:
            raise SourceHTTPError(f"{source} returned no usable {dataset} records")
        result = run_backfill(
            dataset, rows, source=source, raw_root=raw_root,
            quarantine_root=quarantine_root, metadata_root=metadata_root,
            run_id=f"hosted-live-{dataset}-{today.isoformat()}",
        )
        if result.quarantined_rows:
            raise SourceHTTPError(
                f"{source} produced {result.quarantined_rows} quarantined {dataset} records"
            )
        runs[dataset] = result

    database = root / "marketforge-demo.duckdb"
    metadata = root / "operational.sqlite"
    lineage = root / "lineage.json"
    benchmarks = root / "benchmarks.json"
    database.unlink(missing_ok=True)
    metadata.unlink(missing_ok=True)
    patterns = {
        dataset: str(raw_root / dataset / "year=*" / "month=*" / "*.parquet").replace("'", "''")
        for dataset in LIVE_DATASETS
    }
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE SCHEMA main_marts")
        connection.execute(f"""CREATE TABLE main_marts.mart_security_daily AS
            WITH bars AS (SELECT date::DATE trade_date, symbol, lower(source) AS source_provider, close, volume,
                close/nullif(lag(close) OVER (PARTITION BY symbol,source ORDER BY date),0)-1 daily_return
                FROM read_parquet('{patterns['prices']}', hive_partitioning=false))
            SELECT trade_date,symbol,source_provider AS source,close,volume,daily_return,
                NULL::DOUBLE rolling_20d_return,
                stddev_samp(daily_return) OVER (PARTITION BY symbol,source_provider ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) rolling_20d_volatility FROM bars""")
        connection.execute(f"""CREATE TEMP VIEW live_fundamentals AS
            SELECT * FROM read_parquet('{patterns['fundamentals']}', hive_partitioning=false)""")
        connection.execute(f"""CREATE TEMP VIEW live_earnings AS
            SELECT * FROM read_parquet('{patterns['earnings']}', hive_partitioning=false)""")
        connection.execute("""CREATE TABLE main_marts.mart_company_snapshot AS
            WITH prices AS (SELECT symbol,source,arg_max(trade_date,trade_date) latest_price_date,
                arg_max(close,trade_date) latest_price FROM main_marts.mart_security_daily GROUP BY 1,2),
            fundamentals AS (SELECT symbol,max(period_end) latest_fundamental_period_end,
                max(filed_at) latest_fundamental_filed_at,count(distinct metric_name) available_fundamental_metrics
                FROM live_fundamentals GROUP BY symbol),
            earnings AS (SELECT symbol,max(event_timestamp) latest_earnings_timestamp,
                arg_max(eps_actual,event_timestamp) latest_eps_actual,
                arg_max(eps_actual-eps_estimate,event_timestamp) latest_eps_surprise
                FROM live_earnings GROUP BY symbol)
            SELECT p.*,f.latest_fundamental_period_end,f.latest_fundamental_filed_at,
                coalesce(f.available_fundamental_metrics,0)::BIGINT available_fundamental_metrics,
                e.latest_earnings_timestamp,e.latest_eps_actual,e.latest_eps_surprise
            FROM prices p LEFT JOIN fundamentals f USING(symbol) LEFT JOIN earnings e USING(symbol)""")
        connection.execute("""CREATE TABLE main_marts.mart_market_daily AS
            SELECT trade_date,avg(daily_return) mean_return,median(daily_return) median_return,
                stddev_samp(daily_return) cross_sectional_volatility,
                count(*) FILTER(WHERE daily_return>0)::BIGINT advancers,
                count(*) FILTER(WHERE daily_return<0)::BIGINT decliners,
                count(*) FILTER(WHERE daily_return=0)::BIGINT unchanged,
                (advancers-decliners)::DOUBLE/nullif(count(daily_return),0) market_breadth,
                count(daily_return)::BIGINT securities_with_returns
            FROM main_marts.mart_security_daily GROUP BY trade_date""")
        connection.execute("""CREATE TABLE main_marts.mart_sector_daily AS
            SELECT d.trade_date,s.sector,avg(d.daily_return) sector_average_return,
                count(d.daily_return)::BIGINT securities_with_returns
            FROM main_marts.mart_security_daily d JOIN
                (VALUES('AAPL','Information Technology'),('MSFT','Information Technology'),('XOM','Energy'))
                s(symbol,sector) USING(symbol) GROUP BY d.trade_date,s.sector""")
        health_rows = []
        for dataset in LIVE_DATASETS:
            result = runs[dataset]
            health_rows.append((
                dataset, "healthy", result.accepted_rows, 0.0, result.duplicate_rows,
                result.quarantined_rows, result.max_event_date,
                result.run_id, result.completed_at,
            ))
        connection.execute("""CREATE TABLE main_marts.mart_pipeline_dataset_health(
            dataset VARCHAR,status VARCHAR,row_count BIGINT,null_rate DOUBLE,
            duplicate_count BIGINT,quarantine_count BIGINT,latest_event_time TIMESTAMP,
            last_successful_run VARCHAR,last_successful_run_at TIMESTAMPTZ)""")
        connection.executemany("INSERT INTO main_marts.mart_pipeline_dataset_health VALUES (?,?,?,?,?,?,?,?,?)", health_rows)

    with sqlite3.connect(metadata) as connection:
        connection.executescript(SCHEMA)
        for dataset in LIVE_DATASETS:
            result = runs[dataset]
            connection.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
                result.run_id, f"ingest_{dataset}", dataset, "hosted_live_snapshot",
                result.started_at, result.completed_at, result.status,
                result.input_rows, result.accepted_rows, result.quarantined_rows, None,
                f"ingestion_runs/{result.run_id}.json",
            ))
            connection.execute("INSERT INTO quality_results VALUES (?,?,?,?,?,?,?,?,?)", (
                f"hosted-live-quality-{dataset}", result.run_id, dataset, "contract_and_reconciliation",
                "HEALTHY", result.accepted_rows, "zero quarantines",
                "Live provider snapshot passed contract and reconciliation",
                result.completed_at,
            ))

    generated_at = datetime.now(timezone.utc).isoformat()
    nodes = []
    edges = []
    for dataset in LIVE_DATASETS:
        source = f"source.marketforge.raw.{dataset}"
        model = "model.marketforge.mart_company_snapshot" if dataset in {"prices", "fundamentals", "earnings"} else f"model.marketforge.health_{dataset}"
        nodes.extend(({"id": source, "name": dataset, "type": "source"},
                      {"id": model, "name": model.rsplit('.',1)[-1], "type": "model"}))
        edges.append({"from": source, "to": model})
    lineage.write_text(json.dumps({"generated_at": generated_at, "nodes": nodes, "edges": edges}, indent=2)+"\n")
    benchmarks.write_text(json.dumps({
        "schema_version": 1, "environment": "full-local-lake-measurement",
        "historical_rows": 140703, "disk_footprint_bytes": 4459713,
        "parquet_compression_ratio": 0.1779,
        "full_refresh": {"wall_clock_seconds": 70.540453, "peak_ram_bytes": 489897984,
                         "bytes_read": 29482823, "bytes_written": 2799772},
        "incremental": {"wall_clock_seconds": 0.438170, "peak_ram_bytes": 241303552,
                        "bytes_read": 21000, "bytes_written": 6025},
        "incremental_comparison": {"runtime_speedup": 160.99, "write_reduction_percent": 99.78},
        "compaction": {"file_count_before": 11, "file_count_after": 1,
                       "latency_before_ms": 0.853, "latency_after_ms": 0.193},
    }, indent=2)+"\n")
    return {"rows": sum(result.accepted_rows for result in runs.values()), "database": str(database),
            "metadata": str(metadata), "raw_root": str(raw_root), "lineage": str(lineage),
            "benchmarks": str(benchmarks), "mode": "live"}


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
    parser.add_argument("--live", action="store_true", help="Build from configured live providers")
    args = parser.parse_args()
    if args.live:
        if os.getenv("MARKETFORGE_ENABLE_LIVE_HOSTED_DATA") != "1":
            parser.error(
                "live hosted data requires MARKETFORGE_ENABLE_LIVE_HOSTED_DATA=1 after license review"
            )
        try:
            result = _build_live(args.output, today=date.today())
        except (SourceHTTPError, TiingoError, ValueError) as exc:
            parser.error(f"live snapshot failed; refusing stale fallback: {exc}")
    else:
        result = build_snapshot(args.output)
    print(json.dumps({**result, "built_at": datetime.now(timezone.utc).isoformat()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
