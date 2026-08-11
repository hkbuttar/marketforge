import json
import tempfile
import unittest
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from backend.main import create_app


class QueryApiTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        database = root / "marts.duckdb"
        with duckdb.connect(str(database)) as connection:
            connection.execute("CREATE SCHEMA main_marts")
            connection.execute("""CREATE TABLE main_marts.mart_company_snapshot AS SELECT
                'AAPL' AS symbol, 'tiingo' AS "source", DATE '2026-08-10' AS latest_price_date,
                227.18::DOUBLE latest_price, NULL::DATE latest_fundamental_period_end,
                NULL::TIMESTAMPTZ latest_fundamental_filed_at, NULL::BIGINT available_fundamental_metrics,
                NULL::TIMESTAMPTZ latest_earnings_timestamp, NULL::DOUBLE latest_eps_actual,
                NULL::DOUBLE latest_eps_surprise""")
            connection.execute("""CREATE TABLE main_marts.mart_security_daily AS SELECT
                DATE '2026-08-10' AS trade_date, 'AAPL' AS symbol, 'tiingo' AS "source", 227.18::DOUBLE AS close,
                .01::DOUBLE daily_return, .05::DOUBLE rolling_20d_return,
                .02::DOUBLE rolling_20d_volatility, 1000::BIGINT volume""")
            connection.execute("""CREATE TABLE main_marts.mart_pipeline_dataset_health AS SELECT
                'prices' dataset, 'healthy' status, 1::BIGINT row_count, 0::DOUBLE null_rate,
                0::BIGINT duplicate_count, 0::BIGINT quarantine_count,
                TIMESTAMP '2026-08-10' latest_event_time, 'run-1' last_successful_run,
                TIMESTAMPTZ '2026-08-11 00:00:00+00' last_successful_run_at""")
        lineage = root / "lineage.json"
        lineage.write_text(json.dumps({"generated_at": "2026-08-11T00:00:00+00:00", "nodes": [
            {"id": "source.marketforge.raw.prices", "name": "prices", "type": "source",
             "path": "sources.yml", "relation": "raw_prices"}], "edges": []}))
        self.client = TestClient(create_app(database=database, lineage_path=lineage))

    def tearDown(self):
        self.temporary.cleanup()

    def test_allowlisted_security_endpoints(self):
        self.assertEqual(self.client.get("/api/securities").json()["meta"]["returned"], 1)
        self.assertEqual(self.client.get("/api/securities/aapl").json()["symbol"], "AAPL")
        history = self.client.get("/api/securities/AAPL/history?source=tiingo").json()
        self.assertEqual(history["data"][0]["close"], 227.18)

    def test_health_datasets_lineage_and_bounds(self):
        self.assertEqual(self.client.get("/api/pipeline/health").status_code, 200)
        self.assertEqual(self.client.get("/api/datasets").json()["data"][0]["dataset"], "prices")
        self.assertEqual(self.client.get("/api/datasets/prices/lineage").status_code, 200)
        self.assertEqual(self.client.get("/api/securities?limit=501").status_code, 422)

    def test_missing_database_is_service_unavailable(self):
        client = TestClient(create_app(database=Path(self.temporary.name) / "missing.duckdb"))
        self.assertEqual(client.get("/api/securities").status_code, 503)


if __name__ == "__main__":
    unittest.main()
