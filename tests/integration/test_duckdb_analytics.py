import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingestion.loaders import run_backfill
from warehouse.duckdb import install_raw_views


class DuckDBAnalyticsTests(unittest.TestCase):
    def test_view_queries_parquet_in_place_and_prunes_partitions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [
                {"symbol": "AAPL", "date": "2026-01-02", "open": 100, "high": 102,
                 "low": 99, "close": 101, "volume": 10, "source_record_id": "jan"},
                {"symbol": "AAPL", "date": "2026-02-02", "open": 101, "high": 103,
                 "low": 100, "close": 102, "volume": 11, "source_record_id": "feb"},
            ]
            run_backfill(
                "prices", rows, source="provider", raw_root=root / "raw",
                quarantine_root=root / "quarantine", metadata_root=root / "metadata",
                run_id="analytics", now=datetime(2026, 3, 1, tzinfo=timezone.utc),
            )
            with duckdb.connect() as connection:
                install_raw_views(connection, root / "raw")
                self.assertEqual(connection.execute("SELECT count(*) FROM raw_prices").fetchone()[0], 2)
                value = connection.execute(
                    "SELECT close FROM raw_prices WHERE year='2026' AND month='02'"
                ).fetchone()[0]
                self.assertEqual(value, 102)
                plan = connection.execute(
                    "EXPLAIN ANALYZE SELECT * FROM raw_prices WHERE year='2026' AND month='02'"
                ).fetchone()[1]
                self.assertIn("File Filters", plan)
                self.assertIn("Scanning Files: 1/2", plan)

    def test_missing_dataset_does_not_create_broken_view(self):
        with tempfile.TemporaryDirectory() as directory, duckdb.connect() as connection:
            install_raw_views(connection, Path(directory))
            views = connection.execute(
                "SELECT table_name FROM information_schema.views WHERE table_name LIKE 'raw_%'"
            ).fetchall()
            self.assertEqual(views, [])


if __name__ == "__main__":
    unittest.main()
