import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingestion.loaders import IdempotencyConflictError, run_backfill


def price(record_id="one", close=101):
    return {
        "symbol": "AAPL", "date": "2026-08-10", "open": 100, "high": 103,
        "low": 99, "close": close, "volume": 10, "source_record_id": record_id,
    }


class IdempotencyFailureTests(unittest.TestCase):
    def test_same_natural_key_and_values_is_replay_even_with_different_vendor_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {
                "source": "provider", "raw_root": root / "raw",
                "quarantine_root": root / "quarantine", "metadata_root": root / "runs",
                "now": datetime(2026, 8, 11, tzinfo=timezone.utc),
            }
            run_backfill("prices", [price("one")], run_id="first", **options)
            replay = run_backfill("prices", [price("two")], run_id="second", **options)
            self.assertEqual(replay.accepted_rows, 0)
            self.assertEqual(replay.duplicate_rows, 1)

    def test_changed_values_for_existing_key_fail_without_new_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {
                "source": "provider", "raw_root": root / "raw",
                "quarantine_root": root / "quarantine", "metadata_root": root / "runs",
                "now": datetime(2026, 8, 11, tzinfo=timezone.utc),
            }
            run_backfill("prices", [price()], run_id="first", **options)
            files_before = set((root / "raw").glob("**/*.parquet"))
            with self.assertRaisesRegex(IdempotencyConflictError, "changed canonical values"):
                run_backfill("prices", [price("revision", close=102)], run_id="conflict", **options)
            self.assertEqual(set((root / "raw").glob("**/*.parquet")), files_before)
            with duckdb.connect() as connection:
                count = connection.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(root / "raw" / "**/*.parquet")]
                ).fetchone()[0]
            self.assertEqual(count, 1)
            self.assertFalse((root / "runs" / "conflict.json").exists())


if __name__ == "__main__":
    unittest.main()
