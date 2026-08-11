import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from ingestion.checkpoints import CheckpointStore
from scripts.backfill import execute_backfill


def price(day):
    return {
        "symbol": "AAPL", "date": day, "open": 100, "high": 102, "low": 99,
        "close": 101, "volume": 10, "source_record_id": day,
    }


class RangeBackfillTests(unittest.TestCase):
    def test_overlapping_ranges_are_recorded_and_reconciled(self):
        rows = [price("2025-01-02"), price("2025-02-03"), price("2025-03-03"), price("2025-04-01")]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {
                "source": "provider", "raw_root": root / "raw",
                "quarantine_root": root / "quarantine", "metadata_root": root / "metadata",
            }
            first = execute_backfill(
                "prices", rows, start=date(2025, 1, 1), end=date(2025, 3, 31),
                run_id="jan-mar", **options,
            )
            second = execute_backfill(
                "prices", rows, start=date(2025, 2, 1), end=date(2025, 4, 30),
                run_id="feb-apr", **options,
            )
            self.assertEqual(first.accepted_rows, 3)
            self.assertEqual(second.accepted_rows, 1)
            self.assertEqual(second.duplicate_rows, 2)
            self.assertEqual(second.files_written, 1)
            files = list((root / "raw/prices").glob("**/*.parquet"))
            self.assertEqual(len(files), 4)
            self.assertIn("month=04", str(next(path for path in files if "feb-apr" in path.name)))
            count = duckdb.connect().execute(
                "SELECT count(*) FROM read_parquet(?)", [str(root / "raw/**/*.parquet")]
            ).fetchone()[0]
            self.assertEqual(count, 4)
            manifest = json.loads((root / "metadata/feb-apr.json").read_text())
            self.assertEqual(manifest["run_type"], "range_backfill")
            self.assertEqual(manifest["requested_start"], "2025-02-01")
            self.assertEqual(manifest["requested_end"], "2025-04-30")

    def test_range_backfill_does_not_modify_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "checkpoint.sqlite")
            store.advance("prices", "provider", date(2026, 8, 11), "incremental")
            execute_backfill(
                "prices", [price("2025-01-02")], start=date(2025, 1, 1), end=date(2025, 1, 31),
                source="provider", raw_root=root / "raw", quarantine_root=root / "quarantine",
                metadata_root=root / "metadata", run_id="old-backfill",
            )
            checkpoint = store.get("prices", "provider")
            self.assertEqual(checkpoint.last_successful_event_date, date(2026, 8, 11))
            self.assertEqual(checkpoint.last_successful_run_id, "incremental")


if __name__ == "__main__":
    unittest.main()
