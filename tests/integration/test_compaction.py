import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingestion.compaction import compact_partition
from ingestion.loaders import run_backfill
from observability.builds import _resolve_artifact


def price(day, close):
    return {"symbol": "AAPL", "date": day, "open": close, "high": close + 1,
            "low": close - 1, "close": close, "volume": 100, "source": "test",
            "source_record_id": day, "ingested_at": "2026-08-11T00:00:00Z"}


class CompactionTests(unittest.TestCase):
    def setup_partition(self, root):
        for index, day in enumerate(("2026-08-01", "2026-08-02", "2026-08-03")):
            run_backfill("prices", [price(day, 100 + index)], source="test", run_id=f"run-{index}",
                         raw_root=root / "raw", quarantine_root=root / "quarantine",
                         metadata_root=root / "runs", now=datetime(2026, 8, 11, tzinfo=timezone.utc))

    def test_compaction_preserves_rows_schema_and_original_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.setup_partition(root)
            result = compact_partition("prices", 2026, 8, raw_root=root / "raw",
                archive_root=root / "archive", metadata_root=root / "warehouse/metadata/compactions",
                compaction_id="compact-1")
            self.assertEqual((result.files_before, result.files_after), (3, 1))
            self.assertEqual((result.rows_before, result.rows_after), (3, 3))
            self.assertEqual(len(list((root / "archive").glob("**/*.parquet"))), 3)
            with duckdb.connect() as connection:
                self.assertEqual(connection.execute("SELECT count(*) FROM read_parquet(?)",
                    [str(root / "raw/prices/year=2026/month=08/*.parquet")]).fetchone()[0], 3)
            self.assertTrue((root / "warehouse/metadata/compactions/compact-1.json").exists())
            archived = _resolve_artifact(
                root / "raw/prices/year=2026/month=08/part-run-0.parquet",
                {"dataset": "prices", "partition": "year=2026/month=08"}, root,
            )
            self.assertEqual(archived, root / "archive/prices/year=2026/month=08/compact-1/part-run-0.parquet")
            self.assertTrue(archived.exists())

    def test_failed_promotion_rolls_original_partition_back(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.setup_partition(root)
            def fail(stage, _path):
                if stage == "after_archive_before_promotion":
                    raise RuntimeError("injected")
            with self.assertRaisesRegex(RuntimeError, "injected"):
                compact_partition("prices", 2026, 8, raw_root=root / "raw",
                    archive_root=root / "archive", metadata_root=root / "metadata",
                    compaction_id="failed", failure_hook=fail)
            self.assertEqual(len(list((root / "raw/prices/year=2026/month=08").glob("*.parquet"))), 3)


if __name__ == "__main__":
    unittest.main()
