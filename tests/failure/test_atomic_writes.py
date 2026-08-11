import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_backfill, run_incremental


NOW = datetime(2026, 8, 11, tzinfo=timezone.utc)


def row():
    return {
        "symbol": "AAPL", "date": "2026-08-10", "open": 100, "high": 102,
        "low": 99, "close": 101, "volume": 10, "source_record_id": "one",
    }


class InjectedCrash(RuntimeError):
    pass


def crash_at(expected):
    def hook(stage, _path):
        if stage == expected:
            raise InjectedCrash(stage)
    return hook


class AtomicWriteTests(unittest.TestCase):
    def options(self, root):
        return {
            "source": "provider", "raw_root": root / "raw",
            "quarantine_root": root / "quarantine", "metadata_root": root / "runs",
            "now": NOW, "run_id": "atomic-run",
        }

    def test_every_write_window_has_safe_retry(self):
        early_stages = {"before_temp_write", "after_temp_write", "after_temp_validation"}
        for stage in (*early_stages, "after_final_promotion", "before_manifest"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with self.assertRaises(InjectedCrash):
                    run_backfill("prices", [row()], failure_hook=crash_at(stage), **self.options(root))
                final_files = list((root / "raw" / "prices").glob("**/*.parquet"))
                self.assertEqual(bool(final_files), stage not in early_stages)
                self.assertFalse((root / "runs" / "atomic-run.json").exists())

                recovered = run_backfill("prices", [row()], **self.options(root))
                if stage in early_stages:
                    self.assertEqual(recovered.accepted_rows, 1)
                else:
                    self.assertEqual(recovered.accepted_rows, 0)
                    self.assertEqual(recovered.duplicate_rows, 1)
                self.assertTrue((root / "runs" / "atomic-run.json").exists())
                self.assertEqual(len(list((root / "raw" / "prices").glob("**/*.parquet"))), 1)

    def test_partial_writing_file_is_noncanonical_and_rebuilt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            partial = root / "raw/.tmp/atomic-run/prices/year=2026/month=08/part-atomic-run.writing"
            partial.parent.mkdir(parents=True)
            partial.write_bytes(b"not parquet")
            result = run_backfill("prices", [row()], **self.options(root))
            self.assertEqual(result.accepted_rows, 1)
            self.assertFalse(partial.exists())
            self.assertEqual(len(list((root / "raw" / "prices").glob("**/*.parquet"))), 1)

    def test_promotion_before_checkpoint_recovers_via_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "checkpoint.sqlite")
            options = {
                "source": "provider", "checkpoint_store": store,
                "initial_start": date(2026, 8, 10), "through": date(2026, 8, 10),
                "raw_root": root / "raw", "quarantine_root": root / "quarantine",
                "metadata_root": root / "runs", "now": NOW,
            }
            with self.assertRaises(InjectedCrash):
                run_incremental(
                    "prices", [row()], run_id="crashed", failure_hook=crash_at("before_manifest"),
                    **options,
                )
            self.assertIsNone(store.get("prices", "provider"))
            self.assertEqual(len(list((root / "raw" / "prices").glob("**/*.parquet"))), 1)

            recovered = run_incremental("prices", [row()], run_id="recovered", **options)
            self.assertEqual(recovered.backfill.accepted_rows, 0)
            self.assertEqual(recovered.backfill.duplicate_rows, 1)
            self.assertEqual(
                store.get("prices", "provider").last_successful_event_date, date(2026, 8, 10)
            )

    def test_run_id_cannot_escape_staging_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = self.options(root)
            options["run_id"] = "../escape"
            with self.assertRaises(ValueError):
                run_backfill("prices", [row()], **options)


if __name__ == "__main__":
    unittest.main()
