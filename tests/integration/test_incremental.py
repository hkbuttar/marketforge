import tempfile
import unittest
import json
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_incremental


def price(day: str, record_id: str, close: float = 100):
    return {
        "symbol": "AAPL", "date": day, "open": 99, "high": 102, "low": 98,
        "close": close, "volume": 10, "source_record_id": record_id,
    }


class IncrementalTests(unittest.TestCase):
    def test_late_arrival_is_written_and_audited_against_prior_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "checkpoints.sqlite")
            options = {
                "source": "provider", "checkpoint_store": store,
                "raw_root": root / "raw", "quarantine_root": root / "quarantine",
                "metadata_root": root / "runs",
            }
            first = run_incremental(
                "prices", [price("2026-07-17", "on-time-17")],
                initial_start=date(2026, 7, 17), through=date(2026, 7, 17),
                now=datetime(2026, 7, 17, 22, tzinfo=timezone.utc), run_id="on-time", **options,
            )
            self.assertEqual(first.backfill.late_arriving_rows, 0)

            late = run_incremental(
                "prices", [price("2026-07-15", "late-15"), price("2026-07-18", "on-time-18")],
                through=date(2026, 7, 18), overlap_days=3,
                now=datetime(2026, 7, 18, 22, tzinfo=timezone.utc), run_id="late-arrival", **options,
            )

            self.assertEqual(late.fetch_from, date(2026, 7, 15))
            self.assertEqual(late.backfill.accepted_rows, 2)
            self.assertEqual(late.backfill.late_arriving_rows, 1)
            self.assertEqual(late.backfill.earliest_late_event_date, "2026-07-15")
            self.assertEqual(late.backfill.prior_event_watermark, "2026-07-17")
            self.assertEqual(late.backfill.arrival_time, "2026-07-18T22:00:00+00:00")
            self.assertEqual(late.checkpoint_event_date, date(2026, 7, 18))
            july_files = list((root / "raw/prices/year=2026/month=07").glob("*.parquet"))
            self.assertEqual(len(july_files), 2)
            manifest = json.loads((root / "runs/late-arrival.json").read_text())
            self.assertEqual(manifest["late_arriving_rows"], 1)
            self.assertEqual(manifest["earliest_late_event_date"], "2026-07-15")
            self.assertIn("completed_at", manifest)

    def test_checkpoint_window_overlap_and_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "checkpoints.sqlite")
            options = {
                "source": "provider", "checkpoint_store": store,
                "raw_root": root / "raw", "quarantine_root": root / "quarantine",
                "metadata_root": root / "runs",
                "now": datetime(2026, 8, 11, tzinfo=timezone.utc),
            }
            history = [price("2026-08-08", "8"), price("2026-08-09", "9"), price("2026-08-10", "10")]
            first = run_incremental(
                "prices", history, initial_start=date(2026, 8, 9), through=date(2026, 8, 10),
                run_id="first", **options,
            )
            self.assertEqual(first.fetched_rows, 2)
            self.assertEqual(first.backfill.accepted_rows, 2)
            self.assertEqual(store.get("prices", "provider").last_successful_event_date, date(2026, 8, 10))

            available = history + [price("2026-08-11", "11")]
            second = run_incremental(
                "prices", available, through=date(2026, 8, 11), overlap_days=2,
                run_id="second", **options,
            )
            self.assertEqual(second.fetch_from, date(2026, 8, 9))
            self.assertEqual(second.backfill.duplicate_rows, 2)
            self.assertEqual(second.backfill.accepted_rows, 1)
            self.assertEqual(store.get("prices", "provider").last_successful_event_date, date(2026, 8, 11))

            replay = run_incremental(
                "prices", available, through=date(2026, 8, 11), overlap_days=0,
                run_id="replay", **options,
            )
            self.assertEqual(replay.backfill.accepted_rows, 0)
            self.assertEqual(replay.fetched_rows, 0)
            self.assertEqual(replay.backfill.duplicate_rows, 0)
            self.assertEqual(store.get("prices", "provider").last_successful_event_date, date(2026, 8, 11))

    def test_initial_start_required_and_quarantine_does_not_advance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "state.sqlite")
            options = {
                "source": "provider", "checkpoint_store": store,
                "raw_root": root / "raw", "quarantine_root": root / "quarantine",
                "metadata_root": root / "runs", "now": datetime(2026, 8, 11, tzinfo=timezone.utc),
            }
            with self.assertRaises(ValueError):
                run_incremental("prices", [], **options)
            bad = price("2026-08-11", "bad", close=-1)
            result = run_incremental(
                "prices", [bad], initial_start=date(2026, 8, 11), run_id="bad", **options
            )
            self.assertEqual(result.backfill.quarantined_rows, 1)
            self.assertIsNone(store.get("prices", "provider"))

    def test_checkpoint_never_moves_backward(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointStore(Path(directory) / "state.sqlite")
            store.advance("prices", "provider", date(2026, 8, 11), "new")
            checkpoint = store.advance("prices", "provider", date(2026, 8, 1), "old")
            self.assertEqual(checkpoint.last_successful_event_date, date(2026, 8, 11))


if __name__ == "__main__":
    unittest.main()
