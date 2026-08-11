import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingestion.loaders import run_backfill


NOW = "2026-08-10T12:00:00Z"


def price(record_id, trade_date, close=101):
    return {
        "symbol": "AAPL",
        "date": trade_date,
        "open": 100,
        "high": 102,
        "low": 99,
        "close": close,
        "volume": 1000,
        "source_record_id": record_id,
    }


class BackfillTests(unittest.TestCase):
    def test_every_dataset_writes_parquet(self):
        rows = {
            "prices": price("p1", "2026-01-02"),
            "fundamentals": {
                "symbol": "AAPL", "metric_name": "revenue", "period_start": "2025-10-01",
                "period_end": "2025-12-31", "period_type": "quarter", "filed_at": NOW,
                "value": 1.0, "unit": "usd", "currency": "usd", "source_record_id": "f1",
            },
            "earnings": {
                "symbol": "AAPL", "event_timestamp": NOW, "fiscal_period_end": "2025-12-31",
                "event_status": "reported", "eps_estimate": 1.0, "eps_actual": 1.1,
                "source_record_id": "e1",
            },
            "macro": {
                "series_id": "CPI", "observation_date": "2026-01-01", "released_at": NOW,
                "value": 1.0, "unit": "index", "frequency": "monthly", "source_record_id": "m1",
            },
            "news": {
                "event_timestamp": NOW, "headline": "Example", "url": None, "publisher": None,
                "source_record_id": "n1",
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for dataset, row in rows.items():
                result = run_backfill(
                    dataset, [row], source="test-provider", raw_root=root / "raw",
                    quarantine_root=root / "quarantine", metadata_root=root / "metadata",
                    run_id=f"run-{dataset}", now=datetime(2026, 8, 11, tzinfo=timezone.utc),
                )
                self.assertEqual(result.accepted_rows, 1, dataset)
                self.assertEqual(result.files_written, 1, dataset)
                replay = run_backfill(
                    dataset, [row], source="test-provider", raw_root=root / "raw",
                    quarantine_root=root / "quarantine", metadata_root=root / "metadata",
                    run_id=f"replay-{dataset}", now=datetime(2026, 8, 12, tzinfo=timezone.utc),
                )
                self.assertEqual(replay.accepted_rows, 0, dataset)
                self.assertEqual(replay.duplicate_rows, 1, dataset)

    def test_backfill_partitions_quarantines_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {
                "source": "test-provider",
                "raw_root": root / "raw",
                "quarantine_root": root / "quarantine",
                "metadata_root": root / "metadata",
                "now": datetime(2026, 8, 11, tzinfo=timezone.utc),
            }
            rows = [
                price("jan", "2026-01-02"),
                price("feb", "2026-02-02"),
                price("bad", "2026-02-03", close=-1),
            ]
            first = run_backfill("prices", rows, run_id="first", **options)
            self.assertEqual((first.accepted_rows, first.quarantined_rows, first.files_written), (2, 1, 2))
            self.assertEqual(first.input_rows, 3)
            self.assertEqual(first.records_written, 2)
            self.assertEqual(first.reconciliation_status, "passed")
            self.assertEqual(first.pre_write_row_count, 0)
            self.assertEqual(first.post_write_row_count, 2)
            audit = json.loads((root / "reconciliation/first.json").read_text())
            self.assertEqual(audit["source_records_fetched"], 3)
            self.assertEqual(audit["records_accepted"], 2)
            self.assertEqual(audit["records_rejected"], 1)
            self.assertEqual(audit["records_deduplicated"], 0)
            self.assertEqual(audit["status"], "passed")
            files = sorted((root / "raw").glob("**/*.parquet"))
            self.assertIn("year=2026/month=01", str(files[0]))
            count = duckdb.connect().execute(
                "SELECT count(*) FROM read_parquet(?)", [str(root / "raw" / "**/*.parquet")]
            ).fetchone()[0]
            self.assertEqual(count, 2)
            quarantine = json.loads(next((root / "quarantine").glob("**/*.jsonl")).read_text())
            self.assertEqual(quarantine["raw_payload"]["source_record_id"], "bad")

            second = run_backfill("prices", rows[:2], run_id="second", **options)
            self.assertEqual(second.accepted_rows, 0)
            self.assertEqual(second.duplicate_rows, 2)
            self.assertEqual(second.files_written, 0)
            self.assertEqual(second.pre_write_row_count, 2)
            self.assertEqual(second.post_write_row_count, 2)
            self.assertEqual(second.actual_row_delta, 0)
            manifest = json.loads((root / "metadata" / "second.json").read_text())
            self.assertEqual(manifest["status"], "success")

    def test_breaking_type_degrades_run_without_changing_canonical_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_backfill(
                "prices", [price("bad-volume", "2026-01-02") | {"volume": "1.23M"}],
                source="test-provider", raw_root=root / "raw",
                quarantine_root=root / "quarantine", metadata_root=root / "metadata",
                run_id="breaking-type", now=datetime(2026, 8, 11, tzinfo=timezone.utc),
            )
            self.assertEqual(result.status, "degraded")
            self.assertEqual(result.contract_version, 1)
            self.assertEqual(result.quarantined_rows, 1)
            self.assertEqual(result.files_written, 0)
            self.assertFalse(list((root / "raw").glob("**/*.parquet")))
            manifest = json.loads((root / "metadata/breaking-type.json").read_text())
            self.assertEqual(manifest["status"], "degraded")


if __name__ == "__main__":
    unittest.main()
