import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_backfill
from observability.audit_store import AuditStore


class AuditStoreTests(unittest.TestCase):
    def test_syncs_runs_versions_quality_and_checkpoints_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = root / "metadata"
            run_backfill(
                "prices", [{
                    "symbol": "AAPL", "date": "2026-08-10", "open": 100,
                    "high": 102, "low": 99, "close": 101, "volume": 1000,
                    "source_record_id": "AAPL:2026-08-10",
                }],
                source="tiingo", raw_root=root / "raw",
                quarantine_root=root / "quarantine", metadata_root=metadata / "ingestion_runs",
                run_id="audit-run", now=datetime(2026, 8, 11, tzinfo=timezone.utc),
            )
            (metadata / "quality").mkdir()
            (metadata / "quality/prices-tiingo.json").write_text(json.dumps({
                "dataset": "prices", "source": "tiingo", "event_date": "2026-08-10",
                "status": "HEALTHY", "reason": "checks passed", "findings": [],
            }))
            (metadata / "freshness").mkdir()
            (metadata / "freshness/prices.json").write_text(json.dumps({
                "dataset": "prices", "status": "HEALTHY", "reason": "current",
                "age_hours": 0, "expected_event_time": "2026-08-10T21:00:00+00:00",
                "evaluated_at": "2026-08-11T12:00:00+00:00",
            }))
            CheckpointStore(metadata / "checkpoints.sqlite").advance(
                "prices", "tiingo", datetime(2026, 8, 10).date(), "audit-run"
            )
            database = root / "operational.sqlite"
            store = AuditStore(database)
            first = store.sync_all(metadata, root / "raw")
            second = store.sync_all(metadata, root / "raw")
            self.assertEqual(first, second)
            with sqlite3.connect(database) as connection:
                run = connection.execute(
                    "SELECT status, records_fetched, records_written FROM pipeline_runs"
                ).fetchone()
                self.assertEqual(run, ("success", 1, 1))
                version = connection.execute(
                    "SELECT row_count, length(content_hash), partition FROM dataset_versions"
                ).fetchone()
                self.assertEqual(version, (1, 64, "year=2026/month=08"))
                self.assertEqual(connection.execute("SELECT count(*) FROM quality_results").fetchone()[0], 2)
                checkpoint = connection.execute(
                    "SELECT checkpoint_value, run_id FROM checkpoints"
                ).fetchone()
                self.assertEqual(checkpoint, ("2026-08-10", "audit-run"))


if __name__ == "__main__":
    unittest.main()
