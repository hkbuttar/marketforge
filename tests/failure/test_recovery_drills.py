import errno
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import duckdb
from dagster import Failure

from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_backfill, run_incremental
from ingestion.sources.tiingo import TiingoError, fetch_prices
from observability.recovery import recovery_record, write_recovery_record
from orchestration.definitions import _run_dbt


class Response:
    def __init__(self, rows):
        self.body = json.dumps(rows).encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return self.body


def provider_row(volume=1000):
    return [{"date": "2026-08-10T00:00:00Z", "open": 100, "high": 102, "low": 99, "close": 101, "volume": volume}]


def count(root):
    files = list((root / "raw").glob("**/*.parquet"))
    if not files:
        return 0
    with duckdb.connect() as connection:
        return connection.execute("SELECT count(*) FROM read_parquet(?)", [str(root / "raw/**/*.parquet")]).fetchone()[0]


def options(root):
    return {"source": "tiingo", "raw_root": root / "raw", "quarantine_root": root / "quarantine", "metadata_root": root / "runs"}


class RecoveryDrills(unittest.TestCase):
    def persist(self, root, scenario, initial, failure, observed, retry, final, invariants):
        record = recovery_record(
            scenario, initial_state=initial, injected_failure=failure,
            observed_behavior=observed, retry_action=retry, final_state=final,
            invariants=invariants,
        )
        target = write_recovery_record(record, root / "recovery")
        self.assertTrue(record.recovered)
        self.assertTrue(json.loads(target.read_text())["recovered"])

    def test_source_outage_then_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            error = HTTPError("https://api.tiingo.com", 503, "injected", None, None)
            with patch("ingestion.sources.tiingo.urlopen", side_effect=error):
                with self.assertRaises(TiingoError):
                    fetch_prices(["AAPL"], start=date(2026, 8, 10), end=date(2026, 8, 10), api_key="x", max_retries=0)
            observed = count(root)
            with patch("ingestion.sources.tiingo.urlopen", return_value=Response(provider_row())):
                rows = fetch_prices(["AAPL"], start=date(2026, 8, 10), end=date(2026, 8, 10), api_key="x")
            run_backfill("prices", rows, run_id="recovered", **options(root))
            self.persist(root, "source-outage", {"rows": 0}, "HTTP 503", {"rows": observed}, "retry provider request", {"rows": count(root)}, {"no_missing_acknowledged_records": count(root) == 1, "no_duplicates": count(root) == 1})

    def test_quarantine_then_corrected_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bad = provider_row("bad")[0] | {"symbol": "AAPL", "date": "2026-08-10"}
            failed = run_backfill("prices", [bad], run_id="bad", **options(root))
            good = provider_row()[0] | {"symbol": "AAPL", "date": "2026-08-10"}
            recovered = run_backfill("prices", [good], run_id="corrected", **options(root))
            self.persist(root, "malformed-payload", {"rows": 0}, "invalid volume", {"quarantined": failed.quarantined_rows, "rows": 0}, "correct payload and rerun", {"rows": count(root), "accepted": recovered.accepted_rows}, {"bad_row_not_published": failed.accepted_rows == 0, "corrected_row_present": count(root) == 1})

    def test_disk_full_then_retry_same_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = provider_row()[0] | {"symbol": "AAPL", "date": "2026-08-10"}

            def fail(stage, _path):
                if stage == "before_temp_write":
                    raise OSError(errno.ENOSPC, "injected")

            with self.assertRaises(OSError):
                run_backfill("prices", [row], run_id="disk-retry", failure_hook=fail, **options(root))
            after_failure = count(root)
            result = run_backfill("prices", [row], run_id="disk-retry", **options(root))
            self.persist(root, "disk-full", {"rows": 0}, "ENOSPC", {"rows": after_failure}, "free space and retry same run", {"rows": count(root)}, {"no_partial_partition": after_failure == 0, "acknowledged_once": result.accepted_rows == 1 and count(root) == 1})

    def test_post_write_checkpoint_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = provider_row()[0] | {"symbol": "AAPL", "date": "2026-08-10"}
            store = CheckpointStore(root / "checkpoint.sqlite")

            def crash(stage, _path):
                if stage == "before_manifest":
                    raise RuntimeError("injected crash")

            incremental_options = options(root) | {"checkpoint_store": store, "initial_start": date(2026, 8, 10), "through": date(2026, 8, 10)}
            with self.assertRaises(RuntimeError):
                run_incremental("prices", [row], run_id="crash", failure_hook=crash, **incremental_options)
            observed = {"rows": count(root), "checkpoint": store.get("prices", "tiingo") is not None}
            result = run_incremental("prices", [row], run_id="restart", **incremental_options)
            checkpoint = store.get("prices", "tiingo")
            self.persist(root, "post-write-pre-checkpoint", {"rows": 0, "checkpoint": None}, "process crash", observed, "restart with same provider row", {"rows": count(root), "checkpoint": checkpoint.last_successful_event_date.isoformat()}, {"no_duplicate": result.backfill.duplicate_rows == 1 and count(root) == 1, "checkpoint_correct": checkpoint.last_successful_event_date == date(2026, 8, 10)})

    def test_dbt_failure_then_successful_retry_keeps_raw_available(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = provider_row()[0] | {"symbol": "AAPL", "date": "2026-08-10"}
            run_backfill("prices", [row], run_id="raw", **options(root))
            platform = Mock(
                dbt_executable="dbt", dbt_project_dir="dbt", dbt_profiles_dir="dbt",
                raw_root=str(root / "raw"), metadata_root=str(root / "runs"),
            )
            context = Mock()
            failed = Mock(returncode=1, stdout="", stderr="injected")
            succeeded = Mock(returncode=0, stdout="models built", stderr="")
            with patch("orchestration.definitions.subprocess.run", side_effect=[failed, succeeded]) as command:
                with self.assertRaises(Failure):
                    _run_dbt(context, platform, ["build"])
                after_failure = count(root)
                _run_dbt(context, platform, ["build"])
            self.persist(
                root, "dbt-failure", {"raw_rows": 1, "mart_ready": False},
                "dbt nonzero exit", {"raw_rows": after_failure, "mart_ready": False},
                "fix transformation and retry", {"raw_rows": count(root), "dbt_exit": 0},
                {"raw_remained_available": after_failure == 1, "retry_succeeded": command.call_count == 2},
            )


if __name__ == "__main__":
    unittest.main()
