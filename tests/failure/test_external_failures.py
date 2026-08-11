import errno
import tempfile
import unittest
from datetime import date
from email.message import Message
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import duckdb
from dagster import Failure

from ingestion.loaders import run_backfill
from ingestion.sources.tiingo import TiingoError, fetch_prices
from orchestration.definitions import _run_dbt


def response_row(volume=1000):
    return {
        "date": "2026-08-10T00:00:00Z", "open": 100, "high": 102,
        "low": 99, "close": 101, "volume": volume,
    }


class Response:
    def __init__(self, payload):
        import json
        self.body = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return self.body


def http_error(code, retry_after=None):
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return HTTPError("https://api.tiingo.com", code, "injected", headers, None)


def canonical_count(root):
    files = list((root / "raw").glob("**/*.parquet"))
    if not files:
        return 0
    with duckdb.connect() as connection:
        return connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(root / "raw/**/*.parquet")]
        ).fetchone()[0]


class ExternalFailureTests(unittest.TestCase):
    def test_503_retries_with_bounded_backoff_then_preserves_prior_data(self):
        sleeps = []
        with patch("ingestion.sources.tiingo.urlopen", side_effect=[http_error(503), http_error(503), http_error(503)]) as call:
            with self.assertRaisesRegex(TiingoError, "after 3 attempts: HTTP 503"):
                fetch_prices(
                    ["AAPL"], start=date(2026, 8, 10), end=date(2026, 8, 10),
                    api_key="secret", backoff_seconds=1, max_backoff_seconds=1.5,
                    sleep=sleeps.append,
                )
        self.assertEqual(call.call_count, 3)
        self.assertEqual(sleeps, [1, 1.5])

    def test_429_respects_retry_after_and_replay_is_not_duplicated(self):
        sleeps = []
        with patch("ingestion.sources.tiingo.urlopen", side_effect=[http_error(429, 1), Response([response_row()])]):
            rows = fetch_prices(
                ["AAPL"], start=date(2026, 8, 10), end=date(2026, 8, 10),
                api_key="secret", max_backoff_seconds=2, sleep=sleeps.append,
            )
        self.assertEqual(sleeps, [1])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {"source": "tiingo", "raw_root": root / "raw", "quarantine_root": root / "quarantine", "metadata_root": root / "runs"}
            run_backfill("prices", rows, run_id="first", **options)
            replay = run_backfill("prices", rows, run_id="replay", **options)
            self.assertEqual(replay.duplicate_rows, 1)
            self.assertEqual(canonical_count(root), 1)

    def test_malformed_value_is_quarantined_and_canonical_state_unchanged(self):
        with patch("ingestion.sources.tiingo.urlopen", return_value=Response([response_row("not-an-integer")])):
            rows = fetch_prices(["AAPL"], start=date(2026, 8, 10), end=date(2026, 8, 10), api_key="secret")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_backfill(
                "prices", rows, source="tiingo", raw_root=root / "raw",
                quarantine_root=root / "quarantine", metadata_root=root / "runs", run_id="malformed",
            )
            self.assertEqual(result.status, "degraded")
            self.assertEqual(result.quarantined_rows, 1)
            self.assertEqual(canonical_count(root), 0)

    def test_disk_full_before_temp_write_preserves_existing_partition(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {"source": "tiingo", "raw_root": root / "raw", "quarantine_root": root / "quarantine", "metadata_root": root / "runs"}
            base = {"symbol": "AAPL", "date": "2026-08-09", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 10}
            run_backfill("prices", [base], run_id="base", **options)

            def disk_full(stage, _path):
                if stage == "before_temp_write":
                    raise OSError(errno.ENOSPC, "injected disk full")

            newer = base | {"date": "2026-08-10"}
            with self.assertRaisesRegex(OSError, "injected disk full"):
                run_backfill("prices", [newer], run_id="disk-full", failure_hook=disk_full, **options)
            self.assertEqual(canonical_count(root), 1)
            self.assertFalse((root / "runs/disk-full.json").exists())

    def test_dbt_failure_does_not_invalidate_raw_or_publish_mart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = {"symbol": "AAPL", "date": "2026-08-10", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 10}
            run_backfill(
                "prices", [row], source="tiingo", raw_root=root / "raw",
                quarantine_root=root / "quarantine", metadata_root=root / "runs", run_id="raw-ok",
            )
            platform = Mock(dbt_executable="dbt", dbt_project_dir="dbt", dbt_profiles_dir="dbt", raw_root=str(root / "raw"), metadata_root=str(root / "runs"))
            context = Mock()
            with patch("orchestration.definitions.subprocess.run", return_value=Mock(returncode=1, stdout="", stderr="injected dbt failure")):
                with self.assertRaises(Failure):
                    _run_dbt(context, platform, ["build"])
            self.assertEqual(canonical_count(root), 1)
            self.assertFalse((root / "marts").exists())


if __name__ == "__main__":
    unittest.main()
