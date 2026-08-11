import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from observability.freshness import evaluate_freshness, write_freshness_audit


class FreshnessTests(unittest.TestCase):
    def test_prices_use_latest_expected_market_day(self):
        monday_after_close = datetime(2026, 8, 10, 23, tzinfo=timezone.utc)
        healthy = evaluate_freshness(
            "prices", latest_event_time="2026-08-10", last_source_check_at=None,
            evaluated_at=monday_after_close,
        )
        self.assertEqual(healthy.status, "HEALTHY")
        self.assertIn("2026-08-10", healthy.expected_event_time)

        degraded = evaluate_freshness(
            "prices", latest_event_time="2026-08-07", last_source_check_at=None,
            evaluated_at=monday_after_close,
        )
        self.assertEqual(degraded.status, "FAILED")

    def test_weekend_does_not_expect_a_new_price_bar(self):
        sunday = datetime(2026, 8, 9, 18, tzinfo=timezone.utc)
        result = evaluate_freshness(
            "prices", latest_event_time="2026-08-07", last_source_check_at=None,
            evaluated_at=sunday,
        )
        self.assertEqual(result.status, "HEALTHY")

    def test_fundamentals_use_source_check_age_not_quarterly_event_age(self):
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        result = evaluate_freshness(
            "fundamentals", latest_event_time="2026-03-31",
            last_source_check_at="2026-08-10T12:00:00Z", evaluated_at=now,
        )
        self.assertEqual(result.status, "HEALTHY")
        self.assertEqual(result.basis, "source_check")

    def test_missing_evidence_is_unknown_and_reasoned(self):
        result = evaluate_freshness(
            "macro", latest_event_time=None, last_source_check_at=None,
            evaluated_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
        )
        self.assertEqual(result.status, "UNKNOWN")
        self.assertIn("no source check evidence", result.reason)

    def test_audit_is_persisted(self):
        result = evaluate_freshness(
            "news", latest_event_time="2026-08-09T12:00:00Z",
            last_source_check_at="2026-08-10T12:00:00Z",
            evaluated_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
        )
        with tempfile.TemporaryDirectory() as directory:
            target = write_freshness_audit(result, Path(directory))
            payload = json.loads(target.read_text())
            self.assertEqual(payload["status"], "DEGRADED")
            self.assertTrue(payload["reason"])


if __name__ == "__main__":
    unittest.main()
