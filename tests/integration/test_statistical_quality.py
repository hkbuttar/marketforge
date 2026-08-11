import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ingestion.loaders import run_backfill
from quality.anomaly import evaluate_price_quality, write_quality_audit


def price(symbol, day, index, volume=1000):
    close = 100 + index + (0.25 if symbol == "MSFT" else 0)
    return {
        "symbol": symbol, "date": day.isoformat(), "open": close - 0.5,
        "high": close + 1, "low": close - 1, "close": close, "volume": volume,
        "source_record_id": f"{symbol}:{day.isoformat()}",
    }


class StatisticalQualityTests(unittest.TestCase):
    def test_profiles_baseline_and_detects_missing_symbol_and_zero_volume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            start = date(2026, 6, 1)
            history = [
                price(symbol, start + timedelta(days=index), index)
                for index in range(30)
                for symbol in ("AAPL", "MSFT")
            ]
            options = {
                "source": "tiingo", "raw_root": root / "raw",
                "quarantine_root": root / "quarantine", "metadata_root": root / "runs",
                "now": datetime(2026, 7, 1, tzinfo=timezone.utc),
            }
            run_backfill("prices", history, run_id="baseline", **options)
            healthy = evaluate_price_quality(
                root / "raw", source="tiingo", expected_symbols=["AAPL", "MSFT"]
            )
            self.assertEqual(healthy.status, "HEALTHY")
            self.assertEqual(healthy.baseline_days, 29)
            self.assertEqual(healthy.metrics["row_count"], 2)

            latest = start + timedelta(days=30)
            run_backfill("prices", [price("AAPL", latest, 30, volume=0)], run_id="anomaly", **options)
            failed = evaluate_price_quality(
                root / "raw", source="tiingo", expected_symbols=["AAPL", "MSFT"]
            )
            self.assertEqual(failed.status, "FAILED")
            self.assertEqual(failed.missing_symbols, ("MSFT",))
            self.assertEqual(failed.metrics["zero_volume_fraction"], 1.0)
            self.assertIn("zero_volume_fraction", {item.metric for item in failed.findings})
            self.assertIn("missing_symbol_fraction", {item.metric for item in failed.findings})
            target = write_quality_audit(failed, root / "quality")
            self.assertEqual(json.loads(target.read_text())["status"], "FAILED")

    def test_insufficient_history_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_backfill(
                "prices", [price("AAPL", date(2026, 6, 1), 0)], source="tiingo",
                raw_root=root / "raw", quarantine_root=root / "quarantine",
                metadata_root=root / "runs", run_id="one",
            )
            result = evaluate_price_quality(root / "raw", expected_symbols=["AAPL"])
            self.assertEqual(result.status, "UNKNOWN")
            self.assertIn("baseline days", result.reason)


if __name__ == "__main__":
    unittest.main()
