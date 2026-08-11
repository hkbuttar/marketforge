import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from benchmarks.incremental_vs_full import benchmark_incremental_vs_full
from ingestion.loaders import run_backfill


class IncrementalBenchmarkTests(unittest.TestCase):
    def test_incremental_and_full_outputs_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            start = date(2026, 8, 3)
            rows = []
            for offset in range(5):
                day = start + timedelta(days=offset)
                for symbol in ("AAPL", "MSFT"):
                    rows.append({
                        "symbol": symbol, "date": day.isoformat(), "open": 100 + offset,
                        "high": 102 + offset, "low": 99 + offset, "close": 101 + offset,
                        "volume": 1000, "source_record_id": f"{symbol}:{day}",
                    })
            run_backfill(
                "prices", rows, source="tiingo", raw_root=root / "source/raw",
                quarantine_root=root / "source/quarantine", metadata_root=root / "source/runs",
                run_id="source",
            )
            result = benchmark_incremental_vs_full(root / "source/raw", root / "work")
            self.assertTrue(result["canonical_outputs_match"])
            self.assertEqual(result["full_refresh"]["rows_processed"], 10)
            self.assertEqual(result["daily_incremental"]["rows_processed"], 2)
            self.assertEqual(result["incremental_row_percent"], 20.0)


if __name__ == "__main__":
    unittest.main()
