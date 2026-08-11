import csv
import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from benchmarks.run import markdown_report, run_suite, write_reports
from ingestion.loaders import run_backfill


class BenchmarkSuiteTests(unittest.TestCase):
    def test_suite_covers_required_sections_and_writes_three_formats(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = []
            for offset in range(3):
                day = date(2026, 8, 3) + timedelta(days=offset)
                for symbol in ("AAPL", "MSFT"):
                    rows.append({
                        "symbol": symbol, "date": day.isoformat(), "open": 100 + offset,
                        "high": 102 + offset, "low": 99 + offset, "close": 101 + offset,
                        "volume": 1000, "source_record_id": f"{symbol}:{day}:{offset}",
                    })
                run_backfill(
                    "prices", rows[-2:], source="tiingo", raw_root=root / "raw",
                    quarantine_root=root / "quarantine", metadata_root=root / "runs",
                    run_id=f"source-{offset}",
                )

            result = run_suite(root / "raw", iterations=1)
            self.assertEqual(result["historical_rows"], 6)
            self.assertTrue(result["incremental_comparison"]["canonical_outputs_match"])
            self.assertEqual(result["compaction"]["status"], "success")
            self.assertEqual(result["compaction"]["file_count_after"], 1)
            self.assertIn("sector_aggregate", result["queries"])

            paths = write_reports(result, root / "reports")
            self.assertEqual(json.loads(Path(paths["json"]).read_text())["historical_rows"], 6)
            with Path(paths["csv"]).open(newline="") as stream:
                self.assertIn(["historical_rows", "6"], list(csv.reader(stream)))
            report = Path(paths["markdown"]).read_text()
            self.assertEqual(report, markdown_report(result))
            self.assertIn("Full refresh", report)


if __name__ == "__main__":
    unittest.main()
