import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.check_mvp import assess, loaded_price_stats


class MvpReadinessTests(unittest.TestCase):
    def test_missing_lake_reports_zero_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(loaded_price_stats(Path(directory)), (0, 0))

    def test_counts_distinct_loaded_tiingo_securities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "data/raw/prices/year=2026/month=08/prices.parquet"
            target.parent.mkdir(parents=True)
            with duckdb.connect() as connection:
                connection.execute(
                    "COPY (SELECT * FROM (VALUES "
                    "('AAPL', 'tiingo'), ('AAPL', 'tiingo'), ('MSFT', 'tiingo'), "
                    "('DEMO', 'synthetic')) AS t(symbol, source)) "
                    "TO ? (FORMAT PARQUET)",
                    [str(target)],
                )
            self.assertEqual(loaded_price_stats(root), (3, 2))

    def test_repository_assessment_exposes_scale_gate(self):
        root = Path(__file__).parents[2]
        _, loaded = loaded_price_stats(root)
        report = assess(root, target=loaded + 1)
        checks = {item["requirement"]: item for item in report["checks"]}
        self.assertEqual(checks["100 securities"]["status"], "FAIL")
        self.assertEqual(report["loaded_securities"], loaded)


if __name__ == "__main__":
    unittest.main()
