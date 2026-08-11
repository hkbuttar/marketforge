import tempfile
import unittest
from pathlib import Path

import duckdb

from benchmarks.storage_efficiency import FORMATS, benchmark_storage


class StorageEfficiencyTests(unittest.TestCase):
    def test_all_formats_preserve_rows_and_report_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "raw/prices/year=2026/month=08"
            target.mkdir(parents=True)
            with duckdb.connect() as connection:
                connection.execute(
                    f"""COPY (SELECT 'AAPL' symbol, DATE '2026-08-10' date,
                        100.0 open, 102.0 high, 99.0 low, 101.0 AS "close", 1000::BIGINT volume,
                        'tiingo' AS "source", 'AAPL:2026-08-10' source_record_id,
                        TIMESTAMPTZ '2026-08-11 00:00:00+00' ingested_at)
                        TO '{target / 'part-test.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
                )
            result = benchmark_storage(root / "raw", root / "output", iterations=1)
            self.assertEqual(result["rows"], 1)
            self.assertEqual(set(result["formats"]), set(FORMATS))
            for metrics in result["formats"].values():
                self.assertGreater(metrics["bytes"], 0)
                self.assertGreaterEqual(metrics["write_ms"], 0)
                self.assertGreaterEqual(metrics["read_median_ms"], 0)
                self.assertGreaterEqual(metrics["aggregation_median_ms"], 0)


if __name__ == "__main__":
    unittest.main()
