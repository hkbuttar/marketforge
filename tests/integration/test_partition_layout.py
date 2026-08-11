import tempfile
import unittest
from pathlib import Path

import duckdb

from benchmarks.partition_layout import benchmark_layouts


class PartitionLayoutTests(unittest.TestCase):
    def test_three_layouts_preserve_rows_and_report_workloads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "raw/prices/year=2026/month=07"
            target.mkdir(parents=True)
            file = target / "part-test.parquet"
            with duckdb.connect() as connection:
                connection.execute(f"""COPY (
                    SELECT symbol, day::DATE date, 100.0 open, 102.0 high, 99.0 low,
                           101.0 AS "close", 1000::BIGINT volume, 'tiingo' AS "source",
                           symbol || ':' || day::DATE::VARCHAR source_record_id,
                           TIMESTAMPTZ '2026-08-11 00:00:00+00' ingested_at
                    FROM (VALUES ('AAPL'), ('MSFT')) symbols(symbol)
                    CROSS JOIN (VALUES (DATE '2026-07-31'), (DATE '2026-08-03')) dates(day)
                ) TO '{file}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
            result = benchmark_layouts(root / "raw", root / "layouts", iterations=1)
            self.assertEqual(result["rows"], 4)
            self.assertEqual(result["layouts"]["single_file"]["files"], 1)
            self.assertEqual(result["layouts"]["year_month"]["files"], 2)
            self.assertEqual(result["layouts"]["year_month_symbol"]["files"], 4)
            for layout in result["layouts"].values():
                self.assertEqual(len(layout["queries"]), 4)


if __name__ == "__main__":
    unittest.main()
