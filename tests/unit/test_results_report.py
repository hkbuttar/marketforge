import unittest

from benchmarks.results_report import build_report


class ResultsReportTests(unittest.TestCase):
    def test_report_contains_every_required_comparison(self):
        formats = {name: {"bytes": 10, "write_ms": 1, "aggregation_median_ms": 2,
                          "space_saved_percent": 50}
                   for name in ("csv", "parquet_uncompressed", "parquet_snappy", "parquet_zstd")}
        suite = {
            "historical_rows": 10, "storage": {"formats": formats},
            "full_refresh": {"wall_clock_seconds": 2, "peak_ram_bytes": 20,
                             "bytes_read": 10, "bytes_written": 8},
            "incremental": {"wall_clock_seconds": 1, "peak_ram_bytes": 10,
                            "bytes_read": 2, "bytes_written": 1},
            "incremental_comparison": {"runtime_speedup": 2, "write_reduction_percent": 87.5},
            "compaction": {"file_count_before": 2, "file_count_after": 1,
                           "bytes_before": 20, "bytes_after": 10,
                           "latency_before_ms": 2, "latency_after_ms": 1},
        }
        query = {"one_month_one_security": {"median_ms": 1},
                 "full_history_aggregation": {"median_ms": 2}}
        partitions = {"layouts": {name: {"files": 1, "bytes": 10, "queries": query}
                                   for name in ("single_file", "year_month", "year_month_symbol")}}
        serving = {"endpoints": {"/api/securities": {
            "cold_median_ms": 2, "cold_p95_ms": 3, "warm_median_ms": 1, "warm_p95_ms": 2}}}
        report = build_report(suite, partitions, serving)
        for heading in ("Storage", "Partition strategy", "Processing", "Compaction",
                        "Reliability", "Quality", "Serving", "Where sophistication did not help"):
            self.assertIn(f"## {heading}", report)


if __name__ == "__main__":
    unittest.main()
