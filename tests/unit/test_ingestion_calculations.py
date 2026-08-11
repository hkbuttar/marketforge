import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.contracts import PRICES_CONTRACT
from ingestion.loaders.backfill import (
    IdempotencyConflictError,
    _stable_record_id,
    deduplicate_rows,
    partition_paths,
)
from ingestion.loaders.incremental import calculate_fetch_window


def canonical_price(close=101.0, record_id="p1"):
    return {
        "symbol": "AAPL", "date": date(2026, 8, 10), "open": 100.0,
        "high": 102.0, "low": 99.0, "close": close, "volume": 1000,
        "source": "tiingo", "source_record_id": record_id,
        "ingested_at": datetime(2026, 8, 11, tzinfo=timezone.utc),
    }


class IngestionCalculationTests(unittest.TestCase):
    def test_stable_hash_ignores_mapping_order_but_not_content(self):
        first = {"symbol": "AAPL", "date": "2026-08-10"}
        reordered = {"date": "2026-08-10", "symbol": "AAPL"}
        self.assertEqual(_stable_record_id(first), _stable_record_id(reordered))
        self.assertNotEqual(_stable_record_id(first), _stable_record_id({**first, "symbol": "MSFT"}))
        self.assertEqual(len(_stable_record_id(first)), 64)

    def test_partition_paths_are_monthly_and_staged_outside_canonical_tree(self):
        staged, final = partition_paths(Path("lake"), "prices", date(2026, 8, 31), "run-1")
        self.assertEqual(final, Path("lake/prices/year=2026/month=08/part-run-1.parquet"))
        self.assertEqual(staged, Path("lake/.tmp/run-1/prices/year=2026/month=08/part-run-1.parquet"))

    def test_dedup_accepts_new_replay_and_rejects_changed_values(self):
        row = canonical_price()
        existing = {}
        new, duplicates = deduplicate_rows([row], existing, PRICES_CONTRACT)
        self.assertEqual((new, duplicates), ([row], 0))
        replay = canonical_price(record_id="provider-replay-id")
        self.assertEqual(deduplicate_rows([replay], existing, PRICES_CONTRACT), ([], 1))
        with self.assertRaises(IdempotencyConflictError):
            deduplicate_rows([canonical_price(close=999.0)], existing, PRICES_CONTRACT)

    def test_checkpoint_windows_cover_initial_next_day_and_overlap(self):
        through = date(2026, 8, 11)
        self.assertEqual(calculate_fetch_window(
            checkpoint_date=None, initial_start=date(2026, 1, 1), through=through,
            overlap_days=0), (date(2026, 1, 1), through))
        self.assertEqual(calculate_fetch_window(
            checkpoint_date=date(2026, 8, 10), initial_start=None, through=through,
            overlap_days=0), (date(2026, 8, 11), through))
        self.assertEqual(calculate_fetch_window(
            checkpoint_date=date(2026, 8, 10), initial_start=None, through=through,
            overlap_days=3), (date(2026, 8, 8), through))

    def test_checkpoint_window_rejects_missing_baseline_and_negative_overlap(self):
        with self.assertRaises(ValueError):
            calculate_fetch_window(checkpoint_date=None, initial_start=None,
                                   through=date(2026, 8, 11), overlap_days=0)
        with self.assertRaises(ValueError):
            calculate_fetch_window(checkpoint_date=date(2026, 8, 10), initial_start=None,
                                   through=date(2026, 8, 11), overlap_days=-1)


if __name__ == "__main__":
    unittest.main()
