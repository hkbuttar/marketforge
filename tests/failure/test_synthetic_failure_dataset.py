import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.checkpoints import CheckpointStore
from ingestion.contracts import PRICES_CONTRACT
from ingestion.contracts.base import MissingRequiredFieldError
from ingestion.loaders import run_incremental
from quality.security_master import load_symbols, unknown_symbols


ROOT = Path(__file__).parents[2]
CASES = json.loads((ROOT / "tests/fixtures/failures/prices.json").read_text())
BASE = {
    "symbol": "AAPL", "date": "2026-08-10", "open": 100, "high": 102,
    "low": 99, "close": 101, "volume": 1000, "source": "synthetic-failure",
    "source_record_id": "base", "ingested_at": "2026-08-11T00:00:00Z",
}


def rows(case):
    if case.get("empty"):
        return []
    row = {**BASE, **case.get("overrides", {})}
    for field in case.get("remove", []):
        row.pop(field, None)
    return [dict(row) for _ in range(case.get("repeat", 1))]


class SyntheticFailureDatasetTests(unittest.TestCase):
    def test_corpus_contains_every_planned_failure(self):
        self.assertEqual({case["case"] for case in CASES}, {
            "duplicate_row", "missing_symbol", "negative_volume", "high_below_low",
            "wrong_date_type", "late_arriving_record", "unknown_security",
            "schema_added_column", "schema_missing_column", "unexpected_string_numeric",
            "empty_api_response",
        })

    def test_contract_failure_cases_match_declared_outcomes(self):
        for case in CASES:
            if case["expected"] not in {"quarantine", "hard_failure"}:
                continue
            with self.subTest(case=case["case"]):
                if case["expected"] == "hard_failure":
                    with self.assertRaisesRegex(MissingRequiredFieldError, case["reason"]):
                        PRICES_CONTRACT.validate(
                            rows(case), source="synthetic-failure", ingestion_run_id=case["case"])
                else:
                    result = PRICES_CONTRACT.validate(
                        rows(case), source="synthetic-failure", ingestion_run_id=case["case"])
                    self.assertTrue(result.rejected)
                    self.assertIn(case["reason"], result.rejected[-1].error_message)

    def test_unknown_security_fails_universe_resolution(self):
        case = next(item for item in CASES if item["case"] == "unknown_security")
        known = load_symbols(ROOT / "config/price_universe.txt")
        self.assertEqual(unknown_symbols(rows(case), known), ("ZZZZ_UNKNOWN",))

    def test_late_arrival_is_written_but_checkpoint_remains_monotonic(self):
        case = next(item for item in CASES if item["case"] == "late_arriving_record")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "checkpoint.sqlite")
            store.advance("prices", "synthetic-failure", date(2026, 8, 10), "baseline")
            result = run_incremental(
                "prices", rows(case), source="synthetic-failure", checkpoint_store=store,
                overlap_days=10, through=date(2026, 8, 11), raw_root=root / "raw",
                quarantine_root=root / "quarantine", metadata_root=root / "runs",
                run_id="late", now=datetime(2026, 8, 11, tzinfo=timezone.utc),
            )
            self.assertEqual(result.backfill.late_arriving_rows, 1)
            self.assertEqual(
                store.get("prices", "synthetic-failure").last_successful_event_date,
                date(2026, 8, 10),
            )

    def test_empty_response_creates_no_data_and_does_not_advance_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "checkpoint.sqlite")
            result = run_incremental(
                "prices", [], source="synthetic-failure", checkpoint_store=store,
                initial_start=date(2026, 8, 10), through=date(2026, 8, 11),
                raw_root=root / "raw", quarantine_root=root / "quarantine",
                metadata_root=root / "runs", run_id="empty",
            )
            self.assertEqual(result.fetched_rows, 0)
            self.assertIsNone(store.get("prices", "synthetic-failure"))
            self.assertFalse(list((root / "raw").glob("**/*.parquet")))


if __name__ == "__main__":
    unittest.main()
