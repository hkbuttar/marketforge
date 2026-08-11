import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ingestion.contracts import CONTRACTS, PRICES_CONTRACT
from ingestion.contracts.base import write_quarantine


NOW = "2026-08-11T12:00:00Z"


def valid_price(**updates):
    row = {
        "ticker": " aapl ",
        "trade_date": "2026-08-10",
        "open": "225.0",
        "high": 230,
        "low": 224,
        "close": 228.5,
        "volume": "1000",
        "source": "test-provider",
        "source_record_id": "AAPL:2026-08-10",
        "ingested_at": NOW,
    }
    row.update(updates)
    return row


class SourceContractTests(unittest.TestCase):
    def test_every_initial_domain_declares_contract_metadata(self):
        self.assertEqual(set(CONTRACTS), {"prices", "fundamentals", "earnings", "macro", "news"})
        for contract in CONTRACTS.values():
            self.assertTrue(contract.fields)
            self.assertTrue(contract.unique_by)
            self.assertTrue(contract.idempotency_by)
            self.assertIn("event_time_field", contract.source_metadata)
            self.assertIn("source", contract.fields)
            self.assertIn("source_record_id", contract.fields)
            self.assertIn("ingested_at", contract.fields)

    def test_domain_idempotency_keys_are_explicit(self):
        self.assertEqual(CONTRACTS["prices"].idempotency_by, ("symbol", "date", "source"))
        for domain in ("fundamentals", "earnings", "macro", "news"):
            self.assertEqual(CONTRACTS[domain].idempotency_by, ("source", "source_record_id"))

    def test_valid_price_is_normalized(self):
        result = PRICES_CONTRACT.validate(
            [valid_price()], source="test-provider", ingestion_run_id="run-1"
        )
        self.assertFalse(result.rejected)
        self.assertEqual(result.accepted[0]["symbol"], "AAPL")
        self.assertEqual(result.accepted[0]["volume"], 1000)
        self.assertEqual(result.accepted[0]["ingested_at"].tzinfo, timezone.utc)

    def test_invalid_range_never_enters_accepted_rows(self):
        result = PRICES_CONTRACT.validate(
            [valid_price(high=220)], source="test-provider", ingestion_run_id="run-2"
        )
        self.assertFalse(result.accepted)
        self.assertEqual(result.rejected[0].error_type, "contract_violation")
        self.assertIn("high", result.rejected[0].error_message)

    def test_schema_drift_and_batch_duplicates_are_rejected(self):
        drifted = valid_price(extra_vendor_field="surprise")
        duplicate = valid_price()
        result = PRICES_CONTRACT.validate(
            [valid_price(), drifted, duplicate],
            source="test-provider",
            ingestion_run_id="run-3",
        )
        self.assertEqual(len(result.accepted), 1)
        self.assertEqual(len(result.rejected), 2)
        self.assertIn("unexpected columns", result.rejected[0].error_message)
        self.assertIn("duplicate batch key", result.rejected[1].error_message)

    def test_naive_timestamps_and_source_mismatch_are_rejected(self):
        result = PRICES_CONTRACT.validate(
            [valid_price(ingested_at="2026-08-11T12:00:00")],
            source="test-provider",
            ingestion_run_id="run-4",
        )
        self.assertIn("UTC offset", result.rejected[0].error_message)
        result = PRICES_CONTRACT.validate(
            [valid_price(source="other")],
            source="test-provider",
            ingestion_run_id="run-4b",
        )
        self.assertIn("expected 'test-provider'", result.rejected[0].error_message)

    def test_quarantine_is_structured_and_immutable(self):
        result = PRICES_CONTRACT.validate(
            [valid_price(close=-1)], source="test-provider", ingestion_run_id="run-5"
        )
        with tempfile.TemporaryDirectory() as directory:
            target = write_quarantine(result.rejected, Path(directory))
            self.assertIsNotNone(target)
            payload = json.loads(target.read_text())
            self.assertEqual(
                set(payload),
                {"source", "ingestion_run_id", "error_type", "error_message", "raw_payload", "received_at"},
            )
            with self.assertRaises(FileExistsError):
                write_quarantine(result.rejected, Path(directory))

    def test_received_at_must_have_timezone(self):
        with self.assertRaises(ValueError):
            PRICES_CONTRACT.validate(
                [],
                source="test-provider",
                ingestion_run_id="run-6",
                received_at=datetime(2026, 8, 11),
            )


if __name__ == "__main__":
    unittest.main()
