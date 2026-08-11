import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
from hypothesis import given, settings, strategies as st

from backend.services.health import REQUIRED_MARTS, readiness
from ingestion.checkpoints import CheckpointStore
from ingestion.contracts import PRICES_CONTRACT
from ingestion.loaders import run_backfill, run_incremental
from quality.reconciliation import reconcile_run


DATES = st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31))
PRICES = st.floats(min_value=1, max_value=10_000, allow_nan=False, allow_infinity=False)


def price(day: date, close: float, *, symbol: str = "AAPL"):
    spread = max(0.01, close * 0.01)
    return {
        "symbol": symbol, "date": day.isoformat(), "open": close,
        "high": close + spread, "low": max(0.001, close - spread),
        "close": close, "volume": 1000, "source_record_id": f"{symbol}:{day}",
    }


class PlatformInvariantTests(unittest.TestCase):
    @settings(max_examples=12, deadline=None)
    @given(days=st.lists(DATES, min_size=1, max_size=6, unique=True), close=PRICES)
    def test_rerun_equals_original_canonical_result(self, days, close):
        rows = [price(day, close) for day in days]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            options = {"source": "generated", "raw_root": root / "raw",
                       "quarantine_root": root / "quarantine", "metadata_root": root / "runs"}
            first = run_backfill("prices", rows, run_id="first", **options)
            replay = run_backfill("prices", rows, run_id="replay", **options)
            pattern = str(root / "raw/prices/year=*/month=*/*.parquet")
            with duckdb.connect() as connection:
                count, unique = connection.execute("""SELECT count(*),
                    count(DISTINCT (symbol, date, source)) FROM read_parquet(?)""", [pattern]).fetchone()
            self.assertEqual(first.accepted_rows, len(rows))
            self.assertEqual(replay.accepted_rows, 0)
            self.assertEqual(replay.duplicate_rows, len(rows))
            self.assertEqual((count, unique), (len(rows), len(rows)))

    @settings(max_examples=30)
    @given(accepted=st.integers(0, 10_000), rejected=st.integers(0, 10_000),
           deduplicated=st.integers(0, 10_000), pre=st.integers(0, 1_000_000))
    def test_reconciliation_conserves_every_fetched_record(
        self, accepted, rejected, deduplicated, pre
    ):
        fetched = accepted + rejected + deduplicated
        result = reconcile_run(
            fetched=fetched, accepted=accepted, rejected=rejected,
            deduplicated=deduplicated, written=accepted,
            pre_write_rows=pre, post_write_rows=pre + accepted,
        )
        self.assertEqual(result.status, "passed")
        self.assertEqual(
            result.source_records_fetched,
            result.records_accepted + result.records_rejected + result.records_deduplicated,
        )

    @settings(max_examples=8, deadline=None)
    @given(day=DATES, close=PRICES)
    def test_checkpoint_never_advances_after_failed_durable_write(self, day, close):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = CheckpointStore(root / "checkpoint.sqlite")

            def fail(stage, _path):
                if stage == "after_temp_validation":
                    raise RuntimeError("generated durable-write failure")

            with self.assertRaises(RuntimeError):
                run_incremental(
                    "prices", [price(day, close)], source="generated",
                    checkpoint_store=store, initial_start=day, through=day,
                    raw_root=root / "raw", quarantine_root=root / "quarantine",
                    metadata_root=root / "runs", run_id="failed",
                    now=datetime(2031, 1, 1, tzinfo=timezone.utc), failure_hook=fail,
                )
            self.assertIsNone(store.get("prices", "generated"))
            self.assertFalse(list((root / "raw/prices").glob("**/*.parquet")))

    @settings(max_examples=20)
    @given(close=PRICES, invalid=st.booleans())
    def test_price_contract_acceptance_partition_is_total(self, close, invalid):
        row = price(date(2026, 8, 10), close)
        if invalid:
            row["high"] = row["close"] - 0.01
        row.update({"source": "generated", "ingested_at": "2026-08-11T00:00:00Z"})
        result = PRICES_CONTRACT.validate(
            [row], source="generated", ingestion_run_id="property",
            received_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
        )
        self.assertEqual(len(result.accepted) + len(result.rejected), 1)
        self.assertEqual(len(result.rejected), int(invalid))

    @settings(max_examples=8, deadline=None)
    @given(available=st.sets(st.sampled_from(sorted(REQUIRED_MARTS)), max_size=2))
    def test_missing_required_mart_can_never_report_ready(self, available):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "marts.duckdb"
            with duckdb.connect(str(database)) as connection:
                connection.execute("CREATE SCHEMA main_marts")
                for mart in available:
                    connection.execute(f'CREATE TABLE main_marts."{mart}" (marker INTEGER)')
            metadata = root / "operational.sqlite"
            with sqlite3.connect(metadata) as connection:
                connection.execute("CREATE TABLE marker (value INTEGER)")
            checks = readiness(database, metadata)
            required = next(item for item in checks if item["component"] == "required_marts")
            self.assertEqual(required["status"], "failed")


if __name__ == "__main__":
    unittest.main()
