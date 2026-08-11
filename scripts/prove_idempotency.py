#!/usr/bin/env python3
"""Run a price ingestion twice and emit machine-readable idempotency evidence."""

from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingestion.loaders import run_backfill


def snapshot(root: Path) -> tuple[int, int, str]:
    glob = str(root / "prices" / "**/*.parquet")
    with duckdb.connect() as connection:
        rows = connection.execute(
            """SELECT symbol, date, open, high, low, close, volume, source
               FROM read_parquet(?) ORDER BY symbol, date, source""",
            [glob],
        ).fetchall()
        duplicate_keys = connection.execute(
            """SELECT count(*) FROM (
                 SELECT symbol, date, source FROM read_parquet(?)
                 GROUP BY symbol, date, source HAVING count(*) > 1
               )""",
            [glob],
        ).fetchone()[0]
    digest = hashlib.sha256(json.dumps(rows, default=str, separators=(",", ":")).encode()).hexdigest()
    return len(rows), duplicate_keys, digest


def main() -> None:
    records = [
        {"symbol": "AAPL", "date": "2026-08-10", "open": 100, "high": 102,
         "low": 99, "close": 101, "volume": 1000, "source_record_id": "aapl-0810"},
        {"symbol": "MSFT", "date": "2026-08-10", "open": 200, "high": 203,
         "low": 199, "close": 202, "volume": 900, "source_record_id": "msft-0810"},
    ]
    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        options = {
            "source": "proof-provider", "raw_root": root / "raw",
            "quarantine_root": root / "quarantine", "metadata_root": root / "runs", "now": now,
        }
        first = run_backfill("prices", records, run_id="proof-first", **options)
        before = snapshot(root / "raw")
        second = run_backfill("prices", records, run_id="proof-second", **options)
        after = snapshot(root / "raw")
        evidence = {
            "first_run_rows_written": first.accepted_rows,
            "second_run_rows_written": second.accepted_rows,
            "second_run_duplicates_recognized": second.duplicate_rows,
            "logical_rows_before": before[0],
            "logical_rows_after": after[0],
            "duplicate_natural_keys_after": after[1],
            "canonical_checksum_before": before[2],
            "canonical_checksum_after": after[2],
            "proved": before == after and second.accepted_rows == 0 and second.duplicate_rows == len(records),
        }
        print(json.dumps(evidence, indent=2))
        if not evidence["proved"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
