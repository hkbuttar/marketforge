"""Fetch bounded Tiingo EOD prices and ingest them into MarketForge."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

from ingestion.loaders import run_backfill
from ingestion.sources.tiingo import TiingoError, fetch_prices


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", required=True, help="comma-separated Tiingo tickers")
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--quarantine-root", type=Path, default=Path("data/quarantine"))
    parser.add_argument("--metadata-root", type=Path, default=Path("warehouse/metadata/ingestion_runs"))
    parser.add_argument("--run-id")
    args = parser.parse_args()
    try:
        rows = fetch_prices(args.tickers.split(","), start=args.start, end=args.end)
    except (TiingoError, ValueError) as exc:
        parser.error(str(exc))
    result = run_backfill(
        "prices", rows, source="tiingo", raw_root=args.raw_root,
        quarantine_root=args.quarantine_root, metadata_root=args.metadata_root,
        run_id=args.run_id,
    )
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
