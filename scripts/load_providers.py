#!/usr/bin/env python3
"""Load bounded real macro, fundamentals, earnings, or news provider data."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

from ingestion.loaders import run_backfill
from ingestion.sources.businessquant import fetch_earnings
from ingestion.sources.fred import fetch_series
from ingestion.sources.http_json import SourceHTTPError
from ingestion.sources.newsapi import fetch_news
from ingestion.sources.sec_edgar import fetch_fundamentals


DEFAULT_FRED = "CPIAUCSL,UNRATE,FEDFUNDS,GDP,DGS10"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("provider", choices=("fred", "sec", "businessquant", "newsapi"))
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat, default=date.today())
    parser.add_argument("--series", default=DEFAULT_FRED)
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--cik", default="320193")
    parser.add_argument("--query", default='("stock market" OR earnings OR economy)')
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--quarantine-root", type=Path, default=Path("data/quarantine"))
    parser.add_argument("--metadata-root", type=Path, default=Path("warehouse/metadata/ingestion_runs"))
    parser.add_argument("--run-id")
    args = parser.parse_args()
    try:
        if args.provider == "fred":
            start = args.start or date(2021, 1, 1)
            rows = [row for series in args.series.split(",")
                    for row in fetch_series(series.strip(), start=start, end=args.end)]
            dataset, source = "macro", "fred"
        elif args.provider == "sec":
            rows = fetch_fundamentals(args.symbol, args.cik)
            dataset, source = "fundamentals", "sec-edgar"
        elif args.provider == "businessquant":
            rows = fetch_earnings(args.symbol)
            dataset, source = "earnings", "businessquant"
        else:
            start = args.start or args.end
            rows = fetch_news(args.query, start=start, end=args.end, page_size=args.page_size)
            dataset, source = "news", "newsapi"
    except (SourceHTTPError, ValueError) as exc:
        parser.error(str(exc))
    if not rows:
        parser.error(f"{args.provider} returned no usable records")
    result = run_backfill(
        dataset, rows, source=source, raw_root=args.raw_root,
        quarantine_root=args.quarantine_root, metadata_root=args.metadata_root,
        run_id=args.run_id,
    )
    print(json.dumps(asdict(result), indent=2))
    return 0 if result.status in {"success", "degraded"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
