#!/usr/bin/env python3
"""Run one quota-safe shard of the configured Tiingo daily update."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

from ingestion.loaders import run_backfill
from ingestion.sources.tiingo import TiingoError, fetch_prices


ROOT = Path(__file__).parents[1]


def load_universe(path: Path) -> list[str]:
    symbols = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines()]
    return [symbol for symbol in symbols if symbol and not symbol.startswith("#")]


def shard(symbols: list[str], index: int, size: int) -> list[str]:
    if size < 1:
        raise ValueError("shard size must be positive")
    if index < 0:
        raise ValueError("shard index must be non-negative")
    return symbols[index * size:(index + 1) * size]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", required=True, type=int, help="zero-based shard index")
    parser.add_argument("--shard-size", type=int, default=50)
    parser.add_argument("--through", type=date.fromisoformat, default=date.today())
    parser.add_argument("--overlap-days", type=int, default=7)
    parser.add_argument("--universe", type=Path, default=ROOT / "config/price_universe.txt")
    parser.add_argument("--raw-root", type=Path, default=ROOT / "data/raw")
    parser.add_argument("--quarantine-root", type=Path, default=ROOT / "data/quarantine")
    parser.add_argument("--metadata-root", type=Path, default=ROOT / "warehouse/metadata/ingestion_runs")
    args = parser.parse_args()
    if args.overlap_days < 0:
        parser.error("overlap-days must be non-negative")
    try:
        selected = shard(load_universe(args.universe), args.shard, args.shard_size)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    if not selected:
        parser.error(f"shard {args.shard} is empty")
    start = args.through - timedelta(days=args.overlap_days)
    try:
        rows = fetch_prices(selected, start=start, end=args.through)
    except (TiingoError, ValueError) as exc:
        parser.error(str(exc))
    run_id = f"tiingo-daily-{args.through.isoformat()}-shard-{args.shard}"
    result = run_backfill(
        "prices", rows, source="tiingo", raw_root=args.raw_root,
        quarantine_root=args.quarantine_root, metadata_root=args.metadata_root,
        run_id=run_id,
    )
    output = asdict(result)
    output.update({
        "shard": args.shard, "symbols": selected,
        "requested_start": start.isoformat(), "requested_end": args.through.isoformat(),
    })
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
