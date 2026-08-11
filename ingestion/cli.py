"""MarketForge ingestion command line interface."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from ingestion.contracts import CONTRACTS
from ingestion.loaders import run_backfill
from ingestion.sources.files import read_records


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="marketforge-ingest")
    commands = root.add_subparsers(dest="command", required=True)
    backfill = commands.add_parser("backfill", help="load a bounded historical extract")
    backfill.add_argument("--dataset", required=True, choices=sorted(CONTRACTS))
    backfill.add_argument("--source", required=True)
    backfill.add_argument("--input", required=True, help="local path, file URL, or HTTP(S) URL")
    backfill.add_argument("--format", choices=("csv", "json", "jsonl"))
    backfill.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    return root


def main() -> int:
    args = parser().parse_args()
    records = read_records(args.input, args.format)
    local = Path(args.input)
    input_bytes = local.stat().st_size if local.is_file() else 0
    result = run_backfill(
        args.dataset,
        records,
        source=args.source,
        raw_root=args.raw_root,
        input_bytes=input_bytes,
    )
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
