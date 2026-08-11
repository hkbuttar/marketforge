"""MarketForge ingestion command line interface."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

from ingestion.contracts import CONTRACTS
from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_backfill, run_incremental
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
    run = commands.add_parser("run", help="run checkpoint-driven incremental ingestion")
    run.add_argument("--dataset", required=True, choices=sorted(CONTRACTS))
    run.add_argument("--source", required=True)
    run.add_argument("--input", required=True, help="extract containing the available date range")
    run.add_argument("--format", choices=("csv", "json", "jsonl"))
    run.add_argument("--start-date", type=date.fromisoformat)
    run.add_argument("--through-date", type=date.fromisoformat)
    run.add_argument("--overlap-days", type=int, default=0)
    run.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    run.add_argument("--state-db", type=Path, default=Path("warehouse/metadata/checkpoints.sqlite"))
    return root


def main() -> int:
    args = parser().parse_args()
    records = read_records(args.input, args.format)
    local = Path(args.input)
    input_bytes = local.stat().st_size if local.is_file() else 0
    if args.command == "backfill":
        result = run_backfill(
            args.dataset, records, source=args.source, raw_root=args.raw_root, input_bytes=input_bytes
        )
    else:
        result = run_incremental(
            args.dataset,
            records,
            source=args.source,
            checkpoint_store=CheckpointStore(args.state_db),
            initial_start=args.start_date,
            through=args.through_date,
            overlap_days=args.overlap_days,
            raw_root=args.raw_root,
        )
    print(json.dumps(asdict(result), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
