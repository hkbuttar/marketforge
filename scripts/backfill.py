"""Execute an explicitly bounded, overlap-safe historical backfill."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

from ingestion.contracts import CONTRACTS
from ingestion.loaders import BackfillResult, run_backfill
from ingestion.loaders.incremental import event_date
from ingestion.sources.files import read_records


def select_range(
    dataset: str,
    records: Iterable[Mapping[str, Any]],
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    if end < start:
        raise ValueError("end must not precede start")
    selected = []
    for record in records:
        row = dict(record)
        try:
            include = start <= event_date(dataset, row) <= end
        except (TypeError, ValueError, OverflowError):
            # Preserve malformed in-scope provider responses for contract quarantine.
            include = True
        if include:
            selected.append(row)
    return selected


def execute_backfill(
    dataset: str,
    records: Iterable[Mapping[str, Any]],
    *,
    start: date,
    end: date,
    source: str,
    raw_root: Path = Path("data/raw"),
    quarantine_root: Path = Path("data/quarantine"),
    metadata_root: Path = Path("warehouse/metadata/ingestion_runs"),
    run_id: str | None = None,
) -> BackfillResult:
    selected = select_range(dataset, records, start, end)
    return run_backfill(
        dataset,
        selected,
        source=source,
        raw_root=raw_root,
        quarantine_root=quarantine_root,
        metadata_root=metadata_root,
        run_id=run_id,
        run_type="range_backfill",
        requested_start=start,
        requested_end=end,
    )


def rebuild_downstream(dataset: str, dbt_executable: str, raw_root: Path, metadata_root: Path) -> None:
    command = [
        dbt_executable, "build", "--project-dir", "dbt", "--profiles-dir", "dbt",
        "--select", f"source:raw.{dataset}+",
        "--vars", json.dumps({"raw_root": str(raw_root), "metadata_root": str(metadata_root)}),
        "--no-use-colors",
    ]
    subprocess.run(command, check=True)


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--dataset", required=True, choices=sorted(CONTRACTS))
    command.add_argument("--start", required=True, type=date.fromisoformat)
    command.add_argument("--end", required=True, type=date.fromisoformat)
    command.add_argument("--source", required=True)
    command.add_argument("--input", required=True)
    command.add_argument("--format", choices=("csv", "json", "jsonl"))
    command.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    command.add_argument("--quarantine-root", type=Path, default=Path("data/quarantine"))
    command.add_argument("--metadata-root", type=Path, default=Path("warehouse/metadata/ingestion_runs"))
    command.add_argument("--run-id")
    command.add_argument("--skip-downstream", action="store_true")
    command.add_argument("--dbt-executable", default=str(Path(sys.executable).with_name("dbt")))
    return command


def main() -> int:
    command = parser()
    args = command.parse_args()
    try:
        records = read_records(args.input, args.format)
    except FileNotFoundError:
        command.error(
            f"input file not found: {args.input!r}. "
            "Provide a CSV, JSON, or JSONL extract (see extracts/prices.example.jsonl)."
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        command.error(f"unable to read --input {args.input!r}: {exc}")

    result = execute_backfill(
        args.dataset,
        records,
        start=args.start,
        end=args.end,
        source=args.source,
        raw_root=args.raw_root,
        quarantine_root=args.quarantine_root,
        metadata_root=args.metadata_root,
        run_id=args.run_id,
    )
    if not args.skip_downstream:
        rebuild_downstream(args.dataset, args.dbt_executable, args.raw_root, args.metadata_root)
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
