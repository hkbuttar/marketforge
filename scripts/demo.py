#!/usr/bin/env python3
"""Run the reproducible MarketForge end-to-end demonstration scenario."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from ingestion.checkpoints import CheckpointStore
from ingestion.loaders import run_backfill, run_incremental
from ingestion.sources.files import read_records
from observability.freshness import evaluate_freshness


ROOT = Path(__file__).parents[1]
NOW = datetime(2026, 8, 11, 22, tzinfo=timezone.utc)


def _price(day: str, record_id: str, close: float = 100.0, symbol: str = "AAPL") -> dict[str, Any]:
    return {
        "symbol": symbol, "date": day, "open": close - 1, "high": close + 1,
        "low": close - 2, "close": close, "volume": 1_000,
        "source_record_id": record_id,
    }


def _count(pattern: Path) -> int:
    files = list(pattern.parent.glob(pattern.name))
    if not files:
        return 0
    with duckdb.connect() as connection:
        return connection.execute(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(pattern)]
        ).fetchone()[0]


def _dbt_build(work: Path, raw: Path, metadata: Path, select: str) -> dict[str, Any]:
    executable = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    if not Path(executable).is_file():
        raise RuntimeError("dbt is required for the demonstration; install project requirements")
    profiles = work / "profiles"
    profiles.mkdir(exist_ok=True)
    database = work / "demo.duckdb"
    (profiles / "profiles.yml").write_text(
        "marketforge:\n  target: demo\n  outputs:\n    demo:\n      type: duckdb\n"
        f"      path: '{database}'\n      schema: main\n      threads: 2\n",
        encoding="utf-8",
    )
    command = [
        executable, "build", "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(profiles),
        "--vars", json.dumps({"raw_root": str(raw), "metadata_root": str(metadata)}),
        "--select", select, "--indirect-selection", "cautious", "--no-use-colors",
    ]
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    if completed.returncode:
        raise RuntimeError(completed.stdout + completed.stderr)
    return {"selection": select, "status": "success", "database": str(database)}


def run_demo(work: Path, *, transform: bool = True) -> dict[str, Any]:
    raw = work / "raw"
    metadata = work / "metadata"
    quarantine = work / "quarantine"
    checkpoints = CheckpointStore(metadata / "checkpoints.sqlite")
    options = {
        "source": "demo-provider", "checkpoint_store": checkpoints,
        "raw_root": raw, "quarantine_root": quarantine,
        "metadata_root": metadata / "ingestion_runs", "now": NOW,
    }

    initial_prices = [_price("2026-08-07", "initial-07", 98), _price("2026-08-10", "initial-10", 100)]
    initial = run_incremental(
        "prices", initial_prices, initial_start=date(2026, 8, 7), through=date(2026, 8, 10),
        run_id="demo-initial", **options,
    )
    # dbt's raw-source bootstrap requires every contracted source. These tiny
    # retained fixtures keep unrelated branches available throughout the demo.
    for dataset in ("fundamentals", "earnings", "macro", "news"):
        run_backfill(
            dataset, read_records(str(ROOT / f"tests/fixtures/ci/{dataset}.jsonl")),
            source="test-provider", raw_root=raw, quarantine_root=quarantine,
            metadata_root=metadata / "ingestion_runs", run_id=f"demo-seed-{dataset}", now=NOW,
        )
    initial_transform = _dbt_build(work, raw, metadata, "+mart_security_daily") if transform else {
        "selection": "+mart_security_daily", "status": "not_run"
    }

    aug11 = run_incremental(
        "prices", [_price("2026-08-11", "event-11", 103)], through=date(2026, 8, 11),
        run_id="demo-aug11", **options,
    )
    aug11_transform = _dbt_build(work, raw, metadata, "+mart_security_daily") if transform else {
        "selection": "+mart_security_daily", "status": "not_run"
    }
    freshness = evaluate_freshness(
        "prices", latest_event_time=aug11.backfill.max_event_date,
        last_source_check_at=aug11.backfill.completed_at, evaluated_at=NOW,
    )

    malformed = {
        "symbol": "AAPL", "period_end": "2026-06-30", "filed_at": "2026-08-01T00:00:00Z",
        "metric_name": "revenue", "period_start": "2026-04-01", "period_type": "quarter",
        "value": "not-numeric", "unit": "USD", "currency": "USD",
        "source_record_id": "bad-fundamental",
    }
    degraded = run_backfill(
        "fundamentals", [malformed], source="demo-provider", raw_root=raw,
        quarantine_root=quarantine, metadata_root=metadata / "ingestion_runs",
        run_id="demo-malformed", now=NOW,
    )
    other_datasets = {
        dataset: bool(list((raw / dataset).glob("**/*.parquet")))
        for dataset in ("prices", "earnings", "macro", "news")
    }

    late = run_incremental(
        "prices", [_price("2026-08-08", "correction-08", 99)], through=date(2026, 8, 11),
        overlap_days=4, run_id="demo-late-correction", **options,
    )
    late_transform = _dbt_build(work, raw, metadata, "+mart_security_daily") if transform else {
        "selection": "+mart_security_daily", "status": "not_run"
    }

    replay = run_incremental(
        "prices", [_price("2026-08-11", "event-11", 103)], through=date(2026, 8, 11),
        overlap_days=4, run_id="demo-replay", **options,
    )

    crash_row = _price("2026-08-12", "event-12", 104, "MSFT")

    def crash(stage: str, _path: Path) -> None:
        if stage == "after_temp_write":
            raise RuntimeError("injected process termination")

    checkpoint_before = checkpoints.get("prices", "demo-provider").last_successful_event_date
    crashed = False
    try:
        run_incremental(
            "prices", [crash_row], through=date(2026, 8, 12), overlap_days=4,
            run_id="demo-crash", failure_hook=crash, **options,
        )
    except RuntimeError as error:
        crashed = str(error) == "injected process termination"
    canonical_before_retry = _count(raw / "prices/year=2026/month=08/part-demo-crash.parquet")
    checkpoint_after_crash = checkpoints.get("prices", "demo-provider").last_successful_event_date
    recovered = run_incremental(
        "prices", [crash_row], through=date(2026, 8, 12), overlap_days=4,
        run_id="demo-crash", **options,
    )
    checkpoint_after_retry = checkpoints.get("prices", "demo-provider").last_successful_event_date

    result = {
        "schema_version": 1,
        "scenario": "marketforge-end-to-end",
        "initial_state": {
            "latest_price_date": initial.backfill.max_event_date,
            "checkpoint": initial.checkpoint_event_date.isoformat(),
            "pipeline_healthy": initial.backfill.status == "success",
            "transformation": initial_transform,
        },
        "events": [
            {"event": "new_aug11_prices", "accepted_rows": aug11.backfill.accepted_rows,
             "files_written": aug11.backfill.files_written, "freshness": freshness.status,
             "transformation": aug11_transform},
            {"event": "malformed_fundamental", "status": degraded.status,
             "quarantined_rows": degraded.quarantined_rows,
             "other_datasets_available": other_datasets},
            {"event": "late_aug8_correction", "accepted_rows": late.backfill.accepted_rows,
             "late_arriving_rows": late.backfill.late_arriving_rows,
             "earliest_late_event_date": late.backfill.earliest_late_event_date,
             "checkpoint": late.checkpoint_event_date.isoformat(),
             "audit_manifest": "ingestion_runs/demo-late-correction.json",
             "audit_recorded": (metadata / "ingestion_runs/demo-late-correction.json").exists(),
             "transformation": late_transform},
            {"event": "idempotent_aug11_replay", "accepted_rows": replay.backfill.accepted_rows,
             "duplicate_rows": replay.backfill.duplicate_rows,
             "files_written": replay.backfill.files_written},
            {"event": "crash_and_recovery", "failure_injected": crashed,
             "canonical_rows_before_retry": canonical_before_retry,
             "checkpoint_before": checkpoint_before.isoformat(),
             "checkpoint_after_crash": checkpoint_after_crash.isoformat(),
             "recovered_rows": recovered.backfill.accepted_rows,
             "checkpoint_after_retry": checkpoint_after_retry.isoformat()},
        ],
    }
    invariants = {
        "initial_current_through_aug10": result["initial_state"]["latest_price_date"] == "2026-08-10",
        "aug11_current": freshness.status == "HEALTHY" and aug11.backfill.accepted_rows == 1,
        "malformed_row_isolated": degraded.quarantined_rows == 1 and all(other_datasets.values()),
        "late_arrival_audited": late.backfill.late_arriving_rows == 1
        and (metadata / "ingestion_runs/demo-late-correction.json").exists(),
        "checkpoint_monotonic_after_late_data": late.checkpoint_event_date == date(2026, 8, 11),
        "replay_writes_zero_rows": replay.backfill.accepted_rows == 0 and replay.backfill.files_written == 0,
        "crash_published_no_partial_file": crashed and canonical_before_retry == 0,
        "crash_did_not_advance_checkpoint": checkpoint_after_crash == checkpoint_before,
        "restart_recovered_once": recovered.backfill.accepted_rows == 1 and checkpoint_after_retry == date(2026, 8, 12),
    }
    result["invariants"] = invariants
    result["success"] = all(invariants.values())
    return result


def _markdown(result: dict[str, Any]) -> str:
    rows = []
    for event in result["events"]:
        evidence = ", ".join(f"{key}={value}" for key, value in event.items() if key not in {"event", "transformation"})
        rows.append(f"| {event['event'].replace('_', ' ')} | {evidence} |")
    checks = "\n".join(f"- [{'x' if passed else ' '}] {name.replace('_', ' ')}" for name, passed in result["invariants"].items())
    return f"""# MarketForge end-to-end demonstration

Result: **{'PASS' if result['success'] else 'FAIL'}**

| Event | Evidence |
| --- | --- |
{os.linesep.join(rows)}

## Invariants

{checks}
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("demo/results"))
    parser.add_argument("--skip-dbt", action="store_true", help="Exercise data-state transitions without dbt")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="marketforge-demo-") as directory:
        result = run_demo(Path(directory), transform=not args.skip_dbt)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "latest.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    report = _markdown(result)
    (args.output_dir / "latest.md").write_text(report, encoding="utf-8")
    print(report)
    print(f"Evidence: {args.output_dir / 'latest.json'}")
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
