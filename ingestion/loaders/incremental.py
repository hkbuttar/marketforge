"""Checkpoint-driven incremental ingestion, separate from historical backfill."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from ingestion.checkpoints import CheckpointStore
from ingestion.contracts import CONTRACTS
from ingestion.contracts.base import iso_date, utc_datetime
from ingestion.loaders.backfill import BackfillResult, EVENT_FIELDS, FailureHook, run_backfill


@dataclass(frozen=True)
class IncrementalResult:
    fetch_from: date
    fetch_through: date
    fetched_rows: int
    backfill: BackfillResult
    checkpoint_event_date: date | None


def event_date(dataset: str, row: Mapping[str, Any]) -> date:
    contract = CONTRACTS[dataset]
    field = EVENT_FIELDS[dataset]
    candidates = (field, *(alias for alias, target in contract.aliases.items() if target == field))
    value = next((row[name] for name in candidates if name in row), None)
    if value is None:
        raise ValueError(f"record has no event field; expected one of {candidates}")
    parsed = utc_datetime(value).date() if field == "event_timestamp" else iso_date(value)
    return parsed


def calculate_fetch_window(
    *, checkpoint_date: date | None, initial_start: date | None,
    through: date, overlap_days: int,
) -> tuple[date, date]:
    if overlap_days < 0:
        raise ValueError("overlap_days must be non-negative")
    if checkpoint_date:
        fetch_from = (
            checkpoint_date + timedelta(days=1)
            if overlap_days == 0
            else checkpoint_date - timedelta(days=overlap_days - 1)
        )
    elif initial_start:
        fetch_from = initial_start
    else:
        raise ValueError("initial_start is required when no checkpoint exists")
    if through < fetch_from and checkpoint_date is None:
        raise ValueError("through date must not precede fetch start")
    return fetch_from, through


def run_incremental(
    dataset: str,
    records: Iterable[Mapping[str, Any]],
    *,
    source: str,
    checkpoint_store: CheckpointStore,
    initial_start: date | None = None,
    through: date | None = None,
    overlap_days: int = 0,
    raw_root: Path = Path("data/raw"),
    quarantine_root: Path = Path("data/quarantine"),
    metadata_root: Path = Path("warehouse/metadata/ingestion_runs"),
    run_id: str | None = None,
    now: datetime | None = None,
    failure_hook: FailureHook | None = None,
) -> IncrementalResult:
    if dataset not in CONTRACTS:
        raise ValueError(f"unknown dataset {dataset!r}")
    now = now or datetime.now(timezone.utc)
    through = through or now.date()
    checkpoint = checkpoint_store.get(dataset, source)
    fetch_from, through = calculate_fetch_window(
        checkpoint_date=checkpoint.last_successful_event_date if checkpoint else None,
        initial_start=initial_start, through=through, overlap_days=overlap_days,
    )

    supplied = [dict(row) for row in records]
    selected = []
    for row in supplied:
        try:
            in_window = fetch_from <= event_date(dataset, row) <= through
        except (TypeError, ValueError, OverflowError):
            # Let the contract create the structured quarantine diagnostic.
            in_window = True
        if in_window:
            selected.append(row)
    result = run_backfill(
        dataset,
        selected,
        source=source,
        raw_root=raw_root,
        quarantine_root=quarantine_root,
        metadata_root=metadata_root,
        run_id=run_id,
        now=now,
        failure_hook=failure_hook,
        run_type="incremental",
        requested_start=fetch_from,
        requested_end=through,
        late_event_cutoff=checkpoint.last_successful_event_date if checkpoint else None,
    )
    checkpoint_date = checkpoint.last_successful_event_date if checkpoint else None
    if result.max_event_date is not None:
        advanced = checkpoint_store.advance(
            dataset, source, date.fromisoformat(result.max_event_date), result.run_id
        )
        checkpoint_date = advanced.last_successful_event_date
    return IncrementalResult(fetch_from, through, len(selected), result, checkpoint_date)
