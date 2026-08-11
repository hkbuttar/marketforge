"""Deterministic ingestion count and partition-delta reconciliation."""

from __future__ import annotations

from dataclasses import dataclass


class ReconciliationError(RuntimeError):
    """Ingestion accounting cannot explain the observed canonical result."""


@dataclass(frozen=True)
class ReconciliationResult:
    status: str
    source_records_fetched: int
    records_accepted: int
    records_rejected: int
    records_deduplicated: int
    records_written: int
    pre_write_row_count: int
    post_write_row_count: int
    expected_row_delta: int
    actual_row_delta: int
    discrepancies: tuple[str, ...]


def reconcile_run(
    *, fetched: int, accepted: int, rejected: int, deduplicated: int,
    written: int, pre_write_rows: int, post_write_rows: int,
) -> ReconciliationResult:
    discrepancies = []
    if fetched != accepted + rejected + deduplicated:
        discrepancies.append(
            "fetched != accepted + rejected + deduplicated "
            f"({fetched} != {accepted} + {rejected} + {deduplicated})"
        )
    if written != accepted:
        discrepancies.append(f"records_written != records_accepted ({written} != {accepted})")
    actual_delta = post_write_rows - pre_write_rows
    if actual_delta != accepted:
        discrepancies.append(
            f"post-write row delta != expected delta ({actual_delta} != {accepted})"
        )
    return ReconciliationResult(
        status="failed" if discrepancies else "passed",
        source_records_fetched=fetched,
        records_accepted=accepted,
        records_rejected=rejected,
        records_deduplicated=deduplicated,
        records_written=written,
        pre_write_row_count=pre_write_rows,
        post_write_row_count=post_write_rows,
        expected_row_delta=accepted,
        actual_row_delta=actual_delta,
        discrepancies=tuple(discrepancies),
    )
