"""Dataset-specific freshness and source-check SLA evaluation."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class FreshnessPolicy:
    basis: str
    degraded_after: timedelta
    failed_after: timedelta
    cadence: str


@dataclass(frozen=True)
class FreshnessResult:
    dataset: str
    status: str
    reason: str
    basis: str
    cadence: str
    age_hours: float | None
    evaluated_at: str
    latest_event_time: str | None
    last_source_check_at: str | None
    expected_event_time: str | None


POLICIES = {
    "prices": FreshnessPolicy("expected_market_event", timedelta(hours=24), timedelta(hours=48), "U.S. market days after close"),
    "fundamentals": FreshnessPolicy("source_check", timedelta(days=7), timedelta(days=14), "daily source check; quarterly events"),
    "earnings": FreshnessPolicy("source_check", timedelta(days=1), timedelta(days=2), "weekday source check"),
    "macro": FreshnessPolicy("source_check", timedelta(days=2), timedelta(days=7), "daily publication-calendar check"),
    "news": FreshnessPolicy("event_age", timedelta(hours=24), timedelta(hours=48), "four-hour metadata batches"),
}


def _aware(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        if len(value) == 10:
            return datetime.combine(date.fromisoformat(value), time(), timezone.utc)
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError("freshness timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _previous_weekday(value: date) -> date:
    while value.weekday() >= 5:
        value -= timedelta(days=1)
    return value


def expected_price_event_time(evaluated_at: datetime) -> datetime:
    local = evaluated_at.astimezone(ZoneInfo("America/Chicago"))
    candidate = local.date()
    if candidate.weekday() >= 5 or local.time() < time(16, 15):
        candidate -= timedelta(days=1)
    candidate = _previous_weekday(candidate)
    return datetime.combine(candidate, time(16), ZoneInfo("America/Chicago")).astimezone(timezone.utc)


def evaluate_freshness(
    dataset: str, *, latest_event_time: datetime | str | None,
    last_source_check_at: datetime | str | None, evaluated_at: datetime | None = None,
) -> FreshnessResult:
    if dataset not in POLICIES:
        raise ValueError(f"unknown dataset {dataset!r}")
    evaluated_at = _aware(evaluated_at or datetime.now(timezone.utc))
    if dataset == "prices" and isinstance(latest_event_time, str) and len(latest_event_time) == 10:
        latest = datetime.combine(
            date.fromisoformat(latest_event_time), time(16), ZoneInfo("America/Chicago")
        ).astimezone(timezone.utc)
    else:
        latest = _aware(latest_event_time)
    checked = _aware(last_source_check_at)
    policy = POLICIES[dataset]
    expected = expected_price_event_time(evaluated_at) if dataset == "prices" else None
    reference = expected if policy.basis == "expected_market_event" else evaluated_at
    observed = checked if policy.basis == "source_check" else latest
    if observed is None:
        status, age, reason = "UNKNOWN", None, f"no {policy.basis.replace('_', ' ')} evidence"
    else:
        age_delta = max(reference - observed, timedelta())
        age = round(age_delta.total_seconds() / 3600, 3)
        if age_delta >= policy.failed_after:
            status = "FAILED"
        elif age_delta >= policy.degraded_after:
            status = "DEGRADED"
        else:
            status = "HEALTHY"
        reason = (
            f"{policy.basis.replace('_', ' ')} age is {age:.1f}h; "
            f"degraded after {policy.degraded_after.total_seconds() / 3600:.1f}h and "
            f"failed after {policy.failed_after.total_seconds() / 3600:.1f}h"
        )
    return FreshnessResult(
        dataset, status, reason, policy.basis, policy.cadence, age,
        evaluated_at.isoformat(), latest.isoformat() if latest else None,
        checked.isoformat() if checked else None, expected.isoformat() if expected else None,
    )


def write_freshness_audit(result: FreshnessResult, root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"{result.dataset}.json"
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(asdict(result), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)
    return target
