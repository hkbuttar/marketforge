"""Structured evidence for deterministic failure-recovery drills."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RecoveryRecord:
    scenario: str
    initial_state: dict[str, Any]
    injected_failure: str
    observed_behavior: dict[str, Any]
    retry_action: str
    final_state: dict[str, Any]
    invariants: dict[str, bool]
    recovered: bool
    recorded_at: str


def recovery_record(
    scenario: str, *, initial_state: dict[str, Any], injected_failure: str,
    observed_behavior: dict[str, Any], retry_action: str,
    final_state: dict[str, Any], invariants: dict[str, bool],
) -> RecoveryRecord:
    if not invariants:
        raise ValueError("at least one recovery invariant is required")
    return RecoveryRecord(
        scenario=scenario,
        initial_state=initial_state,
        injected_failure=injected_failure,
        observed_behavior=observed_behavior,
        retry_action=retry_action,
        final_state=final_state,
        invariants=invariants,
        recovered=all(invariants.values()),
        recorded_at=datetime.now(timezone.utc).isoformat(),
    )


def write_recovery_record(record: RecoveryRecord, root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", record.scenario).strip("-").lower()
    if not safe:
        raise ValueError("scenario must contain a safe filename character")
    target = root / f"{safe}.json"
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(asdict(record), indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)
    return target
