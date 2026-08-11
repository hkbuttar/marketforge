"""Pre-write storage projections and safe cleanup candidate discovery."""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml


GB = 1_000_000_000


class ResourceLimitExceeded(RuntimeError):
    """A projected job would violate a configured hard resource limit."""


@dataclass(frozen=True)
class StorageAssessment:
    status: str
    current_project_bytes: int
    current_raw_bytes: int
    projected_write_bytes: int
    projected_project_bytes: int
    projected_raw_bytes: int
    free_disk_bytes: int
    warning_bytes: int
    hard_limit_bytes: int
    raw_limit_bytes: int
    minimum_free_bytes: int
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_budget(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("storage"), dict):
        raise ValueError("resource budget must define a storage mapping")
    return value


def tree_size(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file() and not path.is_symlink())


def estimate_records(records: Iterable[Mapping[str, Any]]) -> int:
    # JSON size plus 25% staging/schema overhead is a deliberately conservative
    # pre-write estimate; compressed Parquet is normally smaller.
    return int(sum(len(json.dumps(dict(row), default=str).encode()) for row in records) * 1.25)


def assess_storage(*, project_root: Path, raw_root: Path, projected_write_bytes: int,
                   budget_path: Path) -> StorageAssessment:
    if projected_write_bytes < 0:
        raise ValueError("projected write size cannot be negative")
    budget = load_budget(budget_path)
    storage = budget["storage"]
    warning = int(float(storage["warning_gb"]) * GB)
    hard = int(float(storage["hard_limit_gb"]) * GB)
    minimum_free = int(float(storage["minimum_free_gb"]) * GB)
    raw_limit = int(float(budget["project_limits"]["raw_data_gb"]) * GB)
    if not 0 < warning <= hard or minimum_free < 0 or raw_limit <= 0:
        raise ValueError("invalid storage resource limits")
    current_project = tree_size(project_root)
    current_raw = tree_size(raw_root)
    free = shutil.disk_usage(project_root).free
    projected_project = current_project + projected_write_bytes
    projected_raw = current_raw + projected_write_bytes
    hard_reasons = []
    if projected_project > hard:
        hard_reasons.append("project hard limit exceeded")
    if projected_raw > raw_limit:
        hard_reasons.append("raw-data limit exceeded")
    if free - projected_write_bytes < minimum_free:
        hard_reasons.append("minimum free-disk reserve would be breached")
    warning_reasons = []
    if projected_project > warning:
        warning_reasons.append("project warning threshold exceeded")
    status = "blocked" if hard_reasons else "warning" if warning_reasons else "safe"
    return StorageAssessment(
        status, current_project, current_raw, projected_write_bytes, projected_project,
        projected_raw, free, warning, hard, raw_limit, minimum_free,
        tuple(hard_reasons or warning_reasons),
    )


def enforce_storage(**kwargs: Any) -> StorageAssessment:
    result = assess_storage(**kwargs)
    if result.status == "blocked":
        raise ResourceLimitExceeded("; ".join(result.reasons))
    return result
