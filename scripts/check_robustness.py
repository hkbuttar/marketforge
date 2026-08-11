#!/usr/bin/env python3
"""Assess the repository against the Step 57 maximum-robustness checklist."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).parents[1]


FEATURES: dict[str, tuple[str, ...]] = {
    "multiple sources": (
        "ingestion/sources/files.py",
        "ingestion/sources/tiingo.py",
        "ingestion/sources/streamalpha.py",
    ),
    "late-arriving data": ("tests/integration/test_incremental.py", "docs/late_arriving_data.md"),
    "schema evolution": ("ingestion/contracts/base.py", "tests/unit/test_source_contracts.py"),
    "quarantine": ("ingestion/loaders/backfill.py", "tests/failure/test_synthetic_failure_dataset.py"),
    "reconciliation": ("quality/reconciliation.py", "tests/unit/test_reconciliation.py"),
    "storage benchmarks": ("benchmarks/storage_efficiency.py", "docs/results.md"),
    "partition benchmarks": ("benchmarks/partition_layout.py", "docs/historical_backfill.md"),
    "compaction": ("ingestion/compaction.py", "tests/integration/test_compaction.py"),
    "resource guardrails": ("observability/resource_guardrails.py", "config/resource_budget.yaml"),
    "failure injection": ("tests/failure/test_external_failures.py", "docs/failure_injection.md"),
    "dataset manifests": ("observability/builds.py", "scripts/build_dataset.py"),
    "full lineage": ("warehouse/lineage.py", "scripts/build_lineage.py", "docs/data_lineage.md"),
    "CI": (".github/workflows/ci.yml", ".github/workflows/live-source-smoke.yml"),
    "public demo": ("render.yaml", "frontend/vercel.json", "scripts/build_demo_snapshot.py"),
    "StreamAlpha adapter": ("ingestion/sources/streamalpha.py", "ingestion/streaming/kafka.py"),
}


def assess(root: Path = ROOT) -> dict[str, object]:
    checks = []
    for feature, paths in FEATURES.items():
        missing = [path for path in paths if not (root / path).is_file()]
        checks.append(
            {
                "feature": feature,
                "status": "PASS" if not missing else "FAIL",
                "evidence": list(paths),
                "missing": missing,
            }
        )
    passed = sum(check["status"] == "PASS" for check in checks)
    return {
        "status": "READY" if passed == len(checks) else "NOT_READY",
        "passed": passed,
        "required": len(checks),
        "checks": checks,
        "scope_note": (
            "The public-demo gate verifies reproducible deployment artifacts, not live endpoint uptime."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    report = assess(args.root.resolve())
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "READY" or args.allow_incomplete else 1


if __name__ == "__main__":
    raise SystemExit(main())
