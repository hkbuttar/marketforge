#!/usr/bin/env python3
"""Verify evidence coverage for the final 18-point Definition of Done."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).parents[1]


CONDITIONS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("Bootstrap the environment", "python3 -m venv venv && pip install -r requirements.txt", ("requirements.txt", "README.md")),
    ("Load a historical dataset", "python -m scripts.backfill --help", ("scripts/backfill.py", "tests/integration/test_range_backfill.py")),
    ("Query raw Parquet through DuckDB", "python -m unittest tests.integration.test_duckdb_analytics", ("warehouse/duckdb/analytics.py", "tests/integration/test_duckdb_analytics.py")),
    ("Produce tested dbt marts", "dbt build --project-dir dbt --profiles-dir dbt", ("dbt/dbt_project.yml", "tests/integration/test_dbt_staging.py")),
    ("Run a daily incremental update", "python -m unittest tests.integration.test_incremental", ("ingestion/loaders/incremental.py", "tests/integration/test_incremental.py")),
    ("Rerun without duplicates", "python -m scripts.prove_idempotency", ("scripts/prove_idempotency.py", "tests/failure/test_idempotency.py")),
    ("Backfill an arbitrary range", "python -m scripts.backfill --help", ("scripts/backfill.py", "tests/integration/test_range_backfill.py")),
    ("Handle a late-arriving correction", "python -m scripts.demo", ("docs/late_arriving_data.md", "tests/integration/test_incremental.py")),
    ("Reject a breaking schema change", "python -m unittest tests.unit.test_source_contracts", ("ingestion/contracts/base.py", "tests/unit/test_source_contracts.py")),
    ("Quarantine malformed records", "python -m scripts.demo", ("tests/failure/test_synthetic_failure_dataset.py", "docs/source_contracts.md")),
    ("Recover from a killed write", "python -m scripts.demo", ("tests/failure/test_recovery_drills.py", "docs/recovery_tests.md")),
    ("Trace a mart through dependencies", "python -m scripts.build_lineage --target mart_security_daily", ("scripts/build_lineage.py", "docs/data_lineage.md")),
    ("View freshness and quality", "npm run dev --prefix frontend", ("frontend/src/main.tsx", "scripts/check_freshness.py", "scripts/check_quality.py")),
    ("Query analytics through FastAPI", "python -m unittest tests.integration.test_query_api", ("backend/main.py", "tests/integration/test_query_api.py")),
    ("Compare incremental and full refresh", "python -m benchmarks.incremental_vs_full", ("benchmarks/incremental_vs_full.py", "docs/incremental_vs_full_refresh.md")),
    ("Show disk and compression measurements", "python -m benchmarks.storage_efficiency", ("benchmarks/storage_efficiency.py", "docs/results.md")),
    ("Run the automated test suite", "python -m scripts.test_summary", ("scripts/test_summary.py", ".github/workflows/ci.yml")),
    ("Reproduce documented experiments", "python -m benchmarks.run", ("benchmarks/run.py", "README.md", "docs/end_to_end_demo.md")),
)


def assess(root: Path = ROOT) -> dict[str, object]:
    checks = []
    for number, (condition, command, evidence) in enumerate(CONDITIONS, start=1):
        missing = [path for path in evidence if not (root / path).is_file()]
        checks.append(
            {
                "number": number,
                "condition": condition,
                "status": "PASS" if not missing else "FAIL",
                "command": command,
                "evidence": list(evidence),
                "missing": missing,
            }
        )
    passed = sum(check["status"] == "PASS" for check in checks)
    return {
        "status": "COMPLETE" if passed == len(checks) else "INCOMPLETE",
        "passed": passed,
        "required": len(checks),
        "checks": checks,
        "scope_note": "This gate verifies reproducible evidence coverage; CI and the demo execute behavior.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    report = assess(args.root.resolve())
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "COMPLETE" or args.allow_incomplete else 1


if __name__ == "__main__":
    raise SystemExit(main())
