#!/usr/bin/env python3
"""Assess the repository and retained price lake against the Step 56 MVP."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


ROOT = Path(__file__).parents[1]
TARGET_SECURITIES = 100


def loaded_price_stats(root: Path) -> tuple[int, int]:
    files = sorted((root / "data/raw/prices").glob("**/*.parquet"))
    if not files:
        return 0, 0
    paths = [str(path) for path in files]
    with duckdb.connect() as connection:
        rows, symbols = connection.execute(
            "SELECT count(*), count(DISTINCT symbol) "
            "FROM read_parquet(?, hive_partitioning=false) WHERE source = 'tiingo'",
            [paths],
        ).fetchone()
    return int(rows), int(symbols)


def assess(root: Path = ROOT, target: int = TARGET_SECURITIES) -> dict[str, object]:
    rows, symbols = loaded_price_stats(root)
    requirements = {
        "100 securities": (symbols >= target, f"{symbols}/{target} Tiingo symbols loaded"),
        "one historical price source": (rows > 0, f"{rows} Tiingo price rows loaded"),
        "Parquet": ((root / "data/raw/prices").exists(), "partitioned raw price lake"),
        "DuckDB": ((root / "warehouse/duckdb/analytics.py").is_file(), "analytical catalog support"),
        "dbt": ((root / "dbt/dbt_project.yml").is_file(), "staging, intermediate, and mart project"),
        "Dagster": ((root / "orchestration/definitions.py").is_file(), "asset definitions"),
        "incremental ingestion": ((root / "ingestion/loaders/incremental.py").is_file(), "checkpointed loader"),
        "idempotency": ((root / "tests/failure/test_idempotency.py").is_file(), "replay and conflict tests"),
        "contracts": ((root / "ingestion/contracts/prices.py").is_file(), "executable price contract"),
        "backfill": ((root / "scripts/backfill.py").is_file(), "range-backfill CLI"),
        "quality checks": ((root / "scripts/check_quality.py").is_file(), "statistical and universe checks"),
        "FastAPI": ((root / "backend/main.py").is_file(), "bounded serving API"),
        "basic observability dashboard": ((root / "frontend/src/main.tsx").is_file(), "React control plane"),
    }
    checks = [
        {"requirement": name, "status": "PASS" if passed else "FAIL", "evidence": evidence}
        for name, (passed, evidence) in requirements.items()
    ]
    passed = sum(check["status"] == "PASS" for check in checks)
    return {
        "status": "READY" if passed == len(checks) else "NOT_READY",
        "passed": passed,
        "required": len(checks),
        "loaded_price_rows": rows,
        "loaded_securities": symbols,
        "target_securities": target,
        "checks": checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--target-securities", type=int, default=TARGET_SECURITIES)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    report = assess(args.root.resolve(), args.target_securities)
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "READY" or args.allow_incomplete else 1


if __name__ == "__main__":
    raise SystemExit(main())
