"""Evaluate dataset SLAs from the latest ingestion manifests."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from observability.freshness import POLICIES, evaluate_freshness, write_freshness_audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-root", type=Path, default=Path("warehouse/metadata/ingestion_runs"))
    parser.add_argument("--audit-root", type=Path, default=Path("warehouse/metadata/freshness"))
    args = parser.parse_args()
    manifests = [json.loads(path.read_text()) for path in args.metadata_root.glob("*.json")]
    results = []
    for dataset in POLICIES:
        candidates = [item for item in manifests if item.get("dataset") == dataset]
        latest = max(candidates, key=lambda item: item.get("completed_at", "")) if candidates else {}
        result = evaluate_freshness(
            dataset, latest_event_time=latest.get("max_event_date"),
            last_source_check_at=latest.get("completed_at"),
        )
        write_freshness_audit(result, args.audit_root)
        results.append(asdict(result))
    print(json.dumps(results, indent=2))
    return 1 if any(item["status"] == "FAILED" for item in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
