"""Evaluate lightweight statistical quality checks against the local price lake."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from quality.anomaly import evaluate_price_quality, write_quality_audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--source", default="tiingo")
    parser.add_argument("--universe", type=Path, default=Path("config/price_universe.txt"))
    parser.add_argument("--thresholds", type=Path, default=Path("config/anomaly_thresholds.json"))
    parser.add_argument("--audit-root", type=Path, default=Path("warehouse/metadata/quality"))
    args = parser.parse_args()
    symbols = [line.strip() for line in args.universe.read_text().splitlines() if line.strip() and not line.startswith("#")]
    thresholds = json.loads(args.thresholds.read_text())
    result = evaluate_price_quality(args.raw_root, source=args.source, expected_symbols=symbols, thresholds=thresholds)
    write_quality_audit(result, args.audit_root)
    print(json.dumps(asdict(result), indent=2))
    return 1 if result.status == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
