"""Synchronize retained pipeline evidence into the operational SQLite store."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from observability.audit_store import AuditStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("warehouse/metadata/operational.sqlite"))
    parser.add_argument("--metadata-root", type=Path, default=Path("warehouse/metadata"))
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    counts = AuditStore(args.database).sync_all(args.metadata_root, args.raw_root)
    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
