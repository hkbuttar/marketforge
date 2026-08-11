"""Compact one small-file-heavy immutable raw partition."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from ingestion.compaction import compact_partition
from ingestion.contracts import CONTRACTS


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, choices=sorted(CONTRACTS))
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--month", required=True, type=int)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--archive-root", type=Path, default=Path("data/archive/compaction"))
    parser.add_argument("--metadata-root", type=Path, default=Path("warehouse/metadata/compactions"))
    parser.add_argument("--min-files", type=int, default=2)
    parser.add_argument("--max-file-mb", type=float, default=16)
    args = parser.parse_args()
    result = compact_partition(
        args.dataset, args.year, args.month, raw_root=args.raw_root,
        archive_root=args.archive_root, metadata_root=args.metadata_root,
        min_files=args.min_files, max_file_bytes=int(args.max_file_mb * 1_000_000),
    )
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
