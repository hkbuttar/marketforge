"""Create or verify a reproducible MarketForge dataset-build manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from observability.builds import create_build_manifest, verify_build_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-root", type=Path, default=Path("warehouse/metadata/dataset_builds"))
    parser.add_argument("--dataset", action="append", dest="datasets")
    parser.add_argument("--parameter", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    if args.verify:
        errors = verify_build_manifest(args.verify, args.repo_root.resolve())
        print(json.dumps({"valid": not errors, "errors": errors}, indent=2))
        return 1 if errors else 0
    parameters = {}
    for value in args.parameter:
        if "=" not in value:
            parser.error("--parameter must use KEY=VALUE")
        key, item = value.split("=", 1)
        parameters[key] = item
    manifest, target = create_build_manifest(
        repo_root=args.repo_root, raw_root=args.raw_root, output_root=args.output_root,
        datasets=args.datasets or (), parameters=parameters,
    )
    print(json.dumps({"build_id": manifest["build_id"], "manifest": str(target), "source_partitions": len(manifest["source_partitions"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
