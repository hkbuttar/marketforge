"""Plan or create a lightweight catalog for a historical dataset build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from warehouse.time_travel import create_catalog, load_build, reproduction_plan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--builds-root", type=Path, default=Path("warehouse/metadata/dataset_builds"))
    parser.add_argument("--catalog", action="store_true", help="Create a read-only historical DuckDB catalog")
    parser.add_argument("--output-root", type=Path, default=Path("warehouse/duckdb/history"))
    args = parser.parse_args()
    repo_root = args.repo_root.resolve()
    builds_root = args.builds_root
    if not builds_root.is_absolute():
        builds_root = repo_root / builds_root
    manifest, manifest_path = load_build(args.build_id, builds_root)
    plan = reproduction_plan(manifest, repo_root=repo_root)
    output: dict = {"manifest": str(manifest_path), "plan": plan}
    if args.catalog:
        output_root = args.output_root
        if not output_root.is_absolute():
            output_root = repo_root / output_root
        output["materialization"] = create_catalog(plan, output_root / f"{args.build_id}.duckdb")
    print(json.dumps(output, indent=2))
    return 0 if plan["inputs_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
