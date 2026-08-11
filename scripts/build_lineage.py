"""Export dbt asset lineage enriched with raw dataset metadata."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from warehouse.lineage import ancestors, build_lineage, write_lineage


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("dbt/target/manifest.json"))
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument("--builds-root", type=Path, default=Path("warehouse/metadata/dataset_builds"))
    parser.add_argument("--output", type=Path, default=Path("warehouse/metadata/lineage.json"))
    parser.add_argument("--target", help="Print only this model/source and its ancestors")
    args = parser.parse_args()
    graph = build_lineage(args.manifest, raw_root=args.raw_root, builds_root=args.builds_root)
    write_lineage(graph, args.output)
    view = ancestors(graph, args.target) if args.target else graph
    print(json.dumps({
        "output": str(args.output), "target": view.get("target"),
        "nodes": len(view["nodes"]), "edges": len(view["edges"]),
        "lineage": view if args.target else None,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
