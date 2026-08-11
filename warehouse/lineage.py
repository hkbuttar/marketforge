"""Asset-level lineage assembled from dbt and MarketForge build metadata."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


def _latest_build(root: Path, dataset: str) -> dict[str, Any] | None:
    candidates = []
    for path in root.glob("*.json"):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if dataset in value.get("datasets", []):
            candidates.append((path.stat().st_mtime_ns, value))
    return max(candidates, default=(0, None), key=lambda item: item[0])[1]


def _providers(raw_root: Path, dataset: str) -> list[str]:
    pattern = raw_root / dataset / "year=*" / "month=*" / "*.parquet"
    if not list((raw_root / dataset).glob("year=*/month=*/*.parquet")):
        return []
    try:
        with duckdb.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT source FROM read_parquet(?) WHERE source IS NOT NULL ORDER BY source",
                [str(pattern)],
            ).fetchall()
        return [str(row[0]) for row in rows]
    except duckdb.Error:
        return []


def build_lineage(
    manifest_path: Path, *, raw_root: Path = Path("data/raw"),
    builds_root: Path = Path("warehouse/metadata/dataset_builds"),
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    resources = {**manifest.get("nodes", {}), **manifest.get("sources", {})}
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, str]] = []
    known = set(resources)
    for unique_id, resource in sorted(resources.items()):
        kind = resource.get("resource_type", "unknown")
        if kind not in {"model", "source", "seed", "snapshot"}:
            continue
        node = {
            "id": unique_id,
            "name": resource.get("name"),
            "type": kind,
            "path": resource.get("original_file_path"),
            "relation": resource.get("relation_name"),
        }
        if kind == "source":
            dataset = str(resource.get("name"))
            build = _latest_build(builds_root, dataset)
            node["raw_pattern"] = str(raw_root / dataset / "year=*" / "month=*" / "*.parquet")
            node["external_sources"] = _providers(raw_root, dataset)
            if build:
                node["dataset_build_id"] = build.get("build_id")
                node["source_partitions"] = len(build.get("source_partitions", []))
                node["pipeline_run_ids"] = sorted({
                    item["pipeline_run_id"] for item in build.get("source_partitions", [])
                    if item.get("pipeline_run_id")
                })
        nodes.append(node)
        for parent in resource.get("depends_on", {}).get("nodes", []):
            if parent in known:
                edges.append({"from": parent, "to": unique_id})
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dbt_invocation_id": manifest.get("metadata", {}).get("invocation_id"),
        "nodes": nodes,
        "edges": sorted(edges, key=lambda edge: (edge["from"], edge["to"])),
    }


def ancestors(graph: dict[str, Any], target: str) -> dict[str, Any]:
    by_id = {node["id"]: node for node in graph["nodes"]}
    matches = [node_id for node_id, node in by_id.items() if node_id == target or node["name"] == target]
    if len(matches) != 1:
        raise ValueError(f"target must match exactly one lineage node; found {len(matches)}")
    selected = {matches[0]}
    changed = True
    while changed:
        before = len(selected)
        selected.update(edge["from"] for edge in graph["edges"] if edge["to"] in selected)
        changed = len(selected) != before
    return {
        **{key: value for key, value in graph.items() if key not in {"nodes", "edges"}},
        "target": matches[0],
        "nodes": [node for node in graph["nodes"] if node["id"] in selected],
        "edges": [edge for edge in graph["edges"] if edge["from"] in selected and edge["to"] in selected],
    }


def descendants(graph: dict[str, Any], source: str) -> dict[str, Any]:
    by_id = {node["id"]: node for node in graph["nodes"]}
    matches = [node_id for node_id, node in by_id.items() if node_id == source or node["name"] == source]
    if len(matches) != 1:
        raise ValueError(f"source must match exactly one lineage node; found {len(matches)}")
    selected = {matches[0]}
    changed = True
    while changed:
        before = len(selected)
        selected.update(edge["to"] for edge in graph["edges"] if edge["from"] in selected)
        changed = len(selected) != before
    return {
        **{key: value for key, value in graph.items() if key not in {"nodes", "edges"}},
        "source": matches[0],
        "nodes": [node for node in graph["nodes"] if node["id"] in selected],
        "edges": [edge for edge in graph["edges"] if edge["from"] in selected and edge["to"] in selected],
    }


def write_lineage(graph: dict[str, Any], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(graph, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)
