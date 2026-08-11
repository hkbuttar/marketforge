"""Resolve and catalog an immutable dataset build without copying its data."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from observability.builds import _resolve_artifact


BUILD_ID = re.compile(r"^[0-9a-f]{64}$")


class ReproductionError(RuntimeError):
    pass


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_build(build: str, builds_root: Path) -> tuple[dict[str, Any], Path]:
    if not BUILD_ID.fullmatch(build):
        raise ValueError("build_id must be a 64-character lowercase SHA-256 value")
    path = builds_root / f"{build}.json"
    if not path.is_file():
        raise FileNotFoundError(f"dataset build manifest not found: {path}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("build_id") != build:
        raise ReproductionError("manifest build_id does not match its requested identity")
    return manifest, path


def reproduction_plan(manifest: dict[str, Any], *, repo_root: Path) -> dict[str, Any]:
    artifacts = []
    for item in manifest["source_partitions"]:
        declared = Path(item["artifact"])
        declared = declared if declared.is_absolute() else repo_root / declared
        resolved = _resolve_artifact(declared, item, repo_root)
        available = resolved.is_file()
        actual_hash = _hash(resolved) if available else None
        artifacts.append({
            **item, "resolved_artifact": str(resolved), "available": available,
            "hash_valid": actual_hash == item["content_hash"] if available else False,
        })
    return {
        "build_id": manifest["build_id"], "datasets": manifest["datasets"],
        "parameters": manifest.get("parameters", {}),
        "required_code": {
            "git_commit": manifest["git_commit"], "git_worktree": manifest["git_worktree"],
            "dbt_code_hash": manifest["dbt_code_hash"], "dbt_run_id": manifest.get("dbt_run_id"),
        },
        "artifacts": artifacts,
        "inputs_ready": all(item["available"] and item["hash_valid"] for item in artifacts),
        "expected_rows": sum(int(item["row_count"]) for item in artifacts),
    }


def create_catalog(plan: dict[str, Any], target: Path) -> dict[str, Any]:
    if not plan["inputs_ready"]:
        failures = [item["artifact"] for item in plan["artifacts"]
                    if not item["available"] or not item["hash_valid"]]
        raise ReproductionError(f"historical inputs are unavailable or changed: {failures}")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in plan["artifacts"]:
        grouped[item["dataset"]].append(item)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"historical catalog already exists: {target}")
    temporary = target.with_suffix(".tmp.duckdb")
    counts = {}
    try:
        with duckdb.connect(str(temporary)) as connection:
            connection.execute("CREATE SCHEMA raw")
            for dataset, items in sorted(grouped.items()):
                if not re.fullmatch(r"[a-z][a-z0-9_]*", dataset):
                    raise ReproductionError(f"unsafe dataset identifier in manifest: {dataset!r}")
                files = ",".join(
                    "'" + item["resolved_artifact"].replace("'", "''") + "'" for item in items
                )
                connection.execute(f"""CREATE VIEW raw."{dataset}" AS
                    SELECT * FROM read_parquet([{files}], union_by_name=true, hive_partitioning=false)""")
                actual = connection.execute(f'SELECT count(*) FROM raw."{dataset}"').fetchone()[0]
                expected = sum(int(item["row_count"]) for item in items)
                if actual != expected:
                    raise ReproductionError(
                        f"{dataset} historical row count {actual} does not match manifest {expected}"
                    )
                counts[dataset] = actual
            connection.execute("CREATE TABLE reproduction_metadata (payload JSON)")
            connection.execute("INSERT INTO reproduction_metadata VALUES (?)", [json.dumps({
                "build_id": plan["build_id"], "required_code": plan["required_code"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            })])
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        temporary.with_suffix(temporary.suffix + ".wal").unlink(missing_ok=True)
        raise
    return {"catalog": str(target), "dataset_rows": counts, "bytes": target.stat().st_size}
