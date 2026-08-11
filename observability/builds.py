"""Deterministic logical build manifests for reproducible analytical datasets."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb


def _hash_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_state(repo_root: Path) -> tuple[str, str]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True,
            capture_output=True, check=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=repo_root, text=True,
            capture_output=True, check=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("dataset builds require a Git worktree") from exc
    return commit, "clean" if not status else f"dirty:{_hash_bytes(status.encode())}"


def _code_hash(repo_root: Path) -> str:
    roots = [repo_root / "dbt/dbt_project.yml", repo_root / "dbt/models", repo_root / "dbt/macros"]
    files = []
    for root in roots:
        files.extend([root] if root.is_file() else sorted(path for path in root.rglob("*") if path.is_file()))
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(str(path.relative_to(repo_root)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _dbt_run_id(repo_root: Path) -> str | None:
    target = repo_root / "dbt/target/run_results.json"
    if not target.exists():
        return None
    payload = json.loads(target.read_text())
    return payload.get("metadata", {}).get("invocation_id")


def _source_partitions(raw_root: Path, datasets: set[str], repo_root: Path) -> list[dict[str, Any]]:
    partitions = []
    for path in sorted(raw_root.glob("*/year=*/month=*/*.parquet")):
        relative_raw = path.relative_to(raw_root)
        dataset = relative_raw.parts[0]
        if datasets and dataset not in datasets:
            continue
        with duckdb.connect() as connection:
            rows = connection.execute(
                "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(path)]
            ).fetchone()[0]
        try:
            artifact = str(path.relative_to(repo_root))
        except ValueError:
            artifact = str(path)
        partitions.append({
            "dataset": dataset,
            "partition": "/".join(relative_raw.parts[1:3]),
            "artifact": artifact,
            "content_hash": _file_hash(path),
            "row_count": rows,
            "pipeline_run_id": path.stem.removeprefix("part-"),
        })
    return partitions


def create_build_manifest(
    *, repo_root: Path, raw_root: Path, output_root: Path,
    datasets: Iterable[str] = (), parameters: dict[str, str] | None = None,
) -> tuple[dict[str, Any], Path]:
    repo_root = repo_root.resolve()
    raw_root = raw_root.resolve()
    selected = set(datasets)
    partitions = _source_partitions(raw_root, selected, repo_root)
    if not partitions:
        raise ValueError("no source partitions matched the requested datasets")
    commit, worktree = _git_state(repo_root)
    identity = {
        "git_commit": commit,
        "git_worktree": worktree,
        "dbt_code_hash": _code_hash(repo_root),
        "dbt_run_id": _dbt_run_id(repo_root),
        "datasets": sorted(selected or {item["dataset"] for item in partitions}),
        "parameters": dict(sorted((parameters or {}).items())),
        "source_partitions": partitions,
    }
    build_id = _hash_bytes(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())
    manifest = {"build_id": build_id, **identity, "created_at": datetime.now(timezone.utc).isoformat()}
    output_root.mkdir(parents=True, exist_ok=True)
    target = output_root / f"{build_id}.json"
    if target.exists():
        existing = json.loads(target.read_text())
        if {key: existing[key] for key in identity} != identity:
            raise ValueError(f"existing build manifest does not match build identity: {target}")
        return existing, target
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, target)
    return manifest, target


def verify_build_manifest(manifest_path: Path, repo_root: Path) -> list[str]:
    manifest = json.loads(manifest_path.read_text())
    errors = []
    commit, worktree = _git_state(repo_root)
    if manifest["git_commit"] != commit:
        errors.append(f"git commit changed: {manifest['git_commit']} != {commit}")
    if manifest["git_worktree"] != worktree:
        errors.append(f"Git worktree state changed: {manifest['git_worktree']} != {worktree}")
    if manifest["dbt_code_hash"] != _code_hash(repo_root):
        errors.append("dbt transformation code hash changed")
    for item in manifest["source_partitions"]:
        path = Path(item["artifact"])
        path = path if path.is_absolute() else repo_root / path
        if not path.exists():
            errors.append(f"source artifact missing: {item['artifact']}")
        elif _file_hash(path) != item["content_hash"]:
            errors.append(f"source artifact hash changed: {item['artifact']}")
    identity = {key: manifest[key] for key in (
        "git_commit", "git_worktree", "dbt_code_hash", "dbt_run_id", "datasets", "parameters", "source_partitions"
    )}
    expected_id = _hash_bytes(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())
    if manifest["build_id"] != expected_id:
        errors.append("build identifier does not match manifest identity")
    return errors
