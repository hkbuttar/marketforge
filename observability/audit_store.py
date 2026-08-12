"""SQLite operational metadata store built from retained pipeline evidence."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import duckdb


SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY, job_name TEXT NOT NULL, dataset TEXT NOT NULL,
    run_type TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT NOT NULL,
    status TEXT NOT NULL, records_fetched INTEGER NOT NULL,
    records_written INTEGER NOT NULL, records_rejected INTEGER NOT NULL,
    error TEXT, manifest_path TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dataset_versions (
    dataset TEXT NOT NULL, partition TEXT NOT NULL, content_hash TEXT NOT NULL,
    row_count INTEGER NOT NULL, created_at TEXT NOT NULL, run_id TEXT NOT NULL,
    artifact_path TEXT NOT NULL UNIQUE,
    PRIMARY KEY (dataset, partition, content_hash)
);
CREATE TABLE IF NOT EXISTS quality_results (
    result_id TEXT PRIMARY KEY, run_id TEXT, dataset TEXT NOT NULL,
    check_name TEXT NOT NULL, status TEXT NOT NULL, observed_value REAL,
    expected_value TEXT, message TEXT NOT NULL, evaluated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
    dataset TEXT NOT NULL, source TEXT NOT NULL, checkpoint_type TEXT NOT NULL,
    checkpoint_value TEXT NOT NULL, updated_at TEXT NOT NULL, run_id TEXT NOT NULL,
    PRIMARY KEY (dataset, source, checkpoint_type)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dataset_finished
    ON pipeline_runs(dataset, finished_at);
CREATE INDEX IF NOT EXISTS idx_quality_dataset_check
    ON quality_results(dataset, check_name, evaluated_at);
"""


def _json_files(root: Path):
    return sorted(root.glob("*.json")) if root.exists() else []


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class AuditStore:
    def __init__(self, path: Path = Path("warehouse/metadata/operational.sqlite")):
        self.path = path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.executescript(SCHEMA)
        return connection

    def sync_runs(self, root: Path) -> int:
        rows = []
        for path in _json_files(root):
            item = json.loads(path.read_text())
            rows.append((
                item["run_id"], f"ingest_{item['dataset']}", item["dataset"],
                item.get("run_type", "unknown"), item["started_at"], item["completed_at"],
                item["status"], item.get("input_rows", 0), item.get("records_written", item.get("accepted_rows", 0)),
                item.get("quarantined_rows", 0), item.get("error"), str(path),
            ))
        with closing(self.connect()) as connection, connection:
            connection.executemany(
                "INSERT OR REPLACE INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows
            )
        return len(rows)

    def sync_versions(self, raw_root: Path) -> int:
        files = sorted(raw_root.glob("*/year=*/month=*/*.parquet"))
        rows = []
        for path in files:
            relative = path.relative_to(raw_root)
            dataset = relative.parts[0]
            partition = "/".join(relative.parts[1:3])
            run_id = path.stem.removeprefix("part-")
            with duckdb.connect() as connection:
                row_count = connection.execute(
                    "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(path)]
                ).fetchone()[0]
            created_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
            rows.append((dataset, partition, _sha256(path), row_count, created_at, run_id, str(path)))
        with closing(self.connect()) as connection, connection:
            connection.executemany(
                "INSERT OR REPLACE INTO dataset_versions VALUES (?,?,?,?,?,?,?)", rows
            )
        return len(rows)

    def sync_quality(self, quality_root: Path, freshness_root: Path) -> int:
        rows = []
        now = datetime.now(timezone.utc).isoformat()
        for path in _json_files(quality_root):
            item = json.loads(path.read_text())
            evaluated = item.get("event_date") or now
            findings = item.get("findings", [])
            rows.append((
                f"quality:{item['dataset']}:{item.get('source', '')}:overall:{evaluated}", None, item["dataset"],
                "statistical_quality", item["status"], None, None, item["reason"], evaluated,
            ))
            for finding in findings:
                name = finding["metric"]
                rows.append((
                    f"quality:{item['dataset']}:{item.get('source', '')}:{name}:{evaluated}", None, item["dataset"],
                    name, finding["severity"], finding.get("value"),
                    json.dumps({"median": finding.get("baseline_median"), "mad": finding.get("baseline_mad")}),
                    finding["reason"], evaluated,
                ))
        for path in _json_files(freshness_root):
            item = json.loads(path.read_text())
            rows.append((
                f"freshness:{item['dataset']}:{item['evaluated_at']}", None, item["dataset"], "freshness",
                item["status"], item.get("age_hours"), item.get("expected_event_time"),
                item["reason"], item["evaluated_at"],
            ))
        with closing(self.connect()) as connection, connection:
            connection.executemany(
                "INSERT OR REPLACE INTO quality_results VALUES (?,?,?,?,?,?,?,?,?)", rows
            )
        return len(rows)

    def sync_checkpoints(self, checkpoint_db: Path) -> int:
        if not checkpoint_db.exists():
            return 0
        source = sqlite3.connect(checkpoint_db)
        try:
            rows = source.execute(
                "SELECT dataset, source, last_successful_event_date, updated_at, last_successful_run_id "
                "FROM ingestion_checkpoint"
            ).fetchall()
        finally:
            source.close()
        values = [(dataset, provider, "event_date", value, updated, run_id) for dataset, provider, value, updated, run_id in rows]
        with closing(self.connect()) as connection, connection:
            connection.executemany("INSERT OR REPLACE INTO checkpoints VALUES (?,?,?,?,?,?)", values)
        return len(values)

    def sync_all(self, metadata_root: Path = Path("warehouse/metadata"), raw_root: Path = Path("data/raw")) -> dict[str, int]:
        return {
            "pipeline_runs": self.sync_runs(metadata_root / "ingestion_runs"),
            "dataset_versions": self.sync_versions(raw_root),
            "quality_results": self.sync_quality(metadata_root / "quality", metadata_root / "freshness"),
            "checkpoints": self.sync_checkpoints(metadata_root / "checkpoints.sqlite"),
        }
