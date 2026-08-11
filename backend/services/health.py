"""Read-only liveness/readiness checks for serving dependencies."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb


REQUIRED_MARTS = frozenset({
    "mart_company_snapshot", "mart_security_daily", "mart_pipeline_dataset_health",
})


@dataclass(frozen=True)
class ComponentCheck:
    component: str
    status: str
    detail: str


def readiness(database: Path, metadata_store: Path) -> list[dict[str, str]]:
    checks: list[ComponentCheck] = []
    if not database.is_file():
        checks.append(ComponentCheck("duckdb", "failed", f"database not found: {database}"))
        checks.append(ComponentCheck("required_marts", "failed", "DuckDB is unavailable"))
    else:
        try:
            with duckdb.connect(str(database), read_only=True) as connection:
                connection.execute("SELECT 1").fetchone()
                rows = connection.execute("""SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'main_marts'""").fetchall()
            checks.append(ComponentCheck("duckdb", "ready", "read-only query succeeded"))
            available = {row[0] for row in rows}
            missing = sorted(REQUIRED_MARTS - available)
            checks.append(ComponentCheck(
                "required_marts", "failed" if missing else "ready",
                f"missing: {', '.join(missing)}" if missing else f"available: {len(REQUIRED_MARTS)}",
            ))
        except duckdb.Error as exc:
            checks.append(ComponentCheck("duckdb", "failed", str(exc)))
            checks.append(ComponentCheck("required_marts", "failed", "DuckDB query failed"))
    if not metadata_store.is_file():
        checks.append(ComponentCheck("metadata_store", "failed", f"store not found: {metadata_store}"))
    else:
        try:
            uri = metadata_store.resolve().as_uri() + "?mode=ro"
            with sqlite3.connect(uri, uri=True) as connection:
                connection.execute("SELECT 1").fetchone()
            checks.append(ComponentCheck("metadata_store", "ready", "read-only query succeeded"))
        except sqlite3.Error as exc:
            checks.append(ComponentCheck("metadata_store", "failed", str(exc)))
    return [asdict(check) for check in checks]
