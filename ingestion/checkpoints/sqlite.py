"""Transactional local ingestion checkpoints."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class Checkpoint:
    dataset: str
    source: str
    last_successful_event_date: date
    last_successful_run_id: str
    updated_at: datetime


class CheckpointStore:
    def __init__(self, path: Path = Path("warehouse/metadata/checkpoints.sqlite")):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
                dataset TEXT NOT NULL,
                source TEXT NOT NULL,
                last_successful_event_date TEXT NOT NULL,
                last_successful_run_id TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (dataset, source)
            )
            """
        )
        return connection

    def get(self, dataset: str, source: str) -> Checkpoint | None:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                """SELECT dataset, source, last_successful_event_date,
                          last_successful_run_id, updated_at
                   FROM ingestion_checkpoint WHERE dataset = ? AND source = ?""",
                (dataset, source),
            ).fetchone()
        if row is None:
            return None
        return Checkpoint(row[0], row[1], date.fromisoformat(row[2]), row[3], datetime.fromisoformat(row[4]))

    def advance(self, dataset: str, source: str, event_date: date, run_id: str) -> Checkpoint:
        """Advance monotonically; an overlap run can never move state backward."""
        updated_at = datetime.now(timezone.utc)
        with closing(self._connect()) as connection, connection:
            current = connection.execute(
                "SELECT last_successful_event_date FROM ingestion_checkpoint WHERE dataset=? AND source=?",
                (dataset, source),
            ).fetchone()
            if current and date.fromisoformat(current[0]) > event_date:
                event_date = date.fromisoformat(current[0])
            connection.execute(
                """
                INSERT INTO ingestion_checkpoint VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(dataset, source) DO UPDATE SET
                    last_successful_event_date=excluded.last_successful_event_date,
                    last_successful_run_id=excluded.last_successful_run_id,
                    updated_at=excluded.updated_at
                """,
                (dataset, source, event_date.isoformat(), run_id, updated_at.isoformat()),
            )
        return Checkpoint(dataset, source, event_date, run_id, updated_at)
