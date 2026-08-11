"""Allowlisted, parameterized queries over published marts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from warehouse.lineage import ancestors


class MartUnavailable(RuntimeError):
    pass


class QueryService:
    def __init__(self, database: Path, lineage_path: Path):
        self.database = database
        self.lineage_path = lineage_path

    def _query(self, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
        if not self.database.is_file():
            raise MartUnavailable(f"published mart database not found: {self.database}")
        try:
            with duckdb.connect(str(self.database), read_only=True) as connection:
                result = connection.execute(sql, parameters or [])
                columns = [item[0] for item in result.description]
                return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
        except duckdb.Error as exc:
            raise MartUnavailable(f"published mart query failed: {exc}") from exc

    def securities(self, limit: int) -> list[dict[str, Any]]:
        return self._query("""SELECT symbol, "source" AS source, latest_price_date, latest_price
            FROM main_marts.mart_company_snapshot ORDER BY symbol, "source" LIMIT ?""", [limit])

    def security(self, symbol: str) -> dict[str, Any] | None:
        rows = self._query("""SELECT symbol, "source" AS source, latest_price_date, latest_price,
            latest_fundamental_period_end, latest_fundamental_filed_at,
            available_fundamental_metrics, latest_earnings_timestamp,
            latest_eps_actual, latest_eps_surprise
            FROM main_marts.mart_company_snapshot WHERE symbol = ?
            ORDER BY "source" LIMIT 1""", [symbol.upper()])
        return rows[0] if rows else None

    def history(self, symbol: str, source: str | None, limit: int) -> list[dict[str, Any]]:
        clause = "symbol = ?"
        parameters: list[Any] = [symbol.upper()]
        if source:
            clause += " AND \"source\" = ?"
            parameters.append(source.lower())
        parameters.append(limit)
        return self._query(f"""SELECT trade_date, close, daily_return,
            rolling_20d_return, rolling_20d_volatility, volume
            FROM main_marts.mart_security_daily WHERE {clause}
            ORDER BY trade_date DESC LIMIT ?""", parameters)

    def health(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._query("""SELECT dataset, status, row_count, null_rate,
            duplicate_count, quarantine_count, latest_event_time,
            last_successful_run, last_successful_run_at
            FROM main_marts.mart_pipeline_dataset_health ORDER BY dataset LIMIT ?""", [limit])

    def lineage(self, dataset: str) -> dict[str, Any]:
        if not self.lineage_path.is_file():
            raise MartUnavailable(f"lineage artifact not found: {self.lineage_path}")
        graph = json.loads(self.lineage_path.read_text(encoding="utf-8"))
        try:
            return ancestors(graph, f"source.marketforge.raw.{dataset}")
        except ValueError as exc:
            raise KeyError(dataset) from exc
