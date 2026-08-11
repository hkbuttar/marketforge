"""Allowlisted, parameterized queries over published marts."""

from __future__ import annotations

import json
import hashlib
import sqlite3
from pathlib import Path
from typing import Any

import duckdb

from warehouse.lineage import descendants
from backend.services.cache import QueryCache
from ingestion.contracts import CONTRACTS
from observability.resource_guardrails import GB, load_budget, tree_size


class MartUnavailable(RuntimeError):
    pass


class QueryService:
    def __init__(self, database: Path, lineage_path: Path, *,
                 metadata_store: Path = Path("warehouse/metadata/operational.sqlite"),
                 quarantine_root: Path = Path("data/quarantine"),
                 project_root: Path = Path("."), raw_root: Path = Path("data/raw"),
                 budget_path: Path = Path("config/resource_budget.yaml"),
                 benchmarks_path: Path = Path("benchmarks/results/latest.json"),
                 builds_root: Path = Path("warehouse/metadata/dataset_builds"),
                 cache: QueryCache | None = None):
        self.database = database
        self.lineage_path = lineage_path
        self.builds_root = builds_root
        self.metadata_store = metadata_store
        self.quarantine_root = quarantine_root
        self.project_root = project_root
        self.raw_root = raw_root
        self.budget_path = budget_path
        self.benchmarks_path = benchmarks_path
        self.cache = cache or QueryCache()

    def _version(self, *, include_lineage: bool = False) -> str:
        paths = [self.database]
        paths.extend(self.builds_root.glob("*.json"))
        if include_lineage:
            paths.append(self.lineage_path)
        state = []
        for path in sorted(paths, key=str):
            try:
                stat = path.stat()
                state.append((str(path), stat.st_mtime_ns, stat.st_size))
            except FileNotFoundError:
                state.append((str(path), None, None))
        return hashlib.sha256(repr(state).encode()).hexdigest()

    def _cached(self, endpoint: str, parameters: tuple[Any, ...], compute, *, include_lineage=False):
        key = (endpoint, parameters, self._version(include_lineage=include_lineage))
        return self.cache.get_or_compute(key, compute)

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
        return self._cached("securities", (limit,), lambda: self._query(
            """SELECT symbol, "source" AS source, latest_price_date, latest_price
            FROM main_marts.mart_company_snapshot ORDER BY symbol, "source" LIMIT ?""", [limit]))

    def security(self, symbol: str) -> dict[str, Any] | None:
        rows = self._cached("security", (symbol.upper(),), lambda: self._query(
            """SELECT symbol, "source" AS source, latest_price_date, latest_price,
            latest_fundamental_period_end, latest_fundamental_filed_at,
            available_fundamental_metrics, latest_earnings_timestamp,
            latest_eps_actual, latest_eps_surprise
            FROM main_marts.mart_company_snapshot WHERE symbol = ?
            ORDER BY "source" LIMIT 1""", [symbol.upper()]))
        return rows[0] if rows else None

    def history(self, symbol: str, source: str | None, limit: int) -> list[dict[str, Any]]:
        clause = "symbol = ?"
        parameters: list[Any] = [symbol.upper()]
        if source:
            clause += " AND \"source\" = ?"
            parameters.append(source.lower())
        parameters.append(limit)
        return self._cached("history", (symbol.upper(), source.lower() if source else None, limit),
            lambda: self._query(f"""SELECT trade_date, close, daily_return,
            rolling_20d_return, rolling_20d_volatility, volume
            FROM main_marts.mart_security_daily WHERE {clause}
            ORDER BY trade_date DESC LIMIT ?""", parameters))

    def health(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._cached("health", (limit,), lambda: self._query(
            """SELECT dataset, status, row_count, null_rate,
            duplicate_count, quarantine_count, latest_event_time,
            last_successful_run, last_successful_run_at
            FROM main_marts.mart_pipeline_dataset_health ORDER BY dataset LIMIT ?""", [limit]))

    def dataset(self, name: str) -> dict[str, Any] | None:
        rows = self.health()
        return next((row for row in rows if row["dataset"] == name.lower()), None)

    def dataset_schema(self, name: str) -> dict[str, Any]:
        contract = CONTRACTS.get(name.lower())
        if contract is None:
            raise KeyError(name)
        return {
            "dataset": contract.name,
            "contract_version": contract.version,
            "fields": [
                {"name": field, "type": spec.normalizer.__name__, "nullable": spec.nullable,
                 "description": spec.description or None}
                for field, spec in contract.fields.items()
            ],
            "unique_by": list(contract.unique_by),
            "idempotency_by": list(contract.idempotency_by),
        }

    def _metadata_query(self, sql: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
        if not self.metadata_store.is_file():
            raise MartUnavailable(f"operational metadata store not found: {self.metadata_store}")
        try:
            uri = self.metadata_store.resolve().as_uri() + "?mode=ro"
            with sqlite3.connect(uri, uri=True) as connection:
                connection.row_factory = sqlite3.Row
                return [dict(row) for row in connection.execute(sql, parameters).fetchall()]
        except sqlite3.Error as exc:
            raise MartUnavailable(f"operational metadata query failed: {exc}") from exc

    def runs(self, limit: int) -> list[dict[str, Any]]:
        return self._cached("runs", (limit,), lambda: self._metadata_query(
            """SELECT run_id, job_name, dataset, run_type, started_at, finished_at, status,
                      records_fetched, records_written, records_rejected, error
               FROM pipeline_runs ORDER BY finished_at DESC LIMIT ?""", (limit,)))

    def quality(self, limit: int) -> list[dict[str, Any]]:
        return self._cached("quality", (limit,), lambda: self._metadata_query(
            """SELECT result_id, run_id, dataset, check_name, status, observed_value,
                      expected_value, message, evaluated_at
               FROM quality_results ORDER BY evaluated_at DESC LIMIT ?""", (limit,)))

    def quarantine_summary(self) -> dict[str, Any]:
        def load():
            groups: dict[tuple[str, str], int] = {}
            total = 0
            files = 0
            if self.quarantine_root.exists():
                for path in self.quarantine_root.glob("**/*.jsonl"):
                    files += 1
                    source_hint = next(
                        (part.split("=", 1)[1] for part in path.parts if part.startswith("source=")),
                        "streamalpha" if "streamalpha" in path.parts else "unknown",
                    )
                    try:
                        lines = path.read_text(encoding="utf-8").splitlines()
                    except OSError as exc:
                        raise MartUnavailable(f"quarantine artifact unreadable: {path}: {exc}") from exc
                    for line in lines:
                        try:
                            item = json.loads(line)
                        except json.JSONDecodeError:
                            item = {}
                        source = str(item.get("source", source_hint))
                        error = str(item.get("error_type", "stream_event_violation"))
                        groups[(source, error)] = groups.get((source, error), 0) + 1
                        total += 1
            return {"total_records": total, "artifact_files": files, "groups": [
                {"source": source, "error_type": error, "records": count}
                for (source, error), count in sorted(groups.items())
            ]}
        return self._cached("quarantine", (), load)

    def sectors(self, limit: int) -> list[dict[str, Any]]:
        return self._cached("sectors", (limit,), lambda: self._query(
            """SELECT sector, max(trade_date) latest_date,
                      arg_max(sector_average_return, trade_date) latest_average_return,
                      arg_max(securities_with_returns, trade_date) securities_with_returns
               FROM main_marts.mart_sector_daily GROUP BY sector ORDER BY sector LIMIT ?""", [limit]))

    def sector_history(self, sector: str, limit: int) -> list[dict[str, Any]]:
        return self._cached("sector_history", (sector, limit), lambda: self._query(
            """SELECT trade_date, sector_average_return, securities_with_returns
               FROM main_marts.mart_sector_daily WHERE lower(sector)=lower(?)
               ORDER BY trade_date DESC LIMIT ?""", [sector, limit]))

    def market_breadth(self, limit: int) -> list[dict[str, Any]]:
        return self._cached("market_breadth", (limit,), lambda: self._query(
            """SELECT trade_date, market_breadth, advancers, decliners, unchanged,
                      securities_with_returns FROM main_marts.mart_market_daily
               ORDER BY trade_date DESC LIMIT ?""", [limit]))

    def storage(self) -> dict[str, Any]:
        budget = load_budget(self.budget_path)
        return {
            "project_bytes": tree_size(self.project_root),
            "raw_bytes": tree_size(self.raw_root),
            "transformed_bytes": tree_size(self.project_root / "warehouse/duckdb"),
            "metadata_bytes": tree_size(self.project_root / "warehouse/metadata"),
            "quarantine_bytes": tree_size(self.quarantine_root),
            "project_budget_bytes": int(float(budget["project_limits"]["total_disk_gb"]) * GB),
            "raw_budget_bytes": int(float(budget["project_limits"]["raw_data_gb"]) * GB),
        }

    def benchmarks(self) -> dict[str, Any]:
        if not self.benchmarks_path.is_file():
            raise MartUnavailable(f"benchmark artifact not found: {self.benchmarks_path}")
        try:
            return json.loads(self.benchmarks_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MartUnavailable(f"benchmark artifact unreadable: {exc}") from exc

    def lineage(self, dataset: str) -> dict[str, Any]:
        if not self.lineage_path.is_file():
            raise MartUnavailable(f"lineage artifact not found: {self.lineage_path}")
        def load():
            graph = json.loads(self.lineage_path.read_text(encoding="utf-8"))
            try:
                return descendants(graph, f"source.marketforge.raw.{dataset}")
            except ValueError as exc:
                raise KeyError(dataset) from exc
        return self._cached("lineage", (dataset,), load, include_lineage=True)

    def cache_stats(self) -> dict[str, int | float]:
        return self.cache.stats()
