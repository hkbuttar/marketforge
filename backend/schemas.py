"""Public API response contracts."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PageMeta(ApiModel):
    limit: int
    returned: int


class SecuritySummary(ApiModel):
    symbol: str
    source: str
    latest_price_date: date
    latest_price: float


class SecurityList(ApiModel):
    data: list[SecuritySummary]
    meta: PageMeta


class SecurityDetail(SecuritySummary):
    latest_fundamental_period_end: date | None = None
    latest_fundamental_filed_at: datetime | None = None
    available_fundamental_metrics: int | None = None
    latest_earnings_timestamp: datetime | None = None
    latest_eps_actual: float | None = None
    latest_eps_surprise: float | None = None


class PricePoint(ApiModel):
    trade_date: date
    close: float
    daily_return: float | None = None
    rolling_20d_return: float | None = None
    rolling_20d_volatility: float | None = None
    volume: int


class SecurityHistory(ApiModel):
    symbol: str
    source: str | None
    data: list[PricePoint]
    meta: PageMeta


class DatasetHealth(ApiModel):
    dataset: str
    status: str
    row_count: int
    null_rate: float | None
    duplicate_count: int
    quarantine_count: int
    latest_event_time: datetime | None
    last_successful_run: str | None
    last_successful_run_at: datetime | None


class DatasetHealthList(ApiModel):
    data: list[DatasetHealth]
    meta: PageMeta


class DatasetSummary(ApiModel):
    dataset: str
    status: str
    row_count: int


class DatasetList(ApiModel):
    data: list[DatasetSummary]
    meta: PageMeta


class DatasetSchema(ApiModel):
    dataset: str
    contract_version: int
    fields: list[dict[str, Any]]
    unique_by: list[str]
    idempotency_by: list[str]


class PipelineRun(ApiModel):
    run_id: str
    job_name: str
    dataset: str
    run_type: str
    started_at: datetime
    finished_at: datetime
    status: str
    records_fetched: int
    records_written: int
    records_rejected: int
    error: str | None = None


class QualityResult(ApiModel):
    result_id: str
    run_id: str | None = None
    dataset: str
    check_name: str
    status: str
    observed_value: float | None = None
    expected_value: str | None = None
    message: str
    evaluated_at: datetime


class SectorSummary(ApiModel):
    sector: str
    latest_date: date
    latest_average_return: float | None = None
    securities_with_returns: int


class SectorPoint(ApiModel):
    trade_date: date
    sector_average_return: float | None = None
    securities_with_returns: int


class BreadthPoint(ApiModel):
    trade_date: date
    market_breadth: float | None = None
    advancers: int
    decliners: int
    unchanged: int
    securities_with_returns: int


class LineageResponse(ApiModel):
    dataset: str
    nodes: list[dict[str, Any]]
    edges: list[dict[str, str]]
    generated_at: datetime


class LivenessResponse(ApiModel):
    status: str
    checked_at: datetime


class ComponentCheck(ApiModel):
    component: str
    status: str
    detail: str


class ReadinessResponse(ApiModel):
    status: str
    checked_at: datetime
    checks: list[ComponentCheck]
    cache: dict[str, int | float]
