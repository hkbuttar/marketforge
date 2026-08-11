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
