"""Business Quant analyst EPS estimates adapter."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone

from .http_json import SourceHTTPError, get_json


URL = "https://data.businessquant.com/estimates"
QUARTER = re.compile(r"^Q([1-4])\s+(\d{2}|\d{4})$")


def _period_end(label: str) -> str:
    match = QUARTER.fullmatch(label)
    if not match:
        raise ValueError(f"unsupported Business Quant quarter {label!r}")
    quarter, year = int(match.group(1)), int(match.group(2))
    year += 2000 if year < 100 else 0
    return (f"{year}-03-31", f"{year}-06-30", f"{year}-09-30", f"{year}-12-31")[quarter - 1]


def fetch_earnings(symbol: str, *, api_key: str | None = None,
                   observed_at: datetime | None = None) -> list[dict]:
    key = api_key or os.getenv("BUSINESSQUANT_API_KEY")
    if not key:
        raise SourceHTTPError("BUSINESSQUANT_API_KEY is not set")
    observed = (observed_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    payload = get_json(URL, params={"ticker": symbol, "mode": "eps", "api_key": key})
    groups = payload.get("data", [])
    quarterly = next((group for group in groups if group.get("dimension") == "quarter"), None)
    if quarterly is None:
        raise SourceHTTPError(f"Business Quant returned no quarterly EPS data for {symbol}")
    rows = []
    for item in quarterly.get("estimates", []):
        period = _period_end(item["period"])
        rows.append({
            "symbol": symbol, "event_timestamp": observed.isoformat(),
            "fiscal_period_end": period,
            "event_status": "REPORTED" if item["data_type"] == "reported" else "SCHEDULED",
            "eps_estimate": item.get("value_estimate"), "eps_actual": item.get("value_reported"),
            "source_record_id": f"{symbol.upper()}:{period}:eps:{item['data_type']}",
        })
    return rows
