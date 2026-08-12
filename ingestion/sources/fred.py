"""FRED macroeconomic observation adapter."""

from __future__ import annotations

import os
from datetime import date

from .http_json import SourceHTTPError, get_json


SERIES_URL = "https://api.stlouisfed.org/fred/series"
OBSERVATIONS_URL = f"{SERIES_URL}/observations"


def fetch_series(series_id: str, *, start: date, end: date,
                 api_key: str | None = None) -> list[dict]:
    key = api_key or os.getenv("FRED_API_KEY")
    if not key:
        raise SourceHTTPError("FRED_API_KEY is not set")
    common = {"api_key": key, "file_type": "json"}
    metadata = get_json(SERIES_URL, params={**common, "series_id": series_id})
    series = metadata.get("seriess", [])
    if not series:
        raise SourceHTTPError(f"FRED returned no metadata for {series_id}")
    info = series[0]
    payload = get_json(OBSERVATIONS_URL, params={
        **common, "series_id": series_id,
        "observation_start": start.isoformat(), "observation_end": end.isoformat(),
    })
    rows = []
    for item in payload.get("observations", []):
        if item.get("value") in {None, "."}:
            continue
        observed = item["date"]
        rows.append({
            "series_id": series_id, "observation_date": observed,
            "released_at": None, "value": item["value"],
            "unit": info.get("units_short") or info.get("units") or "UNKNOWN",
            "frequency": info.get("frequency_short") or info.get("frequency") or "UNKNOWN",
            "source_record_id": f"{series_id.upper()}:{observed}",
        })
    return rows
