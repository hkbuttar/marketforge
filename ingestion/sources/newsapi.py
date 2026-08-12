"""NewsAPI metadata-only adapter."""

from __future__ import annotations

import hashlib
import os
from datetime import date

from .http_json import SourceHTTPError, get_json


URL = "https://newsapi.org/v2/everything"


def fetch_news(query: str, *, start: date, end: date, page_size: int = 100,
               api_key: str | None = None) -> list[dict]:
    key = api_key or os.getenv("NEWSAPI_API_KEY")
    if not key:
        raise SourceHTTPError("NEWSAPI_API_KEY is not set")
    if not 1 <= page_size <= 100:
        raise ValueError("page_size must be between 1 and 100")
    payload = get_json(URL, params={
        "q": query, "from": start.isoformat(), "to": end.isoformat(),
        "language": "en", "sortBy": "publishedAt", "pageSize": str(page_size),
    }, headers={"X-Api-Key": key})
    if payload.get("status") != "ok":
        raise SourceHTTPError(f"NewsAPI returned status {payload.get('status')!r}")
    rows = []
    for item in payload.get("articles", []):
        if not item.get("title") or not item.get("publishedAt"):
            continue
        identity = hashlib.sha256(
            f"{item.get('url', '')}\0{item['publishedAt']}".encode()
        ).hexdigest()
        rows.append({
            "event_timestamp": item["publishedAt"], "headline": item["title"],
            "url": item.get("url"), "publisher": (item.get("source") or {}).get("name"),
            "source_record_id": identity,
        })
    return rows
