"""Tiingo end-of-day equity price adapter."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import date
from typing import Any, Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://api.tiingo.com/tiingo/daily"
TICKER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,31}$")


class TiingoError(RuntimeError):
    """A Tiingo request or response could not be safely ingested."""


def _token(explicit: str | None) -> str:
    token = explicit or os.getenv("TIINGO_API_KEY")
    if not token:
        raise TiingoError("TIINGO_API_KEY is not set")
    return token


def _canonical_price(ticker: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        event_date = str(payload["date"])[:10]
        return {
            "symbol": ticker.upper(),
            "date": event_date,
            "open": payload["open"],
            "high": payload["high"],
            "low": payload["low"],
            "close": payload["close"],
            "volume": payload["volume"],
            "source_record_id": f"{ticker.upper()}:{event_date}",
        }
    except KeyError as exc:
        raise TiingoError(f"Tiingo price response is missing field {exc.args[0]!r}") from exc


def fetch_prices(
    tickers: Iterable[str], *, start: date, end: date,
    api_key: str | None = None, timeout: int = 60, max_retries: int = 2,
    backoff_seconds: float = 0.25, max_backoff_seconds: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Fetch raw (unadjusted) daily OHLCV rows for a bounded ticker set."""
    if end < start:
        raise ValueError("end must not precede start")
    if max_retries < 0 or backoff_seconds < 0 or max_backoff_seconds < 0:
        raise ValueError("retry settings must be non-negative")
    token = _token(api_key)
    records = []
    normalized = tuple(dict.fromkeys(ticker.strip().upper() for ticker in tickers))
    if not normalized:
        raise ValueError("at least one ticker is required")
    for ticker in normalized:
        if not TICKER_PATTERN.fullmatch(ticker):
            raise ValueError(f"invalid ticker {ticker!r}")
        query = urlencode({"startDate": start.isoformat(), "endDate": end.isoformat()})
        request = Request(
            f"{BASE_URL}/{ticker}/prices?{query}",
            headers={"Authorization": f"Token {token}", "Accept": "application/json"},
        )
        for attempt in range(max_retries + 1):
            try:
                with urlopen(request, timeout=timeout) as response:  # nosec: fixed HTTPS host
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if retryable and attempt < max_retries:
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    try:
                        delay = float(retry_after) if retry_after is not None else backoff_seconds * (2 ** attempt)
                    except ValueError:
                        delay = backoff_seconds * (2 ** attempt)
                    sleep(min(max_backoff_seconds, max(0.0, delay)))
                    continue
                detail = "authentication failed" if exc.code in {401, 403} else f"HTTP {exc.code}"
                attempts = f" after {attempt + 1} attempts" if retryable else ""
                raise TiingoError(f"Tiingo request failed for {ticker}{attempts}: {detail}") from exc
            except (URLError, TimeoutError) as exc:
                if attempt < max_retries:
                    sleep(min(max_backoff_seconds, backoff_seconds * (2 ** attempt)))
                    continue
                raise TiingoError(
                    f"Tiingo request failed for {ticker} after {attempt + 1} attempts: {exc}"
                ) from exc
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise TiingoError(f"Tiingo returned malformed JSON for {ticker}: {exc}") from exc
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise TiingoError(f"Tiingo returned an unexpected price payload for {ticker}")
        records.extend(_canonical_price(ticker, row) for row in payload)
    return records
