"""Polling adapter for the public StreamAlpha anomaly backend."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

from ingestion.streaming.kafka import KafkaMessage


DEFAULT_BASE_URL = "https://streamalpha-backend.onrender.com"


class StreamAlphaBackendError(RuntimeError):
    """The backend response could not be converted into durable events."""


def _identity(item: dict[str, Any]) -> tuple[str, int]:
    material = {
        "ticker": item.get("ticker"), "window_start": item.get("window_start"),
        "anomaly_type": item.get("anomaly_type"), "detected_at": item.get("detected_at"),
        "details": item.get("details"),
    }
    digest = hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    return f"streamalpha:{digest}", int(digest[:15], 16)


def fetch_anomalies(
    *, base_url: str = DEFAULT_BASE_URL, limit: int = 500,
    ticker: str | None = None, anomaly_type: str | None = None,
    timeout: int = 60,
) -> list[KafkaMessage]:
    if not 1 <= limit <= 10_000:
        raise ValueError("limit must be between 1 and 10000")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base_url must be an absolute HTTP(S) URL")
    query: dict[str, str | int] = {"limit": limit}
    if ticker:
        query["ticker"] = ticker.upper()
    if anomaly_type:
        query["anomaly_type"] = anomaly_type
    endpoint = urljoin(base_url.rstrip("/") + "/", "anomalies") + "?" + urlencode(query)
    request = Request(endpoint, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:  # nosec: caller-configured HTTP(S) backend
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        if isinstance(exc, StreamAlphaBackendError):
            raise
        raise StreamAlphaBackendError(f"StreamAlpha anomaly request failed: {exc}") from exc
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise StreamAlphaBackendError("StreamAlpha /anomalies must return an array of objects")
    messages = []
    for item in payload:
        required = {"ticker", "window_start", "anomaly_type", "detected_at"}
        missing = required - set(item)
        if missing:
            raise StreamAlphaBackendError(f"anomaly is missing fields: {sorted(missing)}")
        event_id, offset = _identity(item)
        details = item.get("details") if isinstance(item.get("details"), dict) else {}
        event = {
            "event_id": event_id,
            "symbol": item["ticker"],
            "event_timestamp": item["window_start"],
            "event_type": item["anomaly_type"],
            "price": details.get("price"),
            "anomaly_score": details.get("anomaly_score", details.get("changepoint_probability")),
            "details": details,
            "detected_at": item["detected_at"],
        }
        messages.append(KafkaMessage(
            "streamalpha.backend.anomalies", 0, offset,
            json.dumps(event, separators=(",", ":")).encode(),
        ))
    return messages


class PolledAnomalyConsumer:
    """Adapter that gives one HTTP result page micro-batch commit semantics."""

    def __init__(self, messages: list[KafkaMessage]):
        self._messages = list(messages)
        self.committed: list[KafkaMessage] = []

    def poll(self, _timeout: float) -> KafkaMessage | None:
        return self._messages.pop(0) if self._messages else None

    def commit(self, messages: list[KafkaMessage]) -> None:
        self.committed.extend(messages)
