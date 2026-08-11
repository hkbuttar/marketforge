"""Read bounded historical source extracts from local files or HTTP(S)."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen


def _content(location: str) -> tuple[str, str]:
    parsed = urlparse(location)
    if parsed.scheme in {"http", "https"}:
        with urlopen(location, timeout=60) as response:  # nosec: caller selects provider URL
            return response.read().decode("utf-8"), Path(parsed.path).suffix.lower()
    path = Path(parsed.path if parsed.scheme == "file" else location)
    return path.read_text(encoding="utf-8"), path.suffix.lower()


def read_records(location: str, input_format: str | None = None) -> list[dict[str, Any]]:
    content, suffix = _content(location)
    input_format = input_format or {".csv": "csv", ".json": "json", ".jsonl": "jsonl"}.get(suffix)
    if input_format == "csv":
        return [dict(row) for row in csv.DictReader(io.StringIO(content))]
    if input_format == "json":
        payload = json.loads(content)
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise ValueError("JSON input must be an array of objects")
        return payload
    if input_format == "jsonl":
        return [json.loads(line) for line in content.splitlines() if line.strip()]
    raise ValueError("input format must be csv, json, or jsonl")
