"""Small bounded JSON client shared by authenticated source adapters."""

from __future__ import annotations

import gzip
import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class SourceHTTPError(RuntimeError):
    pass


def get_json(url: str, *, params: dict[str, str] | None = None,
             headers: dict[str, str] | None = None, timeout: int = 60,
             retries: int = 2) -> Any:
    target = f"{url}?{urlencode(params)}" if params else url
    request = Request(target, headers={"Accept": "application/json", **(headers or {})})
    for attempt in range(retries + 1):
        try:
            with urlopen(request, timeout=timeout) as response:  # nosec: adapter-owned HTTPS URLs
                payload = response.read()
                if response.headers.get("Content-Encoding", "").lower() == "gzip":
                    payload = gzip.decompress(payload)
            return json.loads(payload)
        except HTTPError as exc:
            if (exc.code == 429 or 500 <= exc.code < 600) and attempt < retries:
                time.sleep(min(2.0, 0.25 * (2 ** attempt)))
                continue
            raise SourceHTTPError(f"provider request failed: HTTP {exc.code}") from exc
        except (URLError, TimeoutError, UnicodeError, json.JSONDecodeError) as exc:
            if attempt < retries:
                time.sleep(min(2.0, 0.25 * (2 ** attempt)))
                continue
            raise SourceHTTPError(f"provider request failed: {exc}") from exc
    raise AssertionError("unreachable")
