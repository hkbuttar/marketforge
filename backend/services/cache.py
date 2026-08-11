"""Small thread-safe TTL/LRU cache with observable latency counters."""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import asdict, dataclass
from threading import RLock
from time import monotonic, perf_counter
from typing import Any, Callable, Hashable


@dataclass(frozen=True)
class CacheStats:
    hits: int
    misses: int
    evictions: int
    entries: int
    hit_rate: float
    cached_latency_ms: float
    uncached_latency_ms: float


class QueryCache:
    def __init__(self, *, ttl_seconds: float = 30.0, max_entries: int = 256):
        if ttl_seconds < 0 or max_entries < 1:
            raise ValueError("cache TTL must be non-negative and capacity must be positive")
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._values: OrderedDict[Hashable, tuple[float, Any]] = OrderedDict()
        self._lock = RLock()
        self._hits = self._misses = self._evictions = 0
        self._hit_time = self._miss_time = 0.0

    def get_or_compute(self, key: Hashable, compute: Callable[[], Any]) -> Any:
        started = perf_counter()
        now = monotonic()
        with self._lock:
            cached = self._values.get(key)
            if cached and cached[0] > now:
                self._values.move_to_end(key)
                self._hits += 1
                value = deepcopy(cached[1])
                self._hit_time += perf_counter() - started
                return value
            if cached:
                del self._values[key]
            self._misses += 1
        value = compute()
        elapsed = perf_counter() - started
        with self._lock:
            self._miss_time += elapsed
            self._values[key] = (monotonic() + self.ttl_seconds, deepcopy(value))
            self._values.move_to_end(key)
            while len(self._values) > self.max_entries:
                self._values.popitem(last=False)
                self._evictions += 1
        return value

    def clear(self) -> None:
        with self._lock:
            self._values.clear()

    def stats(self) -> dict[str, int | float]:
        with self._lock:
            total = self._hits + self._misses
            value = CacheStats(
                hits=self._hits, misses=self._misses, evictions=self._evictions,
                entries=len(self._values), hit_rate=self._hits / total if total else 0.0,
                cached_latency_ms=(self._hit_time / self._hits * 1000) if self._hits else 0.0,
                uncached_latency_ms=(self._miss_time / self._misses * 1000) if self._misses else 0.0,
            )
            return asdict(value)
