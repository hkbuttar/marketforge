"""Measure cold and cached latency for an approved mart query."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from time import perf_counter

from backend.services.query import QueryService


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path("warehouse/duckdb/marketforge.duckdb"))
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if args.iterations < 2 or not 1 <= args.limit <= 500:
        parser.error("iterations must be >= 2 and limit must be between 1 and 500")
    service = QueryService(args.database, Path("warehouse/metadata/lineage.json"))
    samples = []
    for _ in range(args.iterations):
        started = perf_counter()
        service.securities(args.limit)
        samples.append((perf_counter() - started) * 1000)
    stats = service.cache_stats()
    print(json.dumps({
        "endpoint": "/api/securities", "iterations": args.iterations,
        "uncached_latency_ms": samples[0],
        "cached_latency_ms_mean": sum(samples[1:]) / len(samples[1:]),
        "cache_hit_rate": stats["hit_rate"], "cache_stats": stats,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
