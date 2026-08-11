# Query serving

The FastAPI service exposes allowlisted, parameterized queries over published dbt
marts. It never accepts SQL and never reads raw Parquet directly.

```bash
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
```

Set `MARKETFORGE_DATABASE` and `MARKETFORGE_LINEAGE` to override the default mart
database and lineage artifact. Available routes are:

- `GET /api/securities?limit=100`
- `GET /api/securities/{symbol}`
- `GET /api/securities/{symbol}/history?source=tiingo&limit=252`
- `GET /api/pipeline/health`
- `GET /api/datasets`
- `GET /api/datasets/{dataset}/lineage`

Limits are validated and capped. Symbols and sources are bound parameters. Missing
published marts return HTTP 503; missing entities return HTTP 404. Interactive
OpenAPI documentation is available at `/docs` while the server is running.

## Cache

Approved query results use a bounded in-process TTL/LRU cache. Keys contain the
endpoint, normalized parameters, published DuckDB file version, and dataset-build
metadata version. Lineage queries also include the lineage artifact version. A new
mart or dataset build therefore cannot reuse a stale result.

Defaults are a 30-second TTL and 256 entries. Returned values are copied so callers
cannot mutate cached state. Benchmark the active mart database with:

```bash
python -m scripts.benchmark_cache --iterations 20 --limit 100
```

The output reports uncached latency, mean cached latency, hit rate, and eviction
counts. The cache remains intentionally process-local; no Redis service is needed
for the laptop deployment.

On 2026-08-11, a 20-query benchmark over a 50-security snapshot derived from the
local Tiingo partitions measured 4.75 ms for the uncached request and 0.145 ms mean
end-to-end latency for cached requests (32.83x faster, 95.2% hit rate including the
initial miss). This is a microbenchmark, not a concurrency claim; it supports the
small in-process cache but not the operational cost of an external cache service.
