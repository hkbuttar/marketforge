# Split deployment

MarketForge deliberately does not upload or operate the complete local lake in a
free web service.

> The production-style ingestion and orchestration pipeline is intentionally
> local-first due to the project's CPU/storage constraint. The hosted demo serves
> a compact materialized snapshot; architecture and failure tests run against the
> full local system.

## Public frontend: Vercel

Import the repository in Vercel with `frontend` as the root directory. The checked
in `vercel.json` builds the Vite SPA and supplies its history fallback. Configure:

```text
VITE_API_URL=https://<marketforge-api>.onrender.com
```

## Lightweight API: Render

The root `render.yaml` creates one Python web service. Its build installs only
`requirements-demo.txt`, generates a deterministic 90-row, three-security ZSTD
Parquet snapshot and read-only DuckDB marts, then starts FastAPI. Set
`MARKETFORGE_CORS_ORIGINS` to the exact Vercel origin.

The snapshot contains AAPL, MSFT, and XOM over 30 weekday observations. It is
generated from code so no credentials, live provider, large binary, or private
source extract enters the deployment. The benchmark endpoint is explicitly
tagged as full-local-lake measurement; it does not claim the hosted fixture was
used for those results.

## What remains local

- Tiingo acquisition and retained full history
- incremental and historical ingestion
- dbt builds and Dagster orchestration
- Kafka/StreamAlpha continuous consumption
- compaction, backfills, quality drills, and failure injection
- historical time-travel catalogs

The hosted API and frontend are disposable read-only demonstrations. Rebuilding
the service recreates its snapshot; it is not a system of record.
