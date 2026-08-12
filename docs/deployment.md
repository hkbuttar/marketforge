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
`requirements-demo.txt`, requests a bounded live snapshot from the configured
providers, writes ZSTD Parquet plus read-only DuckDB marts, then starts FastAPI.
Set `MARKETFORGE_CORS_ORIGINS` to the exact production Vercel origin. The
blueprint also sets `MARKETFORGE_CORS_ORIGIN_REGEX` for HTTPS preview and
production hosts under `vercel.app`; replace or remove that pattern if you use a
custom domain and do not need previews.

The snapshot contains recent AAPL, MSFT, and XOM prices plus bounded FRED macro,
Apple SEC fundamentals, Business Quant EPS, and NewsAPI metadata. Credentials are
read only during the Render build and never enter browser assets, API responses,
Parquet columns, or manifests. A provider failure fails the deployment instead of
silently publishing stale fixture data. The benchmark endpoint is explicitly
tagged as full-local-lake measurement; it does not claim the hosted snapshot was
used for those results.

Render requires these secret environment variables:

```text
MARKETFORGE_ENABLE_LIVE_HOSTED_DATA=1
TIINGO_API_KEY
FRED_API_KEY
SEC_USER_AGENT
SEC_CONTACT_EMAIL
BUSINESSQUANT_API_KEY
NEWSAPI_API_KEY
```

Only set the enable flag after confirming that each account permits public display
of the materialized fields. In particular, internal-use Tiingo terms and NewsAPI's
development plan do not automatically grant redistribution rights.

## What remains local

- Tiingo acquisition and retained full history
- incremental and historical ingestion
- dbt builds and Dagster orchestration
- Kafka/StreamAlpha continuous consumption
- compaction, backfills, quality drills, and failure injection
- historical time-travel catalogs

The hosted API and frontend are disposable read-only demonstrations. Rebuilding
the service recreates its snapshot; it is not a system of record.
