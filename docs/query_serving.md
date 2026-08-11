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
