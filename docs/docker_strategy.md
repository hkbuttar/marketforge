# Docker strategy

MarketForge ships one optional image: the lightweight read-only demonstration
API. The container pins the Python runtime, installs only `requirements-demo.txt`,
generates the curated snapshot during the image build, runs as an unprivileged
user, and exposes a readiness health check.

```bash
make demo-image
make demo-container
```

The React application remains a static Vercel build, where a container adds no
deployment reproducibility. DuckDB, Parquet, dbt, Dagster, Tiingo ingestion,
compaction, and failure drills remain native local workflows. Metadata remains
SQLite; adding Postgres solely for a container topology would create an
unmeasured service dependency.

The build context excludes the full raw lake, virtual environments, tests, dbt
artifacts, historical catalogs, and frontend dependencies. The image therefore
cannot accidentally publish private source extracts or local credentials.

## Overhead measurement

Measure the built image and one idle healthy container with:

```bash
docker image inspect marketforge-api:local --format '{{.Size}}'
docker run --rm -d --name marketforge-api-measure -p 8000:8000 marketforge-api:local
docker stats --no-stream --format '{{.MemUsage}} {{.CPUPerc}}' marketforge-api-measure
docker stop marketforge-api-measure
```

Measured on 2026-08-11 with Docker Desktop's Linux/arm64 engine and the
`python:3.13-slim` base:

| Metric | Observed |
| --- | ---: |
| Docker build context | 556.17 kB |
| Uncompressed image size reported by Docker | 69,855,948 bytes (66.62 MiB) |
| Healthy idle container memory | 75.8 MiB |
| One-sample idle CPU | 0.43% |

The health check passed before resource sampling. Idle CPU is a single observation,
not a stable benchmark; registry transfer size, multi-request throughput, and
production autoscaling are outside this measurement and must not be inferred from
it. The result supports an optional demo API image, not containerizing the full
local platform.
