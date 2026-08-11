# Dagster orchestration

`orchestration.definitions:defs` exposes MarketForge as a software-defined asset
graph rather than one monolithic script:

```text
raw_* -> stg_* -> int_* -> mart_* -> quality_gate -> api_ready
```

Raw assets can operate in three resource-configured modes. `observe` verifies final
Parquet and a successful run manifest; `incremental` runs the checkpoint-driven
loader from configured extracts; `backfill` runs the historical loader. A raw asset
cannot materialize from a manifest without final Parquet, so ingestion success is a
real upstream gate.

Staging, intermediate, and mart models are grouped into one dbt invocation per
layer. This keeps the asset lineage detailed while avoiding concurrent writers to
the same local DuckDB file. `quality_gate` runs all dbt tests and source
freshness checks. Only then can `api_ready` materialize.

Jobs:

- `daily_incremental`: complete graph, configured with incremental raw mode.
- `historical_backfill`: complete graph, configured with backfill raw mode.
- `quality_validation`: quality gate and downstream API readiness.
- `rebuild_marts`: rebuild consumer marts, validate them, and mark API readiness.

Launch locally after installing the orchestration extra:

```bash
.venv/bin/pip install -e '.[transformation,orchestration]'
.venv/bin/dagster dev -m orchestration.definitions
```

The resource exposes local paths, dbt executable/profile locations, source name,
mode, overlap, initial date, and one extract path per dataset. Dagster records asset
materializations, dependencies, failures, execution duration, and the row/file/run
metadata emitted by raw assets. Retry behavior uses the ingestion idempotency and
atomic-write guarantees established earlier.
