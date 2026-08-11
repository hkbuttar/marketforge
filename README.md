# MarketForge — Local-First Analytical Data Platform
Local-first analytical data platform for equity, fundamental, macro, and news data. Incremental ingestion, Parquet lake storage, DuckDB analytics, dbt transformations, Dagster orchestration, schema contracts, lineage, backfills, observability, and FastAPI serving — CPU-only.

## Resource budget

The initial development machine and explicit project limits are recorded in
[`config/resource_budget.yaml`](config/resource_budget.yaml). These are operational
ceilings, not capacity targets. In particular, the data and Docker budgets are
intentionally well below currently available disk space.

Benchmark any major job with the standard-library-only runner:

```bash
python3 scripts/benchmark.py \
  --job example \
  --input data/raw \
  --output data/marts \
  --rows 1000 \
  -- python3 -c "print('job complete')"
```

Results are appended as JSON Lines to `benchmarks/results.jsonl` by default. A
job that exceeds the configured runtime or peak-memory budget exits nonzero
after writing its measurement. Use `--help` for path, row-count, and limit
overrides. See [`benchmarks/README.md`](benchmarks/README.md) for metric semantics.

## Canonical data model

The entity definitions, grain, key strategy, and time semantics are documented
in [`docs/data_model.md`](docs/data_model.md). The corresponding DuckDB DDL lives
in [`warehouse/duckdb/init.sql`](warehouse/duckdb/init.sql).

## Source contracts

Executable contracts for every initial source domain live in
[`ingestion/contracts`](ingestion/contracts). They normalize accepted records and
route violations into immutable quarantine artifacts as described in
[`docs/source_contracts.md`](docs/source_contracts.md).

## Historical backfills

The backfill CLI validates and deduplicates bounded source extracts before writing
immutable, Zstandard-compressed, year/month Parquet partitions. Usage, run metadata,
and the partition-layout decision are documented in
[`docs/historical_backfill.md`](docs/historical_backfill.md).

Normal operation uses a separate checkpoint-driven incremental command with an
optional revision overlap window. See
[`docs/incremental_ingestion.md`](docs/incremental_ingestion.md).
