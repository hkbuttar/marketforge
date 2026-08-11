# MarketForge — Local-First Analytical Data Platform
Local-first analytical data platform for equity, fundamental, macro, and news data. Incremental ingestion, Parquet lake storage, DuckDB analytics, dbt transformations, Dagster orchestration, schema contracts, lineage, backfills, observability, and FastAPI serving — CPU-only.

## Testing and validation

The current deterministic suite passes **111 tests across all 19 required
validation categories**, with zero failures, errors, or skips, as measured on
2026-08-11. This count uses `unittest` test methods; Hypothesis examples and
subtests are intentionally not inflated into separate headline tests.

```bash
python -m scripts.test_summary
```

Coverage includes contracts, normalization, idempotency, deduplication, atomic
recovery, checkpoints, late data, schema evolution, overlap, quality,
reconciliation, dbt and derived metrics, freshness, resource guardrails,
compaction, FastAPI, end-to-end behavior, and injected failures. The complete
evidence map and counting semantics are in
[`docs/testing_validation.md`](docs/testing_validation.md).

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

Idempotency keys and replay/conflict behavior are documented and executable via
[`docs/idempotency.md`](docs/idempotency.md).

Raw Parquet writes use validated same-filesystem staging and atomic promotion;
crash windows and recovery behavior are detailed in
[`docs/atomic_writes.md`](docs/atomic_writes.md).

DuckDB queries Parquet directly through reusable raw views. Analytical benchmark
methodology and results are in [`docs/duckdb_analytics.md`](docs/duckdb_analytics.md).

The dbt transformation project begins with normalized, deduplicated staging views
for every initial dataset. See [`docs/dbt_staging.md`](docs/dbt_staging.md).

Reusable return, volatility, point-in-time fundamental, earnings-surprise, and
macro-alignment logic lives in the dbt intermediate layer described in
[`docs/dbt_intermediate.md`](docs/dbt_intermediate.md).

Consumer-facing security, market, company snapshot, and pipeline-health marts are
documented in [`docs/dbt_marts.md`](docs/dbt_marts.md).

The multi-level dbt quality gate and source freshness policy are documented in
[`docs/dbt_testing.md`](docs/dbt_testing.md).

Dagster asset lineage and the four local jobs are documented in
[`docs/dagster_orchestration.md`](docs/dagster_orchestration.md).

Optional laptop-safe schedules and manual commands are documented in
[`docs/scheduling.md`](docs/scheduling.md).

Explicit, overlap-safe range backfills are documented in
[`docs/backfill_engine.md`](docs/backfill_engine.md).
