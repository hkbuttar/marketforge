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
