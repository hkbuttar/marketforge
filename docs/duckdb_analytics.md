# DuckDB analytical layer

DuckDB views query raw Parquet in place; MarketForge does not copy the lake into a
second warehouse file. `warehouse/duckdb/views.sql` provides the direct SQL form.
`install_raw_views(connection, raw_root)` is safer for application and test startup
because it creates views only for datasets that currently have Parquet files.

Hive `year` and `month` predicates should accompany date predicates where known.
DuckDB then prunes whole partition files before applying symbol and date filters.
For example:

```sql
SELECT *
FROM raw_prices
WHERE year = '2024' AND month = '06'
  AND symbol = 'AAPL'
  AND date BETWEEN DATE '2024-06-01' AND DATE '2024-06-30';
```

## Query benchmark

`benchmarks/query_latency.py` creates a temporary five-year lake containing 100
symbols, 130,400 weekday bars, and 60 year/month Parquet files. Each cold number is
the first measured execution on a new DuckDB connection. Each warm number is the
median of ten executions after one warm-up on a shared connection. The OS page
cache is not flushed, so “cold” means a cold DuckDB connection/catalog, not a cold
machine disk cache.

Initial Apple M4 Pro / DuckDB 1.5.5 results from 2026-08-11:

| Query | Cold | Warm median | Result rows | Files scanned | Largest plan row count |
| --- | ---: | ---: | ---: | ---: | ---: |
| One-symbol point | 20.612 ms | 7.404 ms | 1 | 1/60 | 1 |
| 30-day symbol slice | 7.852 ms | 7.348 ms | 20 | 1/60 | 20 |
| One-year symbol slice | 9.825 ms | 10.799 ms | 262 | 12/60 | 262 |
| Cross-sectional day | 8.686 ms | 7.322 ms | 100 | 1/60 | 100 |
| Full-history aggregation | 9.604 ms | 9.546 ms | 100 | 60/60 | 130,400 |

File counts come from `EXPLAIN ANALYZE` and directly demonstrate partition pruning.
“Largest plan row count” is the largest row count DuckDB exposes on a physical-plan
operator; it is not a claim about low-level Parquet values decoded or bytes read.
DuckDB 1.5.5 does not expose a reliable physical rows-scanned counter in this text
plan, so the benchmark does not invent one. Re-run the script after changing the
DuckDB version, hardware, partitioning, or compression.
