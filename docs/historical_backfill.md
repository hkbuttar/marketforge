# Historical backfill

The backfill path accepts bounded CSV, JSON, or JSONL extracts from a local path
or an HTTP(S) provider URL. Acquisition is deliberately adapter-neutral: provider
credentials and response shapes belong in future source adapters, while this path
owns the invariant sequence of enrichment, contract validation, normalization,
batch and historical deduplication, immutable Parquet writes, quarantine, and run
metadata.

```bash
.venv/bin/python -m ingestion.cli backfill \
  --dataset prices \
  --source provider-name \
  --input extracts/prices.jsonl
```

All five initial datasets are supported. Missing `source_record_id` values receive
a deterministic SHA-256 identity from the received payload. Missing `source` and
`ingested_at` fields are assigned by the loader. Provider identities take priority
when available.

Raw artifacts use `data/raw/<dataset>/year=YYYY/month=MM/part-<run-id>.parquet`.
Files are Zstandard-compressed and created exclusively; existing raw history is
never overwritten. The loader scans retained source identities before writing, so
replaying an extract produces zero new logical records. Each run writes a JSON
manifest under `warehouse/metadata/ingestion_runs` with row counts, byte counts,
runtime, timestamps, quarantine counts, duplicates, and files written.

## Partition experiment

`benchmarks/partition_layout.py` compares year/month and year/month/day layouts
using 100 symbols and one calendar year of weekday bars (26,100 rows). It records
file count, compressed bytes, and median latency for a month-filtered analytical
query. Run it on the target machine whenever DuckDB or hardware changes:

```bash
.venv/bin/python benchmarks/partition_layout.py
```

The selected layout is **year/month**. Both layouts prune a query constrained by
year and month, while daily partitioning creates roughly 22 times more files for
this workload. Monthly files keep file-system and Parquet-footer overhead bounded
without sacrificing the dominant query filter. Exact results from the initial
machine (DuckDB 1.5.5, 2026-08-11) were:

| Layout | Rows | Files | Compressed bytes | Median June query |
| --- | ---: | ---: | ---: | ---: |
| year/month | 26,100 | 12 | 22,444 | 3.088 ms |
| year/month/day | 26,100 | 261 | 252,387 | 60.155 ms |

This synthetic test is intentionally small and is not a universal storage claim;
it demonstrates the file-count and footer overhead relevant to MarketForge's
bounded daily-bar workload.
