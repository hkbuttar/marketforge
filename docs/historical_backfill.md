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

`benchmarks/partition_layout.py` compares a single file, year/month partitions,
and year/month/symbol partitions using the real local price lake. It records file
count, compressed bytes, write cost, pruning, and median latency for four common
workloads. Run it whenever DuckDB, hardware, or the universe changes:

```bash
python -m benchmarks.partition_layout --iterations 3
```

The selected production layout remains **year/month**. A single file is fastest at
the current 140,703-row scale, but every incremental or late-arriving write would
require replacing that file, violating immutable raw storage. Monthly partitioning
adds 15% disk overhead and roughly 15 ms of file-discovery latency while pruning a
day or month query to one of 68 files. It preserves append-only writes and bounds
the amount of data affected operationally.

| Layout | Files | Size | Write | One day | One month/symbol | Full aggregation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Single file | 1 | 2,568,494 B | 35.916 ms | 0.472 ms | 1.448 ms | 2.931 ms |
| Year/month | 68 | 2,799,170 B | 65.760 ms | 16.839 ms | 17.047 ms | 19.290 ms |
| Year/month/symbol | 6,800 | 15,369,352 B | 1,370.805 ms | 1,628.152 ms | 1,626.944 ms | 1,820.128 ms |

Year/month/symbol is rejected: it creates 100 files for one cross-sectional day,
costs 498% more disk than a single file, and makes even a perfectly pruned
single-symbol query expensive because DuckDB must discover thousands of paths.
The measurements are machine- and dataset-specific, but the operational tradeoff
is explicit and reproducible.
