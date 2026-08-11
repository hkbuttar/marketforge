# Partition compaction

Compact a month only when it contains at least two files below the configured
small-file threshold:

```bash
python -m scripts.compact --dataset prices --year 2026 --month 8
```

The command locks the partition against ingestion, checks the canonical schema,
rejects conflicting logical keys, deduplicates identical keys, writes and validates
one ZSTD Parquet file, and verifies unique row-count equivalence. It then swaps the
whole month directory on the same filesystem. Originals move to
`data/archive/compaction` for recovery and are not deleted by automatic cleanup.

Each run writes `warehouse/metadata/compactions/<id>.json` with file count, bytes,
rows, duplicates removed, and median count-query latency before and after. An
exception during directory promotion rolls the original partition back.
