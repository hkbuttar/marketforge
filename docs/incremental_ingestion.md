# Incremental ingestion

Incremental runs are separate from historical backfills and use a SQLite
checkpoint keyed by dataset and source. The first run requires `--start-date`;
later runs derive their lower bound from the last successfully persisted event
date. `--overlap-days N` moves that lower bound backward for providers that revise
recent observations. Existing source identities are reconciled against retained
Parquet files and skipped, while new provider identities remain append-only.

With no overlap, a checkpoint of `2026-08-10` requests from `2026-08-11` onward.
An overlap of 3 instead reprocesses `2026-08-08` through the requested upper bound.

```bash
.venv/bin/python -m ingestion.cli run \
  --dataset prices --source provider-name --input extracts/latest.jsonl \
  --start-date 2026-08-01 --through-date 2026-08-11 --overlap-days 3
```

The input adapter currently represents the provider's available response and is
filtered to the requested window before validation and storage. Future API-specific
adapters should use the same `fetch_from` and `fetch_through` bounds in the remote
request so they do not download irrelevant history.

Checkpoint updates occur only after validation, deduplication, Parquet persistence,
and run-manifest creation succeed. Updates are transactional, durable, monotonic,
and never advance from quarantined-only input. If a process fails before completion,
the prior checkpoint remains available for a safe retry.
