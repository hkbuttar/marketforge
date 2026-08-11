# Atomic writes and crash recovery

Parquet files never write directly to their canonical partition. Each run writes
under `data/raw/.tmp/<run-id>/...` on the same filesystem, reads the completed file
back through DuckDB, verifies its exact columns and row count, and only then uses
an atomic rename into `data/raw/<dataset>/year=.../month=...`. All partitions are
staged and validated before any is promoted. Run IDs are path-safe and bounded.

The final file is the commit marker for a partition. A success manifest is written
only after every promotion, and an incremental checkpoint advances only after the
success manifest. Raw files remain immutable.

| Crash window | Durable state | Recovery |
| --- | --- | --- |
| Before temporary write | No new artifact | Retry the same source window. |
| During temporary write | A noncanonical `.writing` file may remain | Retry; the exact run-scoped `.writing` file is discarded and rebuilt. |
| After temporary validation, before promotion | Valid file under the run-scoped `.tmp` tree | Retry; staging is rebuilt from the immutable source input and revalidated. |
| During multi-partition promotion | Some final files and some staged files | Retry with a new or identical run ID; retained logical keys are recognized, and only missing partitions are written. |
| After promotion, before success manifest | Final immutable files, no success manifest | Retry; idempotency recognizes all rows and creates a zero-write success manifest. |
| After success manifest, before checkpoint | Final files and manifest, old checkpoint | Retry the incremental window; rows are recognized as replays, then the checkpoint advances. |

Empty run-scoped staging directories are harmless and may be removed by a future
compaction/housekeeping command after verifying that no ingestion process is using
them. Only `.parquet` files in final year/month paths are queried as raw data.

Tests inject failures at `before_temp_write`, `after_temp_write`,
`after_temp_validation`, `after_final_promotion`, and `before_manifest`. This hook
is intended for deterministic failure testing; normal callers omit it.
