# Operational metadata and audit store

MarketForge retains JSON manifests and Parquet files as source evidence, then
indexes them into `warehouse/metadata/operational.sqlite` for inexpensive local
queries:

```bash
python -m scripts.build_audit_store
```

The SQLite database contains:

- `pipeline_runs`: ingestion type, timing, status, and fetched/written/rejected counts
- `dataset_versions`: partition, SHA-256 content hash, physical row count, creation time, and producing run
- `quality_results`: statistical and freshness checks with status, observed/expected values, and reasons
- `checkpoints`: dataset/source event watermarks and the run that advanced them

Synchronization is idempotent. SQLite uses WAL mode and full synchronous commits;
it remains an operational index rather than the sole copy of audit evidence. The
database and WAL files are ignored by Git, while schemas and synchronization code
are version controlled.

Example query:

```sql
SELECT dataset, status, records_fetched, records_written, records_rejected
FROM pipeline_runs
ORDER BY finished_at DESC;
```
