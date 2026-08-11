# Range backfill engine

Range backfills are distinct from initial historical loads and normal incremental
runs:

```bash
python -m scripts.backfill \
  --dataset prices --start 2025-01-01 --end 2025-03-31 \
  --source synthetic-example --input extracts/prices.example.jsonl \
  --skip-downstream
```

The included extract is synthetic and intended only as a smoke test. For a real
backfill, replace `--source` with the provider identifier and `--input` with an
existing provider CSV, JSON, or JSONL extract. Omit `--skip-downstream` to run the
affected dbt models and tests after ingestion.

The engine filters the provider response by canonical event time, then uses the
same contracts, quarantine, idempotency checks, validated staging, and atomic
promotion as every ingestion path. Every manifest records `run_type=range_backfill`,
`requested_start`, and `requested_end` alongside actual row, file, byte, and event
metrics.

Raw files remain immutable. “Rewrite affected partitions” therefore means append a
new validated fragment only in months containing new logical records; existing
month files are never destructively replaced. Replaying or overlapping a range
recognizes retained keys and writes no duplicate fragments. Conflicting values for
an existing key still fail visibly under the established revision policy.

After raw promotion, the CLI uses dbt's source selector
`source:raw.<dataset>+` to rebuild only descendants of the affected dataset and run
their tests. `--skip-downstream` is available for controlled multi-source backfills
where one later rebuild is more efficient.

Range backfills never read or update the incremental checkpoint. A backfill for
2025 therefore cannot move a 2026 operational checkpoint backward. The overlapping
Jan–Mar then Feb–Apr integration test proves four logical monthly rows, no duplicate
keys, and only one new April fragment on the second operation.
