# End-to-end demonstration

Run the complete isolated scenario with one command:

```bash
python -m scripts.demo
```

The command creates a temporary lake, executes the Aug 10 initial state and all
five planned events, runs the price dbt dependency branch where relevant, and
writes `demo/results/latest.json` plus `latest.md`. It never changes the normal
`data/` or `warehouse/` trees.

The late Aug 8 event represents a corrected provider omission: the missing bar is
appended to immutable raw storage, identified as late against the Aug 11
watermark, and consumed by the selected price mart branch. Existing canonical
values are never silently overwritten.

For a fast ingestion-only rehearsal, use `--skip-dbt`. The full command is the
version intended for a screen recording because its report exposes evidence for
quarantine isolation, late-data auditing, idempotent replay, and crash recovery.
