# Data reconciliation

Every completed ingestion run is checked using two independent identities:

```text
source records fetched = accepted + rejected + deduplicated
post-write rows - pre-write rows = accepted = records written
```

Here, "accepted" means new logical records retained after contract validation and
idempotency reconciliation. Replay duplicates are counted separately and never
increase canonical row counts. A quarantined record is rejected, not silently
dropped.

The run manifest contains `records_written`, pre/post row counts, expected and
actual deltas, and `reconciliation_status`. A separate immutable-by-run audit is
written to `warehouse/metadata/reconciliation/<run_id>.json`, including the full
accounting and any discrepancy messages.

If either identity fails, MarketForge writes the discrepancy audit and raises
`ReconciliationError` instead of publishing a successful ingestion manifest.
This makes unexplained loss, duplicate inflation, and unexpected partition changes
visible pipeline failures.
