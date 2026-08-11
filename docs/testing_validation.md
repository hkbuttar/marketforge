# Testing and validation summary

MarketForge treats correctness and recovery behavior as executable requirements.
The category-to-test mapping is stored in `tests/validation_manifest.json` and a
meta-test fails if a required category is missing or points to a nonexistent test
module.

Run the complete suite and emit its measured summary with:

```bash
python -m scripts.test_summary
```

The command reports tests run, passed, failed, errored, and skipped. A test method
containing subtests or Hypothesis examples counts once under `unittest`; its many
generated cases are not inflated into the headline number.

| Required category | Primary evidence |
| --- | --- |
| Source contracts and normalization | Contract unit tests and dbt staging build |
| Idempotency and deduplication | Replay/conflict failure tests and ingestion integration tests |
| Atomic writes and process failures | Injected crash windows, disk-full, provider, and dbt drills |
| Checkpoints, overlap, and late data | Incremental, range-backfill, and property tests |
| Schema evolution and quality | Drift, type-change, synthetic-defect, and statistical checks |
| Reconciliation | Unit and property-based conservation invariants |
| dbt and derived metrics | Full fixture build plus hand-calculated analytical expectations |
| Freshness and storage guardrails | SLA boundary and cleanup/disk-budget tests |
| Compaction | Row, schema, archive, and rollback equivalence tests |
| FastAPI | Response contracts, bounds, liveness, readiness, and failure responses |
| End-to-end | Source-to-API fixture and five-event demonstration scenario |

CI also runs Ruff, parses/builds dbt against deterministic fixtures, and compiles
the React/Vite production bundle. Live Tiingo and StreamAlpha availability is not
part of the deterministic push gate.
