# MVP readiness

Step 56 defines a deliberately narrow MVP. MarketForge currently passes 12 of its 13 gates. The retained lake has one real historical provider and 68,894 Tiingo rows, but covers 49 securities rather than the required 100.

| Requirement | Status | Evidence |
| --- | --- | --- |
| 100 securities | **Open** | 49 distinct Tiingo symbols are loaded; the configured universe is intentionally expanded only after history is retained |
| One historical price source | Pass | Tiingo, 2021-01-04 through 2026-08-10 |
| Parquet | Pass | Immutable year/month raw partitions |
| DuckDB | Pass | Direct Parquet views and analytical catalogs |
| dbt | Pass | Staging, intermediate, and mart layers with tests |
| Dagster | Pass | Asset graph, schedules, retries, and checks |
| Incremental ingestion | Pass | Checkpoints, bounded overlap, and atomic promotion |
| Idempotency | Pass | Duplicate replays are no-ops; conflicting values fail |
| Contracts | Pass | Executable price schema and invariants |
| Backfill | Pass | Arbitrary range-backfill CLI |
| Quality checks | Pass | Contract, universe, statistical, and reconciliation gates |
| FastAPI | Pass | Allowlisted and bounded query endpoints |
| Basic observability dashboard | Pass | Eight-view React control plane |

Run the executable gate from the repository root:

```bash
python -m scripts.check_mvp
```

It exits nonzero until all requirements pass. For reporting workflows that must retain the JSON result while incomplete, use `--allow-incomplete`.

## Closing the scale gate

Choose 51 additional liquid U.S. symbols, load their complete Tiingo history over the same retained date range, reconcile the provider and canonical counts, and only then append them to `config/price_universe.txt`. Rerun the historical and incremental benchmarks afterward because file size, runtime, and memory claims will change. Credentials and raw Tiingo data remain local and must not be committed.

No other MVP gate depends on adding another provider or distributed infrastructure.
