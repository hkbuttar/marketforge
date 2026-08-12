# MVP readiness

Step 56 defines a deliberately narrow MVP. MarketForge passes all 13 gates. The retained lake has one real historical provider, 140,700 Tiingo rows, and the required 100-security universe.

| Requirement | Status | Evidence |
| --- | --- | --- |
| 100 securities | Pass | 100 distinct Tiingo symbols are loaded and reconciled through 2026-08-11 |
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

## Maintaining the scale gate

Load complete history before adding any future symbol to `config/price_universe.txt`, then reconcile provider and canonical counts. Credentials and raw Tiingo data remain local and must not be committed. Benchmark results in the repository retain their measured dataset size and should not be generalized to the expanded lake until a complete rerun publishes replacement artifacts.

No other MVP gate depends on adding another provider or distributed infrastructure.
