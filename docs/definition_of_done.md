# Final Definition of Done

All 18 final conditions have reproducible evidence in the repository. The isolated demonstration was rerun on 2026-08-11 and passed every runtime invariant: fresh ingestion, healthy freshness, quarantine isolation, late-arrival auditing, monotonic checkpoints, zero-write replay, no partial publication after an injected crash, unchanged checkpoint after failure, and exactly-once canonical recovery on restart.

| # | Demonstration | Reproducible entry point |
| ---: | --- | --- |
| 1 | Bootstrap the environment | `python3 -m venv venv && pip install -r requirements.txt` |
| 2 | Load historical data | `python -m scripts.backfill --help` |
| 3 | Query Parquet with DuckDB | `python -m unittest tests.integration.test_duckdb_analytics` |
| 4 | Produce tested dbt marts | `dbt build --project-dir dbt --profiles-dir dbt` |
| 5 | Run a daily increment | `python -m unittest tests.integration.test_incremental` |
| 6 | Prove replay idempotency | `python -m scripts.prove_idempotency` |
| 7 | Backfill an arbitrary range | `python -m scripts.backfill --help` |
| 8 | Process late data | `python -m scripts.demo` |
| 9 | Reject breaking schema | `python -m unittest tests.unit.test_source_contracts` |
| 10 | Quarantine malformed input | `python -m scripts.demo` |
| 11 | Recover after a killed write | `python -m scripts.demo` |
| 12 | Trace mart lineage | `python -m scripts.build_lineage --target mart_security_daily` |
| 13 | View freshness and quality | `npm run dev --prefix frontend` |
| 14 | Query through FastAPI | `python -m unittest tests.integration.test_query_api` |
| 15 | Compare incremental/full cost | `python -m benchmarks.incremental_vs_full` |
| 16 | Show disk/compression results | `python -m benchmarks.storage_efficiency` |
| 17 | Run the complete test suite | `python -m scripts.test_summary` |
| 18 | Reproduce engineering experiments | `python -m benchmarks.run` |

Run the evidence-coverage gate with:

```bash
python -m scripts.check_definition_of_done
```

This command checks that every condition retains its implementation, test, documentation, and reproducible entry point. Behavioral execution remains the responsibility of the deterministic suite, dbt build, frontend build, and `scripts.demo`; merely possessing filenames is not presented as runtime proof.

## Status boundaries

The final Definition of Done is complete and the Step 57 robustness audit passes. The independent Step 56 MVP scale target remains 99 of 100 Tiingo securities. These statements are intentionally separate: the final checklist tests platform behavior, while the MVP gate additionally imposes a universe-size target.
