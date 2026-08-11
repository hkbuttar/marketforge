# Maximum-robustness readiness

MarketForge passes all 15 capability gates in Step 57. This layer is additive: it strengthens the original source-to-mart pipeline without changing Parquet as the raw system of record or making StreamAlpha a dependency.

| Capability | Status | Primary evidence |
| --- | --- | --- |
| Multiple sources | Pass | File, Tiingo, StreamAlpha HTTP, and optional Kafka adapters |
| Late-arriving data | Pass | Bounded overlap and immutable correction fragments |
| Schema evolution | Pass | Additive-field policy and breaking-change rejection |
| Quarantine | Pass | Durable rejected payloads with diagnostics |
| Reconciliation | Pass | Expected, written, duplicate, quarantine, and canonical counts |
| Storage benchmarks | Pass | CSV versus Parquet/ZSTD measurements |
| Partition benchmarks | Pass | Single, year/month, and year/month/symbol comparison |
| Compaction | Pass | Isolated rewrite with row/schema equivalence checks |
| Resource guardrails | Pass | Disk, memory, runtime, and free-space budgets |
| Failure injection | Pass | Provider, disk, process, metadata, and dbt failures |
| Dataset manifests | Pass | Content-addressed reproducible build manifests |
| Full lineage | Pass | dbt-manifest-derived source-to-mart graph |
| CI | Pass | Deterministic Python, dbt, and frontend gates plus manual live smoke test |
| Public demo | Pass | Reproducible Render API and Vercel frontend deployment definitions |
| StreamAlpha adapter | Pass | Replay-safe HTTP polling and optional Kafka consumption |

Run the executable audit:

```bash
python -m scripts.check_robustness
```

The public-demo check validates that deployment definitions and the deterministic snapshot builder are present. It does not claim that an external deployment is currently reachable; uptime requires a separate network health check.

## Relationship to the MVP gate

Maximum-robustness capability coverage is complete, but the independent Step 56 scale gate remains at 49 of 100 Tiingo securities. Therefore `check_robustness` returns `READY` while `check_mvp` correctly returns `NOT_READY`. This is an implementation-order deviation from the plan, not a reason to weaken either gate.
