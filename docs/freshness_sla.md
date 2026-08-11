# Freshness and SLA model

Freshness is evaluated per dataset; an old event date is not automatically stale.

| Dataset | Evidence basis | Degraded | Failed | Expected cadence |
| --- | --- | ---: | ---: | --- |
| Prices | Lag behind latest expected U.S. market close | 24h | 48h | Market days after close |
| Fundamentals | Last successful source check | 7d | 14d | Daily check, quarterly events |
| Earnings | Last successful source check | 1d | 2d | Weekday check |
| Macro | Last successful publication-calendar check | 2d | 7d | Daily check, source-specific releases |
| News | Latest event age | 24h | 48h | Four-hour bounded batches |

Price expectations skip weekends and do not require today's bar before the local
post-close ingestion window. A future enhancement can add an exchange holiday
calendar; until then, holidays should be interpreted through the explicit reason
and expected timestamp rather than silently treated as market days.

Run the evaluator with:

```bash
python -m scripts.check_freshness
```

It reads the latest ingestion manifests, prints `HEALTHY`, `DEGRADED`, `FAILED`, or
`UNKNOWN` with a reason, and writes one current audit per dataset under
`warehouse/metadata/freshness`. It exits nonzero when any dataset is `FAILED`, so it
can gate orchestration or CI without conflating `UNKNOWN` with proven failure.
