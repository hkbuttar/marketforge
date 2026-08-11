# Project differentiation

MarketForge is the durable analytical infrastructure in a broader portfolio
ecosystem. It should not absorb the identities of streaming detection systems or
financial-research applications merely because it can exchange data with them.

The descriptions of adjacent projects below are positioning boundaries from the
project plan, not independent audits of their current repositories or guarantees.

## Responsibility map

| Dimension | StreamAlpha | MarketForge | IndexEdge / Alpha Signal Lab |
| --- | --- | --- | --- |
| Primary question | What is happening in the stream now? | Can analytical data be retained, trusted, rebuilt, and served reliably? | What does the data imply for factors, portfolios, or research? |
| Temporal mode | Real-time, event-by-event | Historical plus incremental batch; optional stream micro-batches | Research windows, experiments, and portfolio periods |
| Core work unit | Event/anomaly | Dataset partition, model, run, and version | Factor observation, signal, portfolio, and experiment |
| Main state | Kafka/event-processing state | Immutable Parquet, checkpoints, manifests, marts, metadata | Research datasets, model outputs, backtests, portfolio results |
| Central engineering | Online detection and stream recovery | Contracts, backfills, schema evolution, dbt, lineage, quality, reproducibility | Factor construction, financial analysis, evaluation, interpretation |
| Primary output | Detected event | Governed analytical data product and platform evidence | Research result or portfolio decision support |
| Intended consumers | Event-driven services and operators | APIs, dashboards, research systems, and data operators | Researchers, analysts, or portfolio workflows |

## StreamAlpha boundary

```text
StreamAlpha                           MarketForge
market/event stream                   retained analytical history
        │                                      │
online anomaly detection                       │
        │                                      │
stable anomaly envelope ───────────────► contract adapter
                                               │
                                      immutable event Parquet
                                               │
                                      point-in-time context join
                                               │
                                      mart_intraday_anomalies
```

StreamAlpha determines that an anomaly exists. MarketForge does not reproduce its
online detector; it retains selected events, deduplicates delivery, quarantines
bad envelopes, and enriches them with information knowable before the event:
prior-session return, rolling volatility, relative volume, earlier earnings, and
recent market-relative behavior.

The HTTP integration is replay-safe but not gap-free because the public endpoint
has a limit and no cursor. Kafka is optional when stricter continuous delivery is
required. MarketForge's accurate claim is durable processing with an idempotent
canonical result under replay—not ownership of StreamAlpha's real-time or
exactly-once design claims.

## Research-system boundary

```text
MarketForge governed marts
  prices / returns / volatility / macro / events
                         │
                         ▼
          IndexEdge / Alpha Signal Lab
         factors / experiments / portfolios
                         │
                         ▼
                research conclusions
```

MarketForge may calculate reusable, contract-tested analytical features such as
daily returns or rolling volatility because they are shared data products. It
does not own investment theses, alpha claims, portfolio optimization, strategy
selection, or backtest interpretation. Those belong in research applications
that consume versioned MarketForge outputs.

This separation prevents research-specific assumptions from entering canonical
ingestion and prevents infrastructure work from being presented as investment
performance.

## What is distinctive about MarketForge

1. **Reliability is executable.** Provider failures, disk exhaustion, process
   termination, malformed schemas, replay, late data, dbt failure, and compaction
   rollback are injected and asserted.
2. **Reproducibility is an artifact.** Dataset builds pin content-hashed source
   partitions, and historical catalogs refuse changed inputs.
3. **The laptop constraint is measured.** Encoding, partitioning, full versus
   incremental processing, compaction, query serving, caching, and Docker
   overhead are benchmarked instead of assumed.
4. **Operational and analytical time are separate.** Event time, knowledge time,
   ingestion time, and checkpoint time remain explicit where sources provide the
   necessary evidence.
5. **Complexity must earn its place.** Spark, Kubernetes, Redis, Postgres, Kafka,
   and full-platform containers are not added unless a measured requirement
   justifies them.
6. **The UI is a control plane.** Health, lineage, run history, quarantine,
   quality, storage, and benchmarks dominate; market charts are consumers of the
   platform rather than its identity.

## Non-goals

MarketForge is not:

- an online anomaly detector;
- a trading strategy or stock recommendation engine;
- a portfolio optimizer or backtesting product;
- a claim of distributed-scale processing;
- a replacement for StreamAlpha, IndexEdge, or Alpha Signal Lab;
- evidence of investment performance;
- a universal exactly-once or point-in-time-correct system.

## Portfolio positioning

Use these concise distinctions when presenting the projects together:

- **StreamAlpha detects live market events.**
- **MarketForge turns source and event data into durable, tested analytical data products.**
- **IndexEdge / Alpha Signal Lab use governed data to conduct financial research.**

Together they can demonstrate event processing, analytical infrastructure, and
research consumption without pretending one repository owns every layer.
