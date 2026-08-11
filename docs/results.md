# Results and honest comparison

Measurements were produced on the local CPU-only machine and the retained
68,897-row price lake. They are reproducible observations,
not concurrency, cloud-scale, or cross-machine claims.

Numeric sources: `benchmarks/results/latest.json`, `partitions.json`, and
`serving.json`. Reliability and quality observations are enforced by the failure
and synthetic-defect test suites.

## Storage

| Format | Bytes | Write ms | Aggregation median ms |
| --- | ---: | ---: | ---: |
| CSV | 7113755 | 32.625 | 61.17 |
| Parquet Uncompressed | 4126110 | 38.25 | 1.239 |
| Parquet Snappy | 2061858 | 41.803 | 1.703 |
| Parquet Zstd | 1277601 | 45.528 | 2.026 |

ZSTD uses **82.04% less space than CSV**,
but writes more slowly than CSV and aggregates slightly more slowly than Snappy.
That CPU cost is accepted because disk is the explicit constraint.

## Partition strategy

| Layout | Files | Bytes | Month/symbol ms | Full aggregate ms |
| --- | ---: | ---: | ---: | ---: |
| single/file | 1 | 1278225 | 3.554 | 1.832 |
| year/month | 68 | 1471117 | 15.917 | 18.301 |
| year/month/symbol | 3332 | 7538154 | 791.508 | 845.313 |

The single file wins pure query latency at this scale, but cannot support immutable
incremental appends. Year/month is the operational compromise. Year/month/symbol
is rejected: its small-file discovery and storage overhead are dramatically worse.

## Processing

| Path | Runtime s | Peak RAM bytes | Bytes read | Bytes written |
| --- | ---: | ---: | ---: | ---: |
| Full refresh | 34.474819 | 275136512 | 14484982 | 1471524 |
| Daily incremental | 0.225584 | 155107328 | 10328 | 8862 |

The incremental path is **152.82× faster**
and writes **99.40% fewer bytes**,
with canonical equivalence verified before reporting.

## Compaction

| State | Files | Bytes | Count-query median ms |
| --- | ---: | ---: | ---: |
| Before | 4 | 27622 | 0.692 |
| After | 1 | 25914 | 0.273 |

## Reliability

| Failure | Expected | Observed | Data loss? | Duplicates? | Automatic recovery? |
| --- | ---: | ---: | ---: | ---: | ---: |
| Provider HTTP 503 | Retry bounded request | No row written; retry accepted once | No | No | No—retry required |
| Disk full before write | Leave no canonical fragment | No canonical file; same run retried | No | No | No—retry required |
| Crash after promotion | Replay without duplicate; advance checkpoint | Replay deduplicated; checkpoint advanced | No | No | No—restart required |
| dbt failure | Raw remains queryable; retry transform | Raw retained; retry succeeded | No | No | No—retry required |

## Quality

| Injected defect | Detected? | Quarantined/action | Downstream contamination? |
| --- | ---: | ---: | ---: |
| Duplicate row | Yes | Yes | No |
| Missing required column | Yes | Batch rejected | No |
| Negative volume | Yes | Yes | No |
| High below low | Yes | Yes | No |
| Wrong date type | Yes | Yes | No |
| Unknown security | Yes | Quality gate | No |
| Added schema column | Yes | Yes | No |
| Nonnumeric numeric field | Yes | Yes | No |
| Empty API response | Yes | No payload | No; checkpoint unchanged |

## Serving

| Endpoint | Cold median ms | Cold p95 ms | Warm median ms | Warm p95 ms |
| --- | ---: | ---: | ---: | ---: |
| /api/securities?limit=100 | 5.14 | 5.613 | 0.62 | 0.706 |
| /api/securities/AAPL | 5.238 | 5.56 | 0.511 | 0.551 |
| /api/securities/AAPL/history?source=tiingo&limit=252 | 6.817 | 8.283 | 1.371 | 1.519 |
| /api/pipeline/health | 4.698 | 4.922 | 0.474 | 0.517 |
| /api/datasets | 5.355 | 6.567 | 0.506 | 0.549 |
| /api/datasets/prices/lineage | 0.751 | 1.875 | 0.539 | 0.585 |

Serving results are sequential in-process FastAPI measurements over marts derived
from retained Tiingo prices. They do not establish throughput under concurrency.

## Where sophistication did not help

- A single Parquet file is faster, but loses append-only operational behavior.
- Year/month/symbol partitioning performs far worse than monthly partitions here.
- The in-process cache lowers warm endpoint latency; no result demonstrates a need for Redis.
- SQLite satisfies the tested transactional metadata requirements; Postgres was not benchmarked.
- Kafka remains optional: HTTP polling is simpler when gap-free delivery is not required.
- ZSTD's extra write CPU is measurable; it is retained only because its disk savings matter.
