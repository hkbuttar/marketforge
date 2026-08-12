# Results and honest comparison

Measurements were produced on the local CPU-only machine and the retained
140,703-row price lake. They are reproducible observations,
not concurrency, cloud-scale, or cross-machine claims.

Numeric sources: `benchmarks/results/latest.json`, `partitions.json`, and
`serving.json`. Reliability and quality observations are enforced by the failure
and synthetic-defect test suites.

## Storage

| Format | Bytes | Write ms | Aggregation median ms |
| --- | ---: | ---: | ---: |
| CSV | 14428354 | 56.638 | 65.175 |
| Parquet Uncompressed | 8400475 | 65.268 | 1.819 |
| Parquet Snappy | 4180033 | 69.32 | 2.744 |
| Parquet Zstd | 2567471 | 86.405 | 3.249 |

ZSTD uses **82.21% less space than CSV**,
but writes more slowly than CSV and aggregates slightly more slowly than Snappy.
That CPU cost is accepted because disk is the explicit constraint.

## Partition strategy

| Layout | Files | Bytes | Month/symbol ms | Full aggregate ms |
| --- | ---: | ---: | ---: | ---: |
| single/file | 1 | 2568494 | 1.448 | 2.931 |
| year/month | 68 | 2799170 | 17.047 | 19.29 |
| year/month/symbol | 6800 | 15369352 | 1626.944 | 1820.128 |

The single file wins pure query latency at this scale, but cannot support immutable
incremental appends. Year/month is the operational compromise. Year/month/symbol
is rejected: its small-file discovery and storage overhead are dramatically worse.

## Processing

| Path | Runtime s | Peak RAM bytes | Bytes read | Bytes written |
| --- | ---: | ---: | ---: | ---: |
| Full refresh | 70.540453 | 489897984 | 29482823 | 2799772 |
| Daily incremental | 0.43817 | 241303552 | 21000 | 6025 |

The incremental path is **160.99× faster**
and writes **99.78% fewer bytes**,
with canonical equivalence verified before reporting.

## Compaction

| State | Files | Bytes | Count-query median ms |
| --- | ---: | ---: | ---: |
| Before | 11 | 64753 | 0.853 |
| After | 1 | 50688 | 0.193 |

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
| /api/securities?limit=100 | 5.371 | 6.597 | 0.756 | 0.822 |
| /api/securities/AAPL | 5.935 | 7.174 | 0.564 | 0.747 |
| /api/securities/AAPL/history?source=tiingo&limit=252 | 8.082 | 10.363 | 1.592 | 1.772 |
| /api/pipeline/health | 5.369 | 7.599 | 0.478 | 0.561 |
| /api/datasets | 4.845 | 5.211 | 0.491 | 0.587 |
| /api/datasets/prices/lineage | 0.899 | 2.377 | 0.502 | 0.54 |

Serving results are sequential in-process FastAPI measurements over marts derived
from retained Tiingo prices. They do not establish throughput under concurrency.

## Where sophistication did not help

- A single Parquet file is faster, but loses append-only operational behavior.
- Year/month/symbol partitioning performs far worse than monthly partitions here.
- The in-process cache lowers warm endpoint latency; no result demonstrates a need for Redis.
- SQLite satisfies the tested transactional metadata requirements; Postgres was not benchmarked.
- Kafka remains optional: HTTP polling is simpler when gap-free delivery is not required.
- ZSTD's extra write CPU is measurable; it is retained only because its disk savings matter.
