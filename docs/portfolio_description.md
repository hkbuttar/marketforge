# Portfolio description

## Repository tagline

Local-first analytical data platform demonstrating reliable incremental
ingestion, immutable Parquet storage, DuckDB/dbt analytics, orchestration,
observability, and failure recovery on CPU-only hardware.

## One-sentence thesis

MarketForge tests how faithfully production analytical-engineering behavior can
be reproduced on one CPU-only laptop without adding distributed infrastructure
that the measured workload does not need.

## Short project card

MarketForge is a local-first analytical data platform built around systems
engineering rather than price prediction. It incrementally loads a 100-symbol
Tiingo universe into immutable, partitioned Parquet; queries it with DuckDB;
builds tested analytical marts with dbt; orchestrates asset dependencies with
Dagster; and exposes health, lineage, quarantine, benchmarks, and governed
analytics through FastAPI and React. Failure injection verifies replay-safe
recovery, checkpoint consistency, atomic writes, and schema isolation. On the
140,700-row Tiingo history, daily incremental processing was 160.99× faster than a
full rebuild and wrote 99.78% fewer bytes. The deterministic suite passes 127
tests across 19 validation categories.

## Full portfolio overview

MarketForge is a local-first analytical data platform built to answer a systems
question rather than a modeling one: how much of a production lakehouse's
reliability, reproducibility, observability, and incremental-processing behavior
can be implemented faithfully on a CPU-only laptop without introducing
distributed infrastructure purely for appearance?

The platform ingests real Tiingo end-of-day prices for 100 symbols and provides
contract-tested paths for fundamental, earnings, macroeconomic, news, and
StreamAlpha anomaly events. Every record crosses an explicit validation boundary
before immutable ZSTD Parquet storage. DuckDB queries the lake directly, dbt
creates reusable staging, intermediate, and mart layers, and Dagster models
dependencies and quality gates. SQLite retains transactional run/checkpoint
metadata; FastAPI and React expose bounded catalog, health, lineage, quality,
storage, benchmark, and analytical views.

The laptop constraint is an engineering requirement rather than something to
hide. Storage format, compression codec, partition granularity, compaction,
incremental processing, query serving, and Docker overhead are benchmarked.
Measurements rejected year/month/symbol partitioning because 3,332 small files
made representative queries dramatically slower, while ZSTD reduced the
representative dataset by 82.04% versus CSV. Daily incremental processing handled
100 rows in 0.438 seconds versus 140,700 rows in 70.540 seconds for a canonically
equivalent rebuild.

Reliability is demonstrated through executable failure cases, not architecture
diagrams alone. Tests inject provider outages and rate limits, malformed schemas,
bad values, disk exhaustion, process termination at write boundaries, dbt
failures, late data, duplicate delivery, and compaction rollback. The accurate
claim is an idempotent canonical result under replay—not universal exactly-once
delivery. The deterministic suite passes 127 tests across all 19 required
validation categories with no failures or skips.

MarketForge also makes its limits explicit. It is not a distributed cluster,
DuckDB is not presented as a multi-user cloud warehouse, local filesystem
durability is not object-store durability, and the hosted demo serves a generated
90-row snapshot rather than the full local pipeline. Prices have full-universe
production coverage; FRED, SEC EDGAR, Business Quant, and NewsAPI production
loads are deliberately bounded and retain explicit limitations.

## Résumé bullets

- Designed and built a CPU-only analytical data platform using Python, Parquet,
  DuckDB, dbt, Dagster, SQLite, FastAPI, and React, with immutable raw storage,
  checkpointed ingestion, lineage, quarantine, and reproducible dataset builds.
- Implemented atomic staged writes, monotonic checkpoints, overlap-safe late-data
  handling, canonical replay deduplication, reconciliation invariants, and
  failure-recovery drills for provider, disk, process, transformation, and
  compaction failures.
- Benchmarked equivalent full and incremental builds over 140,700 Tiingo rows;
  reduced daily runtime from 70.540 seconds to 0.438 seconds (160.99×) and bytes
  written by 99.78%, with canonical hashes required to match.
- Evaluated CSV and three Parquet encodings plus three partition layouts; selected
  ZSTD/year-month based on an 82.04% storage reduction versus CSV and rejected a
  3,332-file symbol layout after measuring severe discovery overhead.
- Built a bounded FastAPI/React control plane for dataset catalog, run history,
  freshness, quality, quarantine, lineage, storage, benchmarks, and analytics;
  validated the platform with 127 deterministic tests across 19 categories.

## Interview talking points

### The central tradeoff

The fastest query layout was one Parquet file, but immutable incremental appends
made it operationally inappropriate. Monthly partitions cost roughly 15 ms of
discovery latency at this scale but bound writes and preserve history. This is the
kind of measured compromise the project is intended to show.

### The hardest reliability boundary

A process can die after a canonical file is promoted but before its checkpoint is
advanced. MarketForge deliberately allows redelivery: retained logical identities
turn the retry into a no-op, after which the checkpoint can advance. It avoids an
unearned exactly-once claim.

### Why no Spark, Kubernetes, Redis, or Postgres?

None solves a demonstrated bottleneck in the current workload. DuckDB handles the
analytical scans, an in-process cache materially lowers warm API latency, SQLite
satisfies tested metadata concurrency, and one optional 66.62 MiB demo API image
is enough for deployment reproducibility. Future complexity is conditional on a
measurement that justifies it.

### What is real versus illustrative?

The retained local lake contains 140,700 real Tiingo rows for 100 symbols plus a
three-row synthetic price smoke fixture. The optional StreamAlpha bridge has 500
retained events. Fundamental, earnings, macro, and news domains have full
contracts, dbt models, and deterministic fixtures but no production provider load.
The hosted dashboard uses a separately generated 90-row read-only snapshot.

## Claims to avoid

- Do not call the project a distributed lakehouse or production cloud platform.
- Do not claim end-to-end exactly-once delivery.
- Do not imply all five batch domains contain production data.
- Do not generalize local sequential API latency into concurrency throughput.
- Do not call lightweight manifests equivalent to Iceberg or Delta transaction logs.
- Do not imply universal point-in-time correctness when providers omit knowledge timestamps.

## Suggested links

- [Rigorous README](../README.md)
- [Measured results](results.md)
- [End-to-end demonstration](end_to_end_demo.md)
- [Testing evidence](testing_validation.md)
- [Deployment boundary](deployment.md)
- [Project differentiation](project_differentiation.md)
