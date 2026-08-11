# Job benchmarks

Run major ingestion, transformation, compaction, and serving-materialization jobs
through `scripts/benchmark.py`. Each invocation emits one JSON object and appends
it to `benchmarks/results.jsonl`.

Recorded metrics:

- `cpu`: logical core count, user/system CPU seconds, and process CPU utilization
- `peak_ram_bytes`: maximum resident set size reported for the child process tree
- `wall_clock_seconds`: monotonic elapsed time around the command
- `input_bytes` / `output_bytes`: recursive sizes of the paths supplied on the CLI
- `rows_processed`: supplied by the job or its caller with `--rows`

Input size is measured before execution. Output size is measured before and after;
`output_bytes` is the non-negative growth, while `output_total_bytes` preserves the
final on-disk size. Peak RSS follows the host OS's `getrusage` semantics and is best
used for comparisons on the same machine. Failed jobs are still recorded.

Do not commit generated results; the JSONL file is ignored by Git.

## Storage encoding benchmark

`python -m benchmarks.storage_efficiency` rewrites the representative price lake
into temporary CSV, uncompressed Parquet, Snappy Parquet, and ZSTD Parquet files.
It measures file size, write time, full-read time, and grouped-aggregation time;
temporary copies are removed automatically.

Measured on the 68,897-row, 207-file local price lake (five median-timed reads):

| Encoding | Size | vs CSV | Write | Full read | Aggregation |
| --- | ---: | ---: | ---: | ---: | ---: |
| CSV | 7,113,755 B | baseline | 34.510 ms | 165.925 ms | 61.569 ms |
| Parquet uncompressed | 4,125,732 B | 42.00% smaller | 36.007 ms | 98.931 ms | 1.177 ms |
| Parquet Snappy | 2,061,355 B | 71.02% smaller | 38.726 ms | 103.302 ms | 1.751 ms |
| Parquet ZSTD | 1,276,144 B | 82.06% smaller | 46.657 ms | 102.062 ms | 2.080 ms |

ZSTD remains the production choice: it saves another 38% relative to Snappy and
82% relative to CSV, while adding only about 8 ms to this representative write.
Analytical reads remain columnar and roughly equivalent to the other Parquet
encodings; aggregation is about 30 times faster than CSV in this run. Timings are
machine-specific and should be remeasured after major hardware or dataset changes.

## Partition layout benchmark

`python -m benchmarks.partition_layout --iterations 3` compares one file,
year/month, and year/month/symbol layouts across one-day cross sections,
single-symbol months, one-year scans, and full-history aggregations. Temporary
layouts are removed automatically. The measured decision and results are recorded
in `docs/historical_backfill.md`.

## Incremental versus full refresh

`python -m benchmarks.incremental_vs_full` constructs isolated full-refresh and
incremental lakes from the same retained Tiingo rows. Setup of the prior
incremental state is excluded from the timed daily operation. A deterministic hash
over canonical row values proves both paths produce the same result.

Measured on the 49-symbol price universe through 2026-08-10:

| Metric | Full refresh | Daily incremental |
| --- | ---: | ---: |
| Rows processed | 68,894 | 49 |
| Input bytes | 14,484,982 | 10,328 |
| Output bytes | 1,470,642 | 8,853 |
| Files written | 68 | 1 |
| Wall time | 35.024 s | 0.231 s |
| Peak RAM | 302.9 MB | 156.2 MB |

The incremental path processed **0.0711%** of historical rows, completed **151.77×
faster**, and wrote **99.40% fewer bytes**. This validates incremental ingestion as
the normal production path; full refresh remains a reproducibility and recovery
operation. Results are specific to this machine and current dataset and should be
remeasured as the universe grows.
