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
