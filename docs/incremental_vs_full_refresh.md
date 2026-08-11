# Incremental versus full refresh

The benchmark compares equivalent outcomes, not merely two unrelated commands.
The full path rebuilds all retained Tiingo price history into an empty lake. The
incremental path starts from history through the prior market date and processes
only the latest 49-symbol batch. A deterministic canonical row hash, row count,
volume total, and date range must match before results are reported.

```bash
python -m benchmarks.incremental_vs_full
```

On the current 68,894-row dataset, the daily increment represented 0.0711% of
history and took 0.231 seconds versus 35.024 seconds for the full rebuild—a
151.77× speedup. It wrote 8,853 bytes instead of 1,470,642 bytes and used roughly
156 MB peak RAM instead of 303 MB.

Baseline construction is intentionally outside the timed incremental operation:
that state is the durable result of prior successful daily runs. Input extraction
into equivalent JSONL payloads is also setup; each timed worker includes parsing
its own payload, validation, reconciliation, Parquet writing, manifest creation,
and checkpoint advancement where applicable.
