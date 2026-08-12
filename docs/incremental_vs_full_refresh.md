# Incremental versus full refresh

The benchmark compares equivalent outcomes, not merely two unrelated commands.
The full path rebuilds all retained Tiingo price history into an empty lake. The
incremental path starts from history through the prior market date and processes
only the latest 100-symbol batch. A deterministic canonical row hash, row count,
volume total, and date range must match before results are reported.

```bash
python -m benchmarks.incremental_vs_full
```

On the current 140,700-row Tiingo dataset, the daily increment represented 0.0711%
of history and took 0.438 seconds versus 70.540 seconds for the full rebuild—a
160.99× speedup. It wrote 6,025 bytes instead of 2,799,772 bytes and used roughly
241 MB peak RAM instead of 490 MB.

Baseline construction is intentionally outside the timed incremental operation:
that state is the durable result of prior successful daily runs. Input extraction
into equivalent JSONL payloads is also setup; each timed worker includes parsing
its own payload, validation, reconciliation, Parquet writing, manifest creation,
and checkpoint advancement where applicable.
