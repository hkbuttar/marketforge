# Property and invariant testing

Hypothesis generates bounded synthetic records and dependency states to exercise
platform guarantees rather than only example outputs:

- replaying identical input leaves one canonical security/date/source row
- every fetched record is accepted, rejected, or deduplicated
- failed durable writes never advance checkpoints or expose canonical Parquet
- every price-contract row is classified exactly once
- any missing required mart prevents backend readiness

Expensive filesystem properties use 8–12 examples; pure arithmetic and contract
properties use 20–30. Deadlines are disabled only where temporary DuckDB/SQLite
setup makes wall time machine-dependent. No generated case contacts a provider.

```bash
python -m unittest discover -s tests/property -v
```

Hypothesis prints a minimized counterexample and replay seed when an invariant
fails, making failures reproducible locally and in CI.
