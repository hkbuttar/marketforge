# Lightweight time travel

Inspect everything required by an immutable dataset build:

```bash
python -m scripts.reproduce --build-id <64-character-build-id>
```

Add `--catalog` to create `warehouse/duckdb/history/<build-id>.duckdb`. The catalog
contains metadata and read-only-style views over the exact content-hash-verified
Parquet files named by the build. It resolves originals retained by compaction and
checks every dataset row count. No Parquet data is copied.

The plan reports the required Git commit, dirty/clean fingerprint, dbt code hash,
dbt invocation, parameters, pipeline runs, physical input resolution, and whether
each hash remains valid. The catalog reconstructs the historical raw input boundary;
to reproduce transformed outputs exactly, check out the reported Git commit and run
dbt with the manifest parameters against that catalog.
