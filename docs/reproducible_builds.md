# Reproducible dataset builds

A dataset build is identified by a SHA-256 digest over:

- the Git commit and clean/dirty worktree fingerprint
- every selected raw Parquet artifact, its content hash, partition, row count, and producing pipeline run
- the dbt project/model/macro content hash
- the latest dbt invocation ID, when available
- explicit dataset selection and build parameters

Create a price build manifest:

```bash
python -m scripts.build_dataset \
  --dataset prices \
  --parameter universe=tiingo-49
```

Verify it later:

```bash
python -m scripts.build_dataset \
  --verify warehouse/metadata/dataset_builds/<build-id>.json
```

Verification fails if the Git state or dbt code changes, a source file disappears,
a Parquet content hash changes, or the build identifier was altered. Repeating an
unchanged build returns the same identifier and existing manifest; `created_at` is
not part of the identity.

Build manifests are local operational evidence and ignored by Git because they can
enumerate licensed source artifacts. The generator, schema, and verification tests
remain version controlled.
