# Resource guardrails

`config/resource_budget.yaml` defines warning, hard project, raw-data, and minimum
free-disk thresholds. The backfill CLI conservatively estimates its write from the
selected records before contract validation or file creation. Warning-level jobs
continue with a visible warning; hard-limit violations abort before raw writes.

Preview recoverable cleanup candidates:

```bash
python -m scripts.cleanup
```

Apply the exact previewed policy with `--apply`. Cleanup is restricted to expired
files under `data/raw/.tmp`, `data/staging`, `data/quarantine`, and
`benchmarks/artifacts`. It skips symlinks and never selects canonical raw dataset
partitions. The default 14-day retention can be overridden with
`--older-than-days`, but cannot be less than one day.
