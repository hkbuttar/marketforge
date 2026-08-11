# Failure injection

MarketForge exercises failures at provider, contract, storage, checkpoint, and
transformation boundaries. All scenarios use temporary storage and deterministic
hooks; no production files are damaged to prove safety.

| Injected failure | Expected behavior | Evidence |
| --- | --- | --- |
| Tiingo HTTP 503 | Three total attempts, exponential bounded backoff, visible failure after exhaustion | `test_503_retries_with_bounded_backoff_then_preserves_prior_data` |
| Tiingo HTTP 429 | Honor bounded `Retry-After`, then ingest once; replay deduplicates | `test_429_respects_retry_after_and_replay_is_not_duplicated` |
| Malformed provider value | Contract quarantine, degraded manifest, no canonical row | `test_malformed_value_is_quarantined_and_canonical_state_unchanged` |
| Additive/breaking schema drift | Quarantine or hard failure according to versioned policy | `test_source_contracts`, `test_backfill` |
| Disk full before temporary write | Abort with `ENOSPC`; prior partition and manifest remain intact | `test_disk_full_before_temp_write_preserves_existing_partition` |
| Kill during write/validation/promotion | `.writing` files are noncanonical; retry safely stages or deduplicates | `test_atomic_writes` |
| Kill after promotion before checkpoint | Checkpoint remains old; replay recognizes the promoted key and advances once | `test_promotion_before_checkpoint_recovers_via_replay` |
| dbt build failure | Raw manifest and Parquet stay valid; no mart is published by the failed step | `test_dbt_failure_does_not_invalidate_raw_or_publish_mart` |

Provider retry delays default to 0.25 and 0.5 seconds, capped at two seconds, with
two retries after the initial request. HTTP 429 `Retry-After` is honored only up to
the same cap so a local run cannot sleep indefinitely. Authentication errors and
malformed JSON are not retried because repetition cannot correct them.

These tests establish the failure-side guarantees. Step 27 adds explicit recovery
state narratives and end-to-end restart assertions for every injected class.
