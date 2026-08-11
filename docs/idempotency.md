# Idempotency guarantees

Idempotency is defined at the logical record grain, not by output filename or run
identifier. Each executable contract declares `idempotency_by`:

| Dataset | Idempotency key | Rationale |
| --- | --- | --- |
| Prices | (`symbol`, `date`, `source`) | One source bar per symbol and trading date |
| Fundamentals | (`source`, `source_record_id`) | Providers may publish multiple filing versions |
| Earnings | (`source`, `source_record_id`) | Event identity comes from the provider |
| Macro | (`source`, `source_record_id`) | Preserves provider vintages and revisions |
| News | (`source`, `source_record_id`) | Event identity comes from the provider |

Before a write, the loader reads retained idempotency keys and canonical values.
An identical key/value pair is counted as a replay and writes nothing. The same
key with different canonical values raises `IdempotencyConflictError` before a new
Parquet file or success manifest is created; a revision policy must resolve it
explicitly rather than silently selecting a value.

If a provider supplies no ID, the loader hashes the stable JSON representation of
the received payload with SHA-256 before adding ingestion metadata. This is stable
for byte-equivalent logical values and key ordering, but representation changes
such as `1` versus `1.0`, provider-added fields, or corrected values can produce a
new identity. Source adapters should prefer a documented provider identifier, and
event datasets without one require provider-specific stable-field selection before
production use.

Run the executable proof:

```bash
.venv/bin/python scripts/prove_idempotency.py
```

It ingests the same dataset twice and verifies unchanged logical row count, zero
duplicate natural keys, an identical canonical-value checksum, and explicit replay
recognition on the second run. Failure tests also prove that conflicting values do
not create a second artifact.
