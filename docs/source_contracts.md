# Source contracts

Contracts in `ingestion/contracts` are the boundary between untrusted provider
payloads and canonical ingestion. They declare exact columns, normalizers and
types, nullability, batch uniqueness, value rules, aliases, and source metadata.
Every contract has an explicit integer `version` and `unknown_field_policy`.
The current v1 policy quarantines additive unknown fields until the contract,
canonical schema, staging model, and tests are deliberately upgraded together.

Call `contract.validate(...)` before resolving a symbol or writing raw canonical
data. The result separates `accepted` normalized rows from `rejected` structured
quarantine records. Persist rejected records with `write_quarantine`; it uses
exclusive file creation so retrying a run cannot overwrite the first diagnostic.

```python
from ingestion.contracts import PRICES_CONTRACT
from ingestion.contracts.base import write_quarantine

result = PRICES_CONTRACT.validate(
    provider_rows,
    source="provider-name",
    ingestion_run_id="run-uuid",
)
write_quarantine(result.rejected)
```

Quarantine artifacts use
`data/quarantine/source=<source>/run=<ingestion_run_id>.jsonl`. Each line contains
`source`, `ingestion_run_id`, `error_type`, `error_message`, the untouched
`raw_payload`, and UTC `received_at`. Quarantined data is diagnostic input only;
it must be corrected and pass the contract before entering canonical storage.

## Evolution behavior

- An additive provider field such as `adjusted_close` is quarantined under the v1
  strict policy. It is not silently discarded.
- A breaking value such as `volume="1.23M"` fails normalization, is quarantined,
  marks the ingestion manifest `degraded`, and writes no canonical row.
- If a required column such as `symbol` is absent from the entire batch, validation
  raises `MissingRequiredFieldError`. No raw artifact or successful manifest is
  produced, making a provider-wide breaking schema change a hard pipeline failure.
- Manifests record `contract_version`, allowing retained artifacts and incidents to
  be traced to the validation boundary in force at processing time.

An individual malformed record missing a field is rejected normally when other
records prove the column still exists in the provider batch. The hard-failure rule
is reserved for a column disappearing from the batch schema altogether.
