# Source contracts

Contracts in `ingestion/contracts` are the boundary between untrusted provider
payloads and canonical ingestion. They declare exact columns, normalizers and
types, nullability, batch uniqueness, value rules, aliases, and source metadata.
Schema drift (missing or unexpected columns) rejects the entire affected row.

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
