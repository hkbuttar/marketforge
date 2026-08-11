# dbt data-quality gates

`dbt build` is the transformation quality gate. It builds models in dependency
order and runs built-in, generic, and singular tests before a caller may expose
the refreshed marts.

Coverage includes:

- `not_null` and `unique` stable observation keys;
- `relationships` from intermediate observations back to staging and mart rows
  back to intermediate models;
- accepted macro frequencies, earnings statuses, and health statuses;
- composite natural-key uniqueness;
- OHLC ordering and non-negative volume;
- no future trading dates;
- a configurable sanity bound of ±500% for close-to-close daily returns;
- valid half-open fundamental effective ranges;
- no macro values aligned before availability;
- reconciliation of mart and intermediate row counts;
- presence of all five datasets in the health mart;
- source freshness thresholds appropriate to each dataset cadence.

Run the complete gate from the repository root:

```bash
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt
.venv/bin/dbt source freshness --project-dir dbt --profiles-dir dbt
```

A pipeline is publishable only when ingestion returned success, source contracts
accepted or quarantined records as configured, `dbt build` exited successfully,
and source freshness did not exceed its error threshold. Dagster will encode this
ordering in Step 13; a `healthy` row in the mart alone is not authorization to
publish if the dbt command failed.

Canonical `security_id` relationship testing remains deliberately unavailable:
the project has not yet selected and loaded an authoritative security-master feed.
Current tests prove source-symbol snapshot rows resolve to daily source-symbol
groups. Once canonical resolution is populated, a failing `relationships` test to
the security master is a required replacement—not an optional enhancement.
