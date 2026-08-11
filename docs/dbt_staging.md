# dbt staging layer

The dbt project transforms source-neutral raw Parquet views into five staging
views: `stg_prices`, `stg_fundamentals`, `stg_macro`, `stg_earnings`, and
`stg_news`. Models query Parquet through DuckDB and do not duplicate raw storage.

Each model is intentionally limited to staging concerns:

- consistent names and canonical DuckDB types;
- uppercase symbols/series codes and lowercase source names;
- logical-grain deduplication with deterministic ordering;
- obvious contract-equivalent business filters;
- stable MD5 surrogate keys from declared idempotency fields.

The surrogate keys identify observations, not securities. A source symbol must
still resolve through the canonical security master before downstream facts use a
durable `security_id`. Returns, rolling features, alignments, and other analytical
logic belong in intermediate models beginning in Step 10.

dbt currently supports the project on Python 3.11–3.13. Install and run from the
repository root:

```bash
python3.13 -m venv .venv
.venv/bin/pip install -e '.[transformation]'
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt
```

`create_raw_views` runs first and fails visibly if any expected initial dataset is
missing. Override the lake location for tests or alternate environments with:

```bash
.venv/bin/dbt build --project-dir dbt --profiles-dir dbt \
  --vars '{raw_root: /absolute/path/to/data/raw}'
```

The default profile caps DuckDB at 4 GB RAM and two threads, consistent with the
local resource budget. Staging key uniqueness and non-null invariants run as part
of `dbt build`; broader data-quality coverage remains Step 12.
