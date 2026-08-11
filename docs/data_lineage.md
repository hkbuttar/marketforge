# Data lineage

MarketForge exports asset-level lineage from dbt's canonical `manifest.json` and
enriches each raw source with local dataset-build metadata, immutable Parquet
location, ingestion run IDs, and providers found in the retained raw records.

Generate the complete graph after dbt parsing or a build:

```bash
python -m scripts.build_lineage
```

Trace one model and all of its ancestors:

```bash
python -m scripts.build_lineage --target mart_security_daily
```

The durable graph is written to `warehouse/metadata/lineage.json`. For the daily
security mart, the asset-level path is:

```text
external provider / immutable price Parquet
  -> source.raw.prices
  -> stg_prices
  -> int_daily_returns
  -> int_rolling_volatility
  -> mart_security_daily
```

dbt remains authoritative for model dependencies. MarketForge metadata supplies
the boundary from a dbt source to physical raw artifacts and ingestion runs. This
intentionally avoids maintaining a second SQL parser or claiming column-level
lineage; fields such as `rolling_20d_return` are documented by their owning model
and inherit the model's asset lineage.
