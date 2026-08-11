# Analytics marts

MarketForge currently materializes four consumer-facing DuckDB tables:

- `mart_security_daily`: daily price and reusable return/volatility features,
  20-observation average volume, and relative volume.
- `mart_market_daily`: cross-sectional return statistics, advancers, decliners,
  unchanged securities, and normalized market breadth.
- `mart_company_snapshot`: latest price, fundamental coverage/recency, latest
  earnings result, and price freshness.
- `mart_pipeline_dataset_health`: the central operational product table, combining
  staging row/null/duplicate metrics with latest successful ingestion manifests,
  quarantine counts, event freshness, and an explicit health status.

Marts are tables because they are consumer-facing and should offer predictable
query latency. The raw lake remains Parquet; only transformed outputs are persisted
in the compact DuckDB file.

## Identity and classification limitation

`source_security_key` is explicitly a source-symbol grouping key, not the canonical
durable `security_id`. It is safe for grouping current rows from one provider but
will change if that provider changes a ticker. MarketForge will not label this as a
durable security ID.

For the same reason, `mart_sector_daily` and sector/industry columns are deferred
until an authoritative, effective-dated classification feed populates the canonical
security master. Once available, those marts will join facts through `security_id`,
never directly through ticker text. This preserves the Step 2 identity guarantee.

The health mart reports `unknown` when no success manifest exists, `empty` for an
empty staging model, `degraded` for nulls or duplicate keys, and `healthy` when the
available structural and run checks pass. Dataset-specific freshness service levels
and richer anomaly status are introduced by later quality/observability steps.
