# Real optional-domain sources

MarketForge can populate every dbt source without synthetic production records:

| Domain | Provider | Current bounded load | Important limitation |
| --- | --- | ---: | --- |
| Macro | FRED | 1,622 observations across CPI, unemployment, federal funds, GDP, and 10-year Treasury series | Observation endpoint does not provide vintage release timestamps; `released_at` remains null |
| Fundamentals | SEC EDGAR company facts | 841 Apple 10-K/10-Q facts across selected GAAP metrics | SEC facts can repeat comparative periods and amendments; accession identity is retained |
| Earnings | Business Quant analyst estimates | 25 Apple quarterly EPS observations | Estimates endpoint lacks announcement timestamps; `event_timestamp` records observation time, not claimed release time |
| News | NewsAPI | 25 current metadata records | Headline, publisher, timestamp, and URL only; article descriptions and bodies are not retained |

All provider records enter through the same executable contracts, quarantine, reconciliation, immutable Parquet, and manifest path as prices. Keys stay in `.env`; raw files and provider payloads remain ignored.

```bash
set -a; source .env; set +a
python -m scripts.load_providers fred --start 2021-01-01
python -m scripts.load_providers sec --symbol AAPL --cik 320193
python -m scripts.load_providers businessquant --symbol AAPL
python -m scripts.load_providers newsapi --start YYYY-MM-DD --end YYYY-MM-DD
```

The public SEC data API uses the configured identifying user agent and no authentication token. Provider terms still govern local use and any downstream display or redistribution.
