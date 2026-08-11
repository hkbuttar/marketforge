# Tiingo end-of-day prices

Set the API token in `TIINGO_API_KEY`; it is sent in an authorization header and
is never placed in a URL, output file, or run manifest.

```bash
export TIINGO_API_KEY="..."
python -m scripts.load_tiingo \
  --tickers AAPL,MSFT \
  --start 2021-01-01 \
  --end 2026-08-11
```

The adapter uses Tiingo's raw, unadjusted daily OHLCV fields. Adjusted values and
corporate actions remain available from Tiingo but require an intentional contract
and model change before ingestion. Each source identity is `<TICKER>:<DATE>`, so
overlapping downloads reconcile idempotently.

Tiingo data is licensed for internal use under its account terms. Raw extracts and
Parquet data remain ignored by Git and must not be redistributed from this project.
