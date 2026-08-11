# dbt intermediate models

Intermediate models centralize reusable calculations so marts do not independently
reimplement them:

```text
stg_prices ──> int_daily_returns ──> int_rolling_volatility
stg_fundamentals ──> int_security_fundamentals
stg_earnings ──> int_earnings_surprises
stg_prices + stg_macro ──> int_macro_aligned
```

- `int_daily_returns` computes zero-safe close-to-close returns per symbol/source.
- `int_rolling_volatility` adds trailing 20-observation return, sample volatility,
  annualized volatility, and the actual observation count.
- `int_security_fundamentals` creates half-open effective ranges using filing time
  when available. This supports point-in-time joins without using future filings.
- `int_earnings_surprises` calculates absolute and percentage EPS surprise while
  guarding zero or near-zero estimates.
- `int_macro_aligned` performs an ASOF join from observed trading dates to the most
  recent macro value that was available. Its availability date is never earlier
  than both the observation date and release date, preventing look-ahead leakage.

These are views because the bounded local workload is small and the logic remains
cheap. Materialization can be changed after measurement; marts should depend on
these models rather than copy their window or alignment logic.

Sector membership is intentionally not fabricated from ticker symbols. It will be
added when a populated, effective-dated canonical security classification source
exists; the current schema already reserves durable security, sector, and industry
identities.
