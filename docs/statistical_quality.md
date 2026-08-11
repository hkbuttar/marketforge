# Statistical data-quality monitoring

`python -m scripts.check_quality` profiles the latest Tiingo price date and compares
it with up to 60 prior observations. It records daily row count, null rate, median
volume, median price, zero-volume fraction, cross-sectional return dispersion, and
expected-symbol coverage.

Row count, medians, and return dispersion use median/MAD robust z-scores. Null,
zero-volume, and missing-symbol fractions use explicit absolute thresholds. The
defaults live in `config/anomaly_thresholds.json`; they are deliberately simple and
reviewable rather than a heavyweight anomaly model.

Results are `HEALTHY`, `DEGRADED`, `FAILED`, or `UNKNOWN`, always with a reason.
At least 20 historical days are required before distribution drift can be judged.
The current result and all findings are persisted to
`warehouse/metadata/quality/prices-<source>.json`.

The expected universe is read from `config/price_universe.txt`. Add a symbol only
after its historical load succeeds; otherwise the next quality run correctly
reports it as missing.
