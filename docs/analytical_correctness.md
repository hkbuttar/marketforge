# Analytical correctness validation

The integration fixture independently fixes the expected values for six served
analytics and compares them to dbt outputs. AAPL has 21 closes from 100 through
120, creating exactly 20 return observations. Its final volume is 200 after 19
window observations of 100. MSFT falls from 200 to 190 and XOM remains at 50 on
the final date.

The expected JSON records the hand-calculated daily and 20-day returns, sample
volatility and annualized volatility, equal-weight technology-sector return,
market breadth, and relative volume. The test uses 12-decimal-place comparisons,
so changes to window bounds, sample/population volatility, annualization, null
handling, or aggregation grain fail visibly in CI.

Run only this validation with:

```bash
python -m unittest tests.integration.test_analytical_correctness -v
```
