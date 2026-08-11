select
    price_bar_key,
    symbol,
    trade_date,
    close,
    volume,
    source,
    daily_return,
    count(daily_return) over trailing_20 as return_observations_20d,
    stddev_samp(daily_return) over trailing_20 as rolling_20d_volatility,
    stddev_samp(daily_return) over trailing_20 * sqrt(252.0) as annualized_20d_volatility,
    (close / nullif(lag(close, 20) over (
        partition by symbol, source order by trade_date
    ), 0)) - 1.0 as rolling_20d_return
from {{ ref('int_daily_returns') }}
window trailing_20 as (
    partition by symbol, source
    order by trade_date
    rows between 19 preceding and current row
)
