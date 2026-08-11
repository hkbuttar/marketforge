select
    trade_date,
    avg(daily_return) as mean_return,
    median(daily_return) as median_return,
    stddev_samp(daily_return) as cross_sectional_volatility,
    count(*) filter (where daily_return > 0) as advancers,
    count(*) filter (where daily_return < 0) as decliners,
    count(*) filter (where daily_return = 0) as unchanged,
    (
        count(*) filter (where daily_return > 0)
        - count(*) filter (where daily_return < 0)
    )::double / nullif(count(daily_return), 0) as market_breadth,
    count(daily_return) as securities_with_returns
from {{ ref('int_daily_returns') }}
group by trade_date
