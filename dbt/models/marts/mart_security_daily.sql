select
    price_bar_key,
    md5(source || '|' || symbol) as source_security_key,
    trade_date,
    symbol,
    source,
    close,
    daily_return,
    rolling_20d_return,
    rolling_20d_volatility,
    annualized_20d_volatility,
    volume,
    avg(volume) over (
        partition by symbol, source order by trade_date
        rows between 19 preceding and current row
    ) as average_volume_20d,
    volume / nullif(avg(volume) over (
        partition by symbol, source order by trade_date
        rows between 19 preceding and current row
    ), 0) as relative_volume,
    return_observations_20d
from {{ ref('int_rolling_volatility') }}
