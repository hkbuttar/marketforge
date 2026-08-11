with lagged as (
    select
        price_bar_key,
        symbol,
        trade_date,
        close,
        volume,
        source,
        lag(close) over (
            partition by symbol, source order by trade_date
        ) as previous_close
    from {{ ref('stg_prices') }}
)
select
    price_bar_key,
    symbol,
    trade_date,
    close,
    volume,
    source,
    previous_close,
    case
        when previous_close > 0 then (close / previous_close) - 1.0
        else null
    end as daily_return
from lagged
