with latest_price as (
    select * exclude (row_number)
    from (
        select *, row_number() over (
            partition by symbol, source order by trade_date desc
        ) as row_number
        from {{ ref('stg_prices') }}
    ) where row_number = 1
), fundamental_summary as (
    select
        symbol,
        source,
        max(period_end) as latest_fundamental_period_end,
        max(filed_at) as latest_fundamental_filed_at,
        count(distinct metric_name) as available_fundamental_metrics
    from {{ ref('stg_fundamentals') }}
    group by symbol, source
), latest_earnings as (
    select * exclude (row_number)
    from (
        select *, row_number() over (
            partition by symbol, source order by event_timestamp desc
        ) as row_number
        from {{ ref('int_earnings_surprises') }}
    ) where row_number = 1
)
select
    md5(price.source || '|' || price.symbol) as source_security_key,
    price.symbol,
    price.source,
    price.trade_date as latest_price_date,
    price.close as latest_price,
    fundamentals.latest_fundamental_period_end,
    fundamentals.latest_fundamental_filed_at,
    fundamentals.available_fundamental_metrics,
    earnings.event_timestamp as latest_earnings_timestamp,
    earnings.eps_actual as latest_eps_actual,
    earnings.eps_surprise as latest_eps_surprise,
    date_diff('minute', cast(price.trade_date as timestamp), current_timestamp) as price_freshness_minutes
from latest_price price
left join fundamental_summary fundamentals using (symbol, source)
left join latest_earnings earnings using (symbol, source)
