{{ config(enabled=var('enable_streamalpha', false), materialized='table') }}

with events as (
    select
        event_id,
        upper(symbol) as symbol,
        cast(event_timestamp as timestamptz) as event_timestamp,
        event_type,
        cast(price as double) as event_price,
        cast(anomaly_score as double) as anomaly_score,
        details_json,
        cast(detected_at as timestamptz) as detected_at,
        source_topic,
        kafka_partition,
        kafka_offset,
        cast(ingested_at as timestamptz) as ingested_at
    from read_parquet(
        '{{ var("raw_root", "data/raw") }}/stream_events/date=*/hour=*/*.parquet',
        hive_partitioning=true
    )
)
select
    events.*,
    context.trade_date as context_trade_date,
    context.close as prior_daily_close,
    context.daily_return as prior_daily_return,
    context.rolling_20d_volatility as prior_20d_volatility,
    context.relative_volume as prior_relative_volume,
    context.recent_5d_return,
    market.recent_5d_market_return,
    context.recent_5d_return - market.recent_5d_market_return as recent_factor_excess_return,
    earnings.event_timestamp as latest_earnings_timestamp,
    earnings.event_status as latest_earnings_status,
    earnings.eps_surprise as latest_eps_surprise,
    earnings.eps_surprise_percent as latest_eps_surprise_percent
from events
left join lateral (
    select
        arg_max(trade_date, trade_date) as trade_date,
        arg_max(close, trade_date) as close,
        arg_max(daily_return, trade_date) as daily_return,
        arg_max(rolling_20d_volatility, trade_date) as rolling_20d_volatility,
        arg_max(relative_volume, trade_date) as relative_volume,
        arg_max(close, trade_date) / nullif(arg_min(close, trade_date), 0) - 1.0 as recent_5d_return
    from (
        select * from {{ ref('mart_security_daily') }} daily
        where daily.symbol = events.symbol
          and daily.trade_date < cast(events.event_timestamp as date)
        order by daily.trade_date desc
        limit 5
    ) recent_security
) context on true
left join lateral (
    select product(1.0 + mean_return) - 1.0 as recent_5d_market_return
    from (
        select mean_return from {{ ref('mart_market_daily') }} market_daily
        where market_daily.trade_date < cast(events.event_timestamp as date)
        order by market_daily.trade_date desc
        limit 5
    ) recent_market
) market on true
left join lateral (
    select event_timestamp, event_status, eps_surprise, eps_surprise_percent
    from {{ ref('int_earnings_surprises') }} known_earnings
    where known_earnings.symbol = events.symbol
      and known_earnings.event_timestamp < events.event_timestamp
    order by known_earnings.event_timestamp desc
    limit 1
) earnings on true
