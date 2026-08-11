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
    context.rolling_20d_volatility
from events
left join lateral (
    select trade_date, close, daily_return, rolling_20d_volatility
    from {{ ref('mart_security_daily') }} daily
    where daily.symbol = events.symbol
      and daily.trade_date <= cast(events.event_timestamp as date)
    order by daily.trade_date desc
    limit 1
) context on true
