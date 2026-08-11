with normalized as (
    select
        upper(trim(symbol)) as symbol,
        cast(date as date) as trade_date,
        cast(open as double) as open,
        cast(high as double) as high,
        cast(low as double) as low,
        cast(close as double) as close,
        cast(volume as bigint) as volume,
        lower(trim(source)) as source,
        trim(source_record_id) as source_record_id,
        cast(ingested_at as timestamptz) as ingested_at
    from {{ source('raw', 'prices') }}
), ranked as (
    select *, row_number() over (
        partition by symbol, trade_date, source
        order by ingested_at desc, source_record_id desc
    ) as record_rank
    from normalized
    where symbol is not null and trade_date is not null
      and open > 0 and high > 0 and low > 0 and close > 0 and volume >= 0
      and high >= greatest(low, open, close) and low <= least(open, close)
)
select
    md5(source || '|' || symbol || '|' || cast(trade_date as varchar)) as price_bar_key,
    symbol, trade_date, open, high, low, close, volume,
    source, source_record_id, ingested_at
from ranked where record_rank = 1
