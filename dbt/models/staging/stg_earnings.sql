with normalized as (
    select
        upper(trim(symbol)) as symbol,
        cast(event_timestamp as timestamptz) as event_timestamp,
        cast(fiscal_period_end as date) as fiscal_period_end,
        upper(trim(event_status)) as event_status,
        cast(eps_estimate as double) as eps_estimate,
        cast(eps_actual as double) as eps_actual,
        lower(trim(source)) as source,
        trim(source_record_id) as source_record_id,
        cast(ingested_at as timestamptz) as ingested_at
    from {{ source('raw', 'earnings') }}
), ranked as (
    select *, row_number() over (
        partition by source, source_record_id order by ingested_at desc
    ) as record_rank
    from normalized
    where symbol is not null and event_timestamp is not null and event_status is not null
)
select
    md5(source || '|' || source_record_id) as earnings_event_key,
    symbol, event_timestamp, fiscal_period_end, event_status,
    eps_estimate, eps_actual, source, source_record_id, ingested_at
from ranked where record_rank = 1
