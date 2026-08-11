with normalized as (
    select
        upper(trim(symbol)) as symbol,
        lower(trim(metric_name)) as metric_name,
        cast(period_start as date) as period_start,
        cast(period_end as date) as period_end,
        upper(trim(period_type)) as period_type,
        cast(filed_at as timestamptz) as filed_at,
        cast(value as double) as value,
        upper(trim(unit)) as unit,
        upper(trim(currency)) as currency,
        lower(trim(source)) as source,
        trim(source_record_id) as source_record_id,
        cast(ingested_at as timestamptz) as ingested_at
    from {{ source('raw', 'fundamentals') }}
), ranked as (
    select *, row_number() over (
        partition by source, source_record_id order by ingested_at desc
    ) as record_rank
    from normalized
    where symbol is not null and metric_name is not null and period_end is not null
      and (period_start is null or period_end >= period_start)
      and (filed_at is null or filed_at <= ingested_at)
)
select
    md5(source || '|' || source_record_id) as fundamental_observation_key,
    symbol, metric_name, period_start, period_end, period_type, filed_at,
    value, unit, currency, source, source_record_id, ingested_at
from ranked where record_rank = 1
