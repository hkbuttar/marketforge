with normalized as (
    select
        upper(trim(series_id)) as series_id,
        cast(observation_date as date) as observation_date,
        cast(released_at as timestamptz) as released_at,
        cast(value as double) as value,
        upper(trim(unit)) as unit,
        case upper(trim(frequency))
            when 'D' then 'DAILY'
            when 'W' then 'WEEKLY'
            when 'M' then 'MONTHLY'
            when 'Q' then 'QUARTERLY'
            when 'A' then 'ANNUAL'
            else upper(trim(frequency))
        end as frequency,
        lower(trim(source)) as source,
        trim(source_record_id) as source_record_id,
        cast(ingested_at as timestamptz) as ingested_at
    from {{ source('raw', 'macro') }}
), ranked as (
    select *, row_number() over (
        partition by source, source_record_id order by ingested_at desc
    ) as record_rank
    from normalized
    where series_id is not null and observation_date is not null
      and (released_at is null or released_at <= ingested_at)
)
select
    md5(source || '|' || source_record_id) as macro_observation_key,
    series_id, observation_date, released_at, value, unit, frequency,
    source, source_record_id, ingested_at
from ranked where record_rank = 1
