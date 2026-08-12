with effective_dated as (
    select
        fundamental_observation_key,
        symbol,
        metric_name,
        period_start,
        period_end,
        period_type,
        filed_at,
        value,
        unit,
        currency,
        source,
        source_record_id,
        ingested_at,
        coalesce(filed_at, cast(period_end as timestamp)) as effective_from
    from {{ ref('stg_fundamentals') }}
)
select
    *,
    lead(effective_from) over (
        partition by symbol, metric_name, period_end, source
        order by effective_from, ingested_at, source_record_id
    ) as effective_to
from effective_dated
