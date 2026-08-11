select
    earnings_event_key,
    symbol,
    event_timestamp,
    fiscal_period_end,
    event_status,
    eps_estimate,
    eps_actual,
    eps_actual - eps_estimate as eps_surprise,
    case
        when eps_estimate is not null and abs(eps_estimate) > 0.000000001
            then (eps_actual - eps_estimate) / abs(eps_estimate)
        else null
    end as eps_surprise_percent,
    source,
    source_record_id,
    ingested_at
from {{ ref('stg_earnings') }}
