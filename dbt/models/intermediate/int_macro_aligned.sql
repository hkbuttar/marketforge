with trading_dates as (
    select distinct trade_date from {{ ref('stg_prices') }}
), series as (
    select distinct series_id, source from {{ ref('stg_macro') }}
), available_macro as (
    select
        *,
        greatest(
            observation_date,
            cast(coalesce(released_at, cast(observation_date as timestamp)) as date)
        ) as available_date
    from {{ ref('stg_macro') }}
)
select
    dates.trade_date,
    series.series_id,
    series.source,
    macro.macro_observation_key,
    macro.observation_date,
    macro.released_at,
    macro.available_date,
    macro.value,
    macro.unit,
    macro.frequency
from trading_dates dates
cross join series
asof left join available_macro macro
    on series.series_id = macro.series_id
    and series.source = macro.source
    and dates.trade_date >= macro.available_date
