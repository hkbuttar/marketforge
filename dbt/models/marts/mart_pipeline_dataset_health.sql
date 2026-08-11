with dataset_metrics as (
    select 'prices' dataset, max(trade_date)::timestamp latest_event_time, count(*) row_count,
           avg(case when symbol is null or close is null then 1.0 else 0.0 end) null_rate,
           count(*) - count(distinct price_bar_key) duplicate_count from {{ ref('stg_prices') }}
    union all
    select 'fundamentals', max(period_end)::timestamp, count(*),
           avg(case when symbol is null or metric_name is null or value is null then 1.0 else 0.0 end),
           count(*) - count(distinct fundamental_observation_key) from {{ ref('stg_fundamentals') }}
    union all
    select 'earnings', max(event_timestamp)::timestamp, count(*),
           avg(case when symbol is null or event_timestamp is null then 1.0 else 0.0 end),
           count(*) - count(distinct earnings_event_key) from {{ ref('stg_earnings') }}
    union all
    select 'macro', max(observation_date)::timestamp, count(*),
           avg(case when series_id is null or value is null then 1.0 else 0.0 end),
           count(*) - count(distinct macro_observation_key) from {{ ref('stg_macro') }}
    union all
    select 'news', max(event_timestamp)::timestamp, count(*),
           avg(case when headline is null or event_timestamp is null then 1.0 else 0.0 end),
           count(*) - count(distinct news_event_key) from {{ ref('stg_news') }}
), ranked_runs as (
    select *, row_number() over (
        partition by dataset order by cast(completed_at as timestamptz) desc, run_id desc
    ) as run_rank
    from ingestion_runs
), latest_runs as (
    select * from ranked_runs where run_rank = 1
), ranked_successful_runs as (
    select *, row_number() over (
        partition by dataset order by cast(completed_at as timestamptz) desc, run_id desc
    ) as run_rank
    from ingestion_runs
    where status = 'success'
), latest_successful_runs as (
    select * from ranked_successful_runs where run_rank = 1
)
select
    metrics.dataset,
    successful.run_id as last_successful_run,
    cast(successful.completed_at as timestamptz) as last_successful_run_at,
    metrics.latest_event_time,
    date_diff('minute', metrics.latest_event_time, current_timestamp) as freshness_minutes,
    metrics.row_count,
    metrics.null_rate,
    metrics.duplicate_count,
    coalesce(runs.quarantined_rows, 0) as quarantine_count,
    case
        when metrics.row_count = 0 then 'empty'
        when metrics.duplicate_count > 0 or metrics.null_rate > 0
          or coalesce(runs.quarantined_rows, 0) > 0 then 'degraded'
        when successful.run_id is null then 'unknown'
        else 'healthy'
    end as status
from dataset_metrics metrics
left join latest_runs runs using (dataset)
left join latest_successful_runs successful using (dataset)
