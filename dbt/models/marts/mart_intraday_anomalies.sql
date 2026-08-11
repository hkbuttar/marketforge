{{ config(enabled=var('enable_streamalpha', false), materialized='view') }}

-- Stable portfolio-facing name; the original mart remains available for callers
-- that adopted the bridge before the cross-project integration was completed.
select * from {{ ref('mart_stream_anomalies') }}
