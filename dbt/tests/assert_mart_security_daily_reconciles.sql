select 'row_count_mismatch' as failure
where
  (select count(*) from {{ ref('mart_security_daily') }})
  != (select count(*) from {{ ref('int_rolling_volatility') }})
