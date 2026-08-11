select * from {{ ref('int_macro_aligned') }}
where available_date > trade_date
