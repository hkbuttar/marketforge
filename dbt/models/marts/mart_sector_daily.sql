select
    returns.trade_date,
    sectors.sector,
    avg(returns.daily_return) as sector_average_return,
    count(returns.daily_return) as securities_with_returns
from {{ ref('int_daily_returns') }} returns
inner join {{ ref('security_sectors') }} sectors
    on returns.symbol = sectors.symbol
group by returns.trade_date, sectors.sector
