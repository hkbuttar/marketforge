select snapshot.source_security_key
from {{ ref('mart_company_snapshot') }} snapshot
left join (
    select distinct source_security_key from {{ ref('mart_security_daily') }}
) daily using (source_security_key)
where daily.source_security_key is null
