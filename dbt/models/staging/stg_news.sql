with normalized as (
    select
        cast(event_timestamp as timestamptz) as event_timestamp,
        trim(headline) as headline,
        trim(url) as url,
        trim(publisher) as publisher,
        lower(trim(source)) as source,
        trim(source_record_id) as source_record_id,
        cast(ingested_at as timestamptz) as ingested_at
    from {{ source('raw', 'news') }}
), ranked as (
    select *, row_number() over (
        partition by source, source_record_id order by ingested_at desc
    ) as record_rank
    from normalized
    where event_timestamp is not null and headline is not null and length(headline) > 0
      and (url is null or starts_with(url, 'http://') or starts_with(url, 'https://'))
)
select
    md5(source || '|' || source_record_id) as news_event_key,
    event_timestamp, headline, url, publisher,
    source, source_record_id, ingested_at
from ranked where record_rank = 1
