with expected(dataset) as (
    values ('prices'), ('fundamentals'), ('earnings'), ('macro'), ('news')
)
select expected.dataset
from expected
left join {{ ref('mart_pipeline_dataset_health') }} health using (dataset)
where health.dataset is null
