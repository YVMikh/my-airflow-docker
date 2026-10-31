with source as (
    select * from {{ source('eco_raw_data', 'almaty_h2s_history') }}
)
select
    cast(datetime_utc as timestamp) as measured_at,
    cast(cluster_id as varchar) as cluster_id,
    cast(cluster_name as varchar) as cluster_name,
    cast(value_ugm3 as numeric(10,4)) as h2s_value
from source