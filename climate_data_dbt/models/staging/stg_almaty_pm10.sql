with source as (
    select * from {{ source('eco_raw_data', 'almaty_pm10_history') }}
)
select
    cast(datetime_utc as timestamp) as measured_at,
    cast(cluster_id as varchar) as cluster_id,
    cast(cluster_name as varchar) as cluster_name,
    cast(value_ugm3 as numeric(10,4)) as pm10_value
from source