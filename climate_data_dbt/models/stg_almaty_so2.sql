with source as (
    select * from {{ source('eco_raw_data', 'almaty_so2_history') }}
)
select
    cast(datetime_utc as timestamp) as measured_at,
    cast(station_id as varchar) as station_id,
    cast(value_ugm3 as numeric(6,2)) as so2_value,
    cast(cluster_name as varchar) as cluster_name
from source