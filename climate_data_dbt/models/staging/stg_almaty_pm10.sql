with source as (
    select * from {{ source('eco_raw_data', 'almaty_pm10_history') }}
)
select
    date_trunc('hour', cast(datetime_utc as timestamp)) as measured_at,
    cast(cluster_id as varchar) as cluster_id,
    cast(max(cluster_name) as varchar) as cluster_name,
    cast(max(value_ugm3) as numeric(10,4)) as pm10_max,
    cast(min(value_ugm3) as numeric(10,4)) as pm10_min,
    cast(avg(value_ugm3) as numeric(10,4)) as pm10_avg,
    cast(percentile_cont(0.5) within group (order by value_ugm3) as numeric(10,4)) as pm10_median
from source
where datetime_utc is not null
  and value_ugm3 is not null
  and cluster_id is not null
group by 1, 2