{#
    Hourly statistics of one pollutant per geographical cluster.

    source_table : raw table in the eco_raw_data source (for example almaty_pm10_history)
    prefix       : pollutant code used in column names (pm10 -> pm10_max, pm10_min, ...)
    min_value    : smallest physically possible value. Rows below it are dropped from the statistics.
                   Defaults to var('air_quality_min_value', 0); pass 'none' to keep every row.

    Output grain: one row per measured_at (UTC hour start) and cluster_id.
    The statistics are taken over all readings of all stations of the cluster within the hour.
#}
{% macro stg_air_quality_hourly(source_table, prefix, min_value=None) %}
{%- set floor = min_value if min_value is not none else var('air_quality_min_value', 0) -%}
with source as (
    select * from {{ source('eco_raw_data', source_table) }}
)
select
    date_trunc('hour', cast(datetime_utc as timestamp)) as measured_at,
    cast(cluster_id as varchar) as cluster_id,
    cast(max(cluster_name) as varchar) as cluster_name,
    cast(max(value_ugm3) as numeric(10,4)) as {{ prefix }}_max,
    cast(min(value_ugm3) as numeric(10,4)) as {{ prefix }}_min,
    cast(avg(value_ugm3) as numeric(10,4)) as {{ prefix }}_avg,
    cast(percentile_cont(0.5) within group (order by value_ugm3) as numeric(10,4)) as {{ prefix }}_median,
    count(distinct station_id) as {{ prefix }}_station_count,
    count(*) as {{ prefix }}_reading_count
from source
where datetime_utc is not null
  and value_ugm3 is not null
  and cluster_id is not null
  {%- if floor is not none and floor != 'none' %}
  and value_ugm3 >= {{ floor }}
  {%- endif %}
group by 1, 2
{% endmacro %}
