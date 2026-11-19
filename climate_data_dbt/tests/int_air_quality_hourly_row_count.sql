-- Returns a row when the long model has a different number of rows than all pollutant staging models combined.
{% set pollutants = ['pm25', 'pm10', 'so2', 'h2s', 'no2', 'no', 'o3', 'tsp', 'co'] %}

with staging_total as (
    select
        {% for pollutant in pollutants %}
        (select count(*) from {{ ref('stg_almaty_' ~ pollutant) }}){% if not loop.last %} +{% endif %}
        {% endfor %}
        as row_count
),

long_total as (
    select count(*) as row_count
    from {{ ref('int_air_quality_hourly') }}
)

select
    staging_total.row_count as staging_rows,
    long_total.row_count as long_model_rows
from staging_total
cross join long_total
where staging_total.row_count <> long_total.row_count
