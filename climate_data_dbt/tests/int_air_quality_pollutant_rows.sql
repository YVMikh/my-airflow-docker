{#
    For every pollutant the number of non-NULL maximums in the wide table must equal the number
    of rows of its staging model (one row per hour and cluster). Returns the pollutants that differ.
#}
{% set gas_mapping = {
    'pm10': 'stg_almaty_pm10',
    'pm25': 'stg_almaty_pm25',
    'so2': 'stg_almaty_so2',
    'h2s': 'stg_almaty_h2s',
    'no2': 'stg_almaty_no2',
    'pm_no': 'stg_almaty_no',
    'o3': 'stg_almaty_o3',
    'tsp': 'stg_almaty_tsp',
    'co': 'stg_almaty_co'
} %}

with compared as (
    {% for gas, model_name in gas_mapping.items() %}
    select
        '{{ gas }}' as pollutant,
        (select count(*) from {{ ref(model_name) }}) as staging_rows,
        (select count({{ gas }}_max) from {{ ref('int_almaty_air_quality') }}) as wide_rows
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select *
from compared
where staging_rows <> wide_rows
