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

with
-- 1. Забираем уже агрегированные (максимальные) данные из staging
{% for gas, model_name in gas_mapping.items() %}
{{ gas }}__clean as (
    select
        measured_at,
        cluster_id,
        cluster_name,
        {% set source_col = 'no_value' if gas == 'pm_no' else gas ~ '_value' %}
        {{ source_col }} as {{ gas }}
    from {{ ref(model_name) }}
),
{% endfor %}

-- 2. Собираем уникальную сетку Час + Кластер со всех источников
all_keys as (
    select distinct
        measured_at,
        cluster_id,
        cluster_name
    from (
        {% for gas in gas_mapping.keys() %}
        select measured_at, cluster_id, cluster_name from {{ gas }}__clean
        {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_keys
)

-- 3. Быстрая финальная склейка 1 к 1
select
    k.measured_at,
    k.cluster_id,
    k.cluster_name,
    {% for gas in gas_mapping.keys() %}
    {{ gas }}__clean.{{ gas }}{% if not loop.last %},{% endif %}
    {% endfor %}
from all_keys k
{% for gas in gas_mapping.keys() %}
left join {{ gas }}__clean
    on k.measured_at = {{ gas }}__clean.measured_at
    and k.cluster_id = {{ gas }}__clean.cluster_id
{% endfor %}
