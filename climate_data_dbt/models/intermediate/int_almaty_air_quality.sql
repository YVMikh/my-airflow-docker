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
-- 1. Забираем агрегированные данные (максимум, минимум, среднее, медиану) из staging
{% for gas, model_name in gas_mapping.items() %}
{{ gas }}__clean as (
    select
        measured_at,
        cluster_id,
        cluster_name,
        -- Для pm_no базовый префикс 'no', для остальных совпадает с названием газа
        {% set base_prefix = 'no' if gas == 'pm_no' else gas %}
        {{ base_prefix }}_max as {{ gas }}_max,
        {{ base_prefix }}_min as {{ gas }}_min,
        {{ base_prefix }}_avg as {{ gas }}_avg,
        {{ base_prefix }}_median as {{ gas }}_median
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

-- 3. Финальная склейка всех метрик по каждому газу
select
    k.measured_at,
    k.cluster_id,
    k.cluster_name,
    {% for gas in gas_mapping.keys() %}
    {{ gas }}__clean.{{ gas }}_max,
    {{ gas }}__clean.{{ gas }}_min,
    {{ gas }}__clean.{{ gas }}_avg,
    {{ gas }}__clean.{{ gas }}_median{% if not loop.last %},{% endif %}
    {% endfor %}
from all_keys k
{% for gas in gas_mapping.keys() %}
left join {{ gas }}__clean
    on k.measured_at = {{ gas }}__clean.measured_at
    and k.cluster_id = {{ gas }}__clean.cluster_id
{% endfor %}
