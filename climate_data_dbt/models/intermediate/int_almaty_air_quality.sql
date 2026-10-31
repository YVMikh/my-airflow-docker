{# Явный маппинг: Имя колонки в финальной таблице -> Имя staging-модели #}
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
-- 1. Собираем чистые данные из staging моделей, используя кластеры
{% for gas, model_name in gas_mapping.items() %}
{{ gas }}_clean as (
    select
        measured_at,
        cluster_id,
        cluster_name,
        -- Формируем имя исходной колонки (no_value для pm_no, иначе [gas]_value)
        {% set source_col = 'no_value' if gas == 'pm_no' else gas ~ '_value' %}
        {{ source_col }} as {{ gas }}
    from {{ ref(model_name) }}
),
{% endfor %}

-- 2. Собираем уникальные комбинации времени и кластеров со ВСЕХ источников
all_keys as (
    select distinct
        measured_at,
        cluster_id,
        cluster_name
    from (
        {% for gas in gas_mapping.keys() %}
        select measured_at, cluster_id, cluster_name from {{ gas }}_clean
        {% if not loop.last %} union all {% endif %}
        {% endfor %}
    ) as combined_keys
)

-- 3. Финальная сборка в единую широкую таблицу по кластерам
select
    k.measured_at,
    k.cluster_id,
    k.cluster_name,
    {% for gas in gas_mapping.keys() %}
    {{ gas }}_clean.{{ gas }}{% if not loop.last %},{% endif %}
    {% endfor %}
from all_keys k
{% for gas in gas_mapping.keys() %}
left join {{ gas }}_clean
    on k.measured_at = {{ gas }}_clean.measured_at
    and k.cluster_id = {{ gas }}_clean.cluster_id
{% endfor %}
