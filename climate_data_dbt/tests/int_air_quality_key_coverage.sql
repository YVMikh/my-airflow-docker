{#
    The wide table must contain exactly the set of (measured_at, cluster_id) keys that exist in
    at least one staging model: no lost and no multiplied rows. Returns a row on a mismatch.
#}
{% set staging_models = [
    'stg_almaty_pm10', 'stg_almaty_pm25', 'stg_almaty_so2', 'stg_almaty_h2s', 'stg_almaty_no2',
    'stg_almaty_no', 'stg_almaty_o3', 'stg_almaty_tsp', 'stg_almaty_co'
] %}

with expected as (
    select count(*) as n
    from (
        {% for model_name in staging_models %}
        select measured_at, cluster_id from {{ ref(model_name) }}
        {% if not loop.last %} union {% endif %}
        {% endfor %}
    ) as keys
),

actual as (
    select count(*) as n from {{ ref('int_almaty_air_quality') }}
)

select
    expected.n as expected_rows,
    actual.n as actual_rows
from expected
cross join actual
where expected.n <> actual.n
