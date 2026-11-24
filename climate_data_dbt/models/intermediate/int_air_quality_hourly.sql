{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['measured_at', 'cluster_id', 'pollutant'], 'unique': True},
            {'columns': ['pollutant', 'measured_at']}
        ]
    )
}}

{#
    Long-format air quality model and the main integration model for air quality.
    Grain: one row per hour (measured_at, start of the hour, UTC), geographical cluster and pollutant.
    Statistics of value_ugm3 across all readings of the cluster's stations within the hour,
    plus the number of distinct stations.

    Each stg_almaty_<code> model names its columns by the same prefix as its code
    ({code}_max, {code}_min, {code}_avg, {code}_median, {code}_station_count),
    so the pollutant code is used both for the ref and for the column names.
#}

{% set pollutants = ['pm25', 'pm10', 'so2', 'h2s', 'no2', 'no', 'o3', 'tsp', 'co'] %}

{% for code in pollutants %}
    select
        measured_at,
        cluster_id,
        '{{ code }}' as pollutant,
        {{ code }}_max as value_max,
        {{ code }}_min as value_min,
        {{ code }}_avg as value_avg,
        {{ code }}_median as value_median,
        {{ code }}_station_count as station_count
    from {{ ref('stg_almaty_' ~ code) }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
