{{ config(
    materialized='table',
    indexes=[{'columns': ['measured_at', 'cluster_id'], 'unique': True}]
) }}

{{ stg_air_quality_hourly('almaty_h2s_history', 'h2s') }}
