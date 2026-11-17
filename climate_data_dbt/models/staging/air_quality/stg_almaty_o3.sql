{{ config(
    materialized='table',
    indexes=[{'columns': ['measured_at', 'cluster_id'], 'unique': True}]
) }}

{{ stg_air_quality_hourly('almaty_o3_history', 'o3') }}
