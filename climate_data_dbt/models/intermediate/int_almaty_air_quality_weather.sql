{{ config(
    indexes=[{'columns': ['measured_at', 'cluster_id'], 'unique': True}]
) }}

{#
    Hourly air quality per cluster with the weather of the same hour (one weather point for the whole
    city, so every cluster of an hour gets the same weather; it is not the local weather of the cluster).
    LEFT JOIN: air quality measurements are never lost when weather is missing (has_weather = false),
    for example for the latest hours that the weather archive does not cover yet.
    The grain stays that of int_almaty_air_quality: one row per measured_at and cluster_id.
#}

select
    a.*,  -- noqa: AM04
    (w.measured_at is not null) as has_weather,
    w.temperature_c,
    w.humidity_percent,
    w.wind_speed_ms,
    w.wind_direction_deg,
    w.wind_u_ms,
    w.wind_v_ms,
    w.is_calm_wind,
    w.surface_pressure_hpa,
    w.cloud_cover_percent,
    w.precipitation_mm,
    w.snowfall_cm,
    w.shortwave_radiation_wm2,
    w.obs_observed_at,
    w.obs_minutes_from_hour,
    w.obs_temperature_c,
    w.obs_dewpoint_c,
    w.obs_relative_humidity_percent,
    w.obs_wind_speed_ms,
    w.obs_wind_direction_deg,
    w.obs_wind_gust_ms,
    w.obs_wind_u_ms,
    w.obs_wind_v_ms,
    w.obs_pressure_qnh_hpa,
    w.obs_visibility_km,
    w.obs_sky_cover_1,
    w.obs_cloud_base_1_m,
    w.obs_present_weather,
    w.obs_is_calm,
    w.obs_is_variable_direction,
    w.obs_has_invalid_value
from {{ ref('int_almaty_air_quality') }} as a
left join {{ ref('int_weather_hourly') }} as w
    on a.measured_at = w.measured_at
