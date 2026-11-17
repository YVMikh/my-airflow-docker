{{ config(
    indexes=[{'columns': ['measured_at'], 'unique': True}]
) }}

{#
    One row per hour (UTC, start of the hour) of weather for Almaty.

    * Model columns come from ONE model, var('primary_weather_model') (ecmwf_ifs by default).
      They are instantaneous values at the start of the hour; precipitation, snowfall and
      radiation describe the PRECEDING hour in Open-Meteo, so for the hour [t, t+1) they are taken from
      the row labelled t + var('weather_flux_label_offset_hours') hours (1 by default).
    * Wind is also given as east/north components (direction is where the wind blows FROM), which can be
      averaged correctly, unlike a direction in degrees. Calm wind is flagged.
    * obs_* columns are airport (UAAA) observations, kept separate from the model values and never
      mixed with them: the nearest report within half an hour of the hour (a report without invalid
      values is preferred), with its distance in minutes.
#}

{% set primary_model = var('primary_weather_model', 'ecmwf_ifs') %}
{% set flux_offset = var('weather_flux_label_offset_hours', 1) %}
{% set calm_threshold = var('calm_wind_threshold_ms', 0.5) %}

with model_hours as (
    select *
    from {{ ref('stg_weather_openmeteo') }}
    where model = '{{ primary_model }}'
),

model_aligned as (
    select
        m.measured_at,
        m.temperature_c,
        m.humidity_percent,
        m.wind_speed_ms,
        m.wind_direction_deg,
        m.surface_pressure_hpa,
        m.cloud_cover_percent,
        -- sums and means over the preceding hour: take the row labelled one hour later
        f.precipitation_mm,
        f.snowfall_cm,
        f.shortwave_radiation_wm2,
        m.grid_elevation_m
    from model_hours m
    left join model_hours f
        on f.measured_at = m.measured_at + interval '{{ flux_offset }} hour'
),

wind as (
    select
        *,
        case when wind_direction_deg is not null then -wind_speed_ms * sin(radians(wind_direction_deg)) end as wind_u_ms,
        case when wind_direction_deg is not null then -wind_speed_ms * cos(radians(wind_direction_deg)) end as wind_v_ms,
        (wind_speed_ms < {{ calm_threshold }}) as is_calm_wind
    from model_aligned
),

obs_bucketed as (
    select
        *,
        -- nearest hour: a report at hh:29 belongs to hh:00, a report at hh:30 to the next hour
        date_trunc('hour', observed_at + interval '30 minutes') as bucket_hour
    from {{ ref('stg_weather_metar') }}
    where station = 'UAAA'
),

obs_ranked as (
    select
        *,
        row_number() over (
            partition by bucket_hour
            -- a report without invalid values is preferred, then the nearest one
            order by has_invalid_value, abs(extract(epoch from (observed_at - bucket_hour))), observed_at
        ) as rn
    from obs_bucketed
),

obs as (
    select
        bucket_hour as measured_at,
        observed_at as obs_observed_at,
        cast(round(extract(epoch from (observed_at - bucket_hour)) / 60) as integer) as obs_minutes_from_hour,
        temperature_c as obs_temperature_c,
        dewpoint_c as obs_dewpoint_c,
        relative_humidity_percent as obs_relative_humidity_percent,
        wind_speed_ms as obs_wind_speed_ms,
        wind_direction_deg as obs_wind_direction_deg,
        wind_gust_ms as obs_wind_gust_ms,
        case when wind_direction_deg is not null then -wind_speed_ms * sin(radians(wind_direction_deg)) end as obs_wind_u_ms,
        case when wind_direction_deg is not null then -wind_speed_ms * cos(radians(wind_direction_deg)) end as obs_wind_v_ms,
        pressure_qnh_hpa as obs_pressure_qnh_hpa,
        visibility_km as obs_visibility_km,
        sky_cover_1 as obs_sky_cover_1,
        cloud_base_1_m as obs_cloud_base_1_m,
        present_weather as obs_present_weather,
        is_calm as obs_is_calm,
        is_variable_direction as obs_is_variable_direction,
        has_invalid_value as obs_has_invalid_value
    from obs_ranked
    where rn = 1
)

select
    w.measured_at,
    w.temperature_c,
    w.humidity_percent,
    w.wind_speed_ms,
    w.wind_direction_deg,
    cast(w.wind_u_ms as numeric(6,2)) as wind_u_ms,
    cast(w.wind_v_ms as numeric(6,2)) as wind_v_ms,
    w.is_calm_wind,
    w.surface_pressure_hpa,
    w.cloud_cover_percent,
    w.precipitation_mm,
    w.snowfall_cm,
    w.shortwave_radiation_wm2,
    w.grid_elevation_m,
    o.obs_observed_at,
    o.obs_minutes_from_hour,
    o.obs_temperature_c,
    o.obs_dewpoint_c,
    o.obs_relative_humidity_percent,
    o.obs_wind_speed_ms,
    o.obs_wind_direction_deg,
    o.obs_wind_gust_ms,
    cast(o.obs_wind_u_ms as numeric(6,2)) as obs_wind_u_ms,
    cast(o.obs_wind_v_ms as numeric(6,2)) as obs_wind_v_ms,
    o.obs_pressure_qnh_hpa,
    o.obs_visibility_km,
    o.obs_sky_cover_1,
    o.obs_cloud_base_1_m,
    o.obs_present_weather,
    o.obs_is_calm,
    o.obs_is_variable_direction,
    o.obs_has_invalid_value
from wind w
left join obs o
    on o.measured_at = w.measured_at
