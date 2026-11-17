{#
    Open-Meteo weather, one row per hour (UTC) and model. All loaded models are kept
    (ecmwf_ifs is the primary one, the choice is made in the intermediate layer via
    var('primary_weather_model')). Only renaming and type casting happens here.

    Meaning of values (Open-Meteo): temperature, humidity, wind and pressure are instantaneous values at
    the timestamp; precipitation, snowfall and shortwave radiation are a sum or mean over the PRECEDING hour.
#}
select
    cast(weather_time as timestamp) as measured_at,
    model,
    cast(temperature_c as numeric(7,2)) as temperature_c,
    cast(humidity_percent as numeric(6,2)) as humidity_percent,
    cast(wind_speed_ms as numeric(6,2)) as wind_speed_ms,
    cast(wind_direction_deg as numeric(6,2)) as wind_direction_deg,
    cast(precipitation_mm as numeric(7,2)) as precipitation_mm,
    cast(snowfall_cm as numeric(7,2)) as snowfall_cm,
    cast(surface_pressure_hpa as numeric(7,2)) as surface_pressure_hpa,
    cast(cloud_cover_percent as numeric(6,2)) as cloud_cover_percent,
    cast(shortwave_radiation_wm2 as numeric(7,2)) as shortwave_radiation_wm2,
    cast(grid_lat as numeric(8,4)) as grid_lat,
    cast(grid_lon as numeric(8,4)) as grid_lon,
    cast(grid_elevation_m as numeric(7,1)) as grid_elevation_m,
    loaded_at
from {{ source('eco_raw_data', 'almaty_weather_openmeteo_hourly') }}
where weather_time is not null
  and model is not null
