with source as (
    select * from {{ source('eco_raw_data', 'almaty_hourly_weather') }}
)
select
    cast(weather_time as timestamp) as measured_at,
    cast(temperature_c as numeric(6,2)) as temperature,
    cast(humidity_percent as numeric(6,2)) as humidity,
    cast(wind_direction_deg as numeric(6,2)) as wind_direction,
    cast(wind_speed_ms as numeric(6,2)) as wind_speed
from source