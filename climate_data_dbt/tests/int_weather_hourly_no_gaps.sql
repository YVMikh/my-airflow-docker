{# The hourly series of the primary model must be continuous: hours between the first and the last = rows. #}
select
    count(*) as actual_hours,
    cast(extract(epoch from (max(measured_at) - min(measured_at))) / 3600 + 1 as bigint) as expected_hours
from {{ ref('int_weather_hourly') }}
having count(*) <> cast(extract(epoch from (max(measured_at) - min(measured_at))) / 3600 + 1 as bigint)
