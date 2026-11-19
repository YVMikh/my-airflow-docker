-- Returns rows that violate min <= median <= max, min <= avg <= max, or station_count >= 1.
-- A small tolerance absorbs floating-point rounding in avg and median.
select
    measured_at,
    cluster_id,
    pollutant,
    value_min,
    value_avg,
    value_median,
    value_max,
    station_count
from {{ ref('int_air_quality_hourly') }}
where value_min > value_median + 1e-9
   or value_median > value_max + 1e-9
   or value_min > value_avg + 1e-9
   or value_avg > value_max + 1e-9
   or station_count < 1
