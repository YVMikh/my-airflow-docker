{# Joining weather must neither multiply nor lose air quality rows. #}
with air as (
    select count(*) as n from {{ ref('int_almaty_air_quality') }}
),
joined as (
    select count(*) as n from {{ ref('int_almaty_air_quality_weather') }}
)
select
    air.n as air_rows,
    joined.n as joined_rows
from air
cross join joined
where air.n <> joined.n
