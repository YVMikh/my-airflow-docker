{#
    Airport (UAAA) observations, one row per report (about every 30 minutes, plus special reports).
    Units are converted to SI. A value outside its physically possible range becomes NULL and the row
    is marked with has_invalid_value, so bad readings do not disappear silently. Reports are NOT
    aligned to the hour here, that is done in the intermediate layer.

    pressure_qnh_hpa is the altimeter setting (pressure reduced to sea level by the standard
    atmosphere). It is NOT station pressure and must not be compared directly with the
    surface_pressure of the models.
#}
with source as (
    select *
    from {{ source('eco_raw_data', 'almaty_weather_metar_obs') }}
    where
        valid is not null
        and station is not null
)

select
    station,
    cast(valid as timestamp) as observed_at,

    cast(case when tmpf between -58 and 122 then (tmpf - 32) / 1.8 end as numeric(7, 2)) as temperature_c,
    cast(case when dwpf between -70 and 100 then (dwpf - 32) / 1.8 end as numeric(7, 2)) as dewpoint_c,
    cast(case when relh between 0 and 100 then relh end as numeric(6, 2)) as relative_humidity_percent,
    cast(case when drct between 0 and 360 then drct end as numeric(6, 2)) as wind_direction_deg,
    cast(case when sknt between 0 and 120 then sknt * 0.514444 end as numeric(6, 2)) as wind_speed_ms,
    cast(case when gust between 0 and 150 then gust * 0.514444 end as numeric(6, 2)) as wind_gust_ms,
    cast(case when alti between 25 and 32 then alti * 33.8639 end as numeric(7, 2)) as pressure_qnh_hpa,
    cast(case when mslp between 900 and 1100 then mslp end as numeric(7, 2)) as pressure_msl_hpa,
    cast(case when vsby between 0 and 100 then vsby * 1.609344 end as numeric(7, 2)) as visibility_km,

    skyc1 as sky_cover_1,
    skyc2 as sky_cover_2,
    skyc3 as sky_cover_3,
    skyc4 as sky_cover_4,
    cast(skyl1 * 0.3048 as numeric(8, 1)) as cloud_base_1_m,
    cast(skyl2 * 0.3048 as numeric(8, 1)) as cloud_base_2_m,
    cast(skyl3 * 0.3048 as numeric(8, 1)) as cloud_base_3_m,
    cast(skyl4 * 0.3048 as numeric(8, 1)) as cloud_base_4_m,
    wxcodes as present_weather,

    -- Wind direction is not reported for calm or variable wind: such hours are flagged, not dropped
    coalesce(sknt = 0, false) as is_calm,
    coalesce(sknt > 0 and drct is null, false) as is_variable_direction,

    (
        (tmpf is not null and tmpf not between -58 and 122)
        or (dwpf is not null and dwpf not between -70 and 100)
        or (relh is not null and relh not between 0 and 100)
        or (drct is not null and drct not between 0 and 360)
        or (sknt is not null and sknt not between 0 and 120)
        or (gust is not null and gust not between 0 and 150)
        or (alti is not null and alti not between 25 and 32)
        or (mslp is not null and mslp not between 900 and 1100)
        or (vsby is not null and vsby not between 0 and 100)
    ) as has_invalid_value,

    metar as metar_report,
    cast(lat as numeric(8, 4)) as station_lat,
    cast(lon as numeric(8, 4)) as station_lon,
    cast(elevation as numeric(7, 1)) as station_elevation_m,
    loaded_at
from source
