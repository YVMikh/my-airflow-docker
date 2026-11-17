{#
    Reports with a physically impossible value (typos in the original METAR, for example a pressure group
    such as Q7016 or a wind of 81 m/s) are set to NULL in the staging model and flagged. About nine such
    reports are known (2022-2024), so isolated cases are normal. The test warns when there are more than
    15 and fails above 500, which would point to a parsing problem rather than typos.
#}
{{ config(warn_if='>15', error_if='>500') }}

select
    observed_at,
    metar_report
from {{ ref('stg_weather_metar') }}
where has_invalid_value
