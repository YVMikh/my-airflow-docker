{{ config(
    materialized='table'
) }}

with no_data as (
    select * from {{ ref('stg_almaty_no') }}
),

no2_data as (
    select * from {{ ref('stg_almaty_no2') }}
),

o3_data as (
    select * from {{ ref('stg_almaty_o3') }}
),

pm10_data as (
    select * from {{ ref('stg_almaty_pm10') }}
),

pm25_data as (
    select * from {{ ref('stg_almaty_pm25') }}
),

so2_data as (
    select * from {{ ref('stg_almaty_so2') }}
),

-- Шаг 1: Собираем все уникальные временные метки (и кластеры/станции, если они у вас есть)
unique_keys as (
    select measured_at from no_data
    union
    select measured_at from no2_data
    union
    select measured_at from o3_data
    union
    select measured_at from pm10_data
    union
    select measured_at from pm25_data
    union
    select measured_at from so2_data
)

-- Шаг 2: Джойним все показатели к единой временной шкале
select
    k.measured_at,
    n.no_value,
    n2.no2_value,
    o3.o3_value,
    p10.pm10_value,
    p25.pm25_value,
    s2.so2_value
from unique_keys k
left join no_data n on k.measured_at = n.measured_at
left join no2_data n2 on k.measured_at = n2.measured_at
left join o3_data o3 on k.measured_at = o3.measured_at
left join pm10_data p10 on k.measured_at = p10.measured_at
left join pm25_data p25 on k.measured_at = p25.measured_at
left join so2_data s2 on k.measured_at = s2.measured_at
order by k.measured_at desc
