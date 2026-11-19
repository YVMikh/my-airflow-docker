# Findings from Weather Data

Results of the analysis of weather data loaded by the pipeline. The figures are a snapshot from November 2026 and will change as new data arrives.

## Weather models vs. airport observations

Open-Meteo reanalysis and forecast models were compared with METAR observations from the Almaty airport (UAAA) over about 59,000 overlapping hours.

| Model | Temperature MAE | Wind correlation | Notes |
|---|---|---|---|
| `ecmwf_ifs` | 1.91 °C | 0.32 | Selected as the primary model; accuracy improves over time (MAE from 2.2 to 1.5 °C) |
| `era5` | 2.20 °C | 0.01 | Kept as a reference series |
| `era5_land` | bias of −1.79 °C | n/a | No wind or precipitation; only temperature and humidity are available |

`ecmwf_ifs` is used as the primary model in `int_weather_hourly`.

## Airport observations (METAR)

- 26 gaps longer than three hours (the longest is 101 hours).
- 9 reports with typos. They are nulled and flagged in `stg_weather_metar`.
- Wind direction is empty in 14.8% of reports, which corresponds to calm and variable wind.
