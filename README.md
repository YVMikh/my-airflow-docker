# Climate and Air Quality Data Pipeline — Almaty

## Project Overview

This Data Engineering project collects, consolidates, and transforms air quality and weather data for Almaty, Kazakhstan, from publicly available sources.

The project implements an automated, multi-layer ELT pipeline. Airflow loads raw observations from external sources into PostgreSQL, and dbt Core transforms them into analytical datasets. Load monitoring and data quality checks are written to a dedicated `monitoring` schema. The whole stack runs locally in Docker Compose.

**Project status:** Active development. Ingestion, monitoring, staging, intermediate models, and dbt tests are implemented. The marts layer and BI dashboards are planned.

## Objectives

- Automate data collection from public air quality and weather sources.
- Consolidate observations from several sources in one PostgreSQL database.
- Load data incrementally and idempotently, with independent watermarks per source.
- Aggregate multi-station observations into stable geographical clusters, preserving the distribution of values within each cluster (maximum, minimum, mean, and median).
- Track load history and data quality in a dedicated monitoring schema.
- Build analytical models for future BI visualization in Apache Superset: a long-format air quality model as the main integration model, and a model that joins air quality with weather.

## Technology Stack

| Component | Purpose |
|---|---|
| Python / Pandas | Data extraction, chunked file processing |
| Apache Airflow 2.10.5 | Orchestration, scheduling, task dependencies |
| PostgreSQL | Raw, transformed, and monitoring data |
| dbt Core 1.12.7 (dbt-postgres 1.11.0) | Layered transformations, model management, data tests |
| Docker Compose | Containerized local environment |
| Git | Version control |

## Data Sources

| Source | Content | Coverage |
|---|---|---|
| Air quality archives (`csv.gz`, GitHub) | 9 pollutants: PM2.5, PM10, NO2, NO, SO2, CO, O3, H2S, TSP | Station-level observations, grouped into geographical clusters |
| Open-Meteo archive API | Hourly weather for the point 43.2565, 76.9285 (UTC). Models: `ecmwf_ifs`, `era5`, `era5_land` | Since 2020-01-01 |
| METAR reports, Almaty airport (UAAA, via Iowa Environmental Mesonet) | Observed weather at the airport | Since 2020-01-01 |

`era5_land` provides only temperature and humidity.

Timestamps in all sources mark the start of the measured hour in UTC, for example `2026-10-08 15:00:00+00:00`.

## Architecture

```
Sources ──► Airflow DAGs ──► PostgreSQL raw ──► dbt staging ──► dbt intermediate ──► (marts, planned)
                  │                                   │
                  └──────────► monitoring schema ◄────┘
```

### 1. Ingestion — Airflow

Each source has an `_init` DAG (full historical load) and a `_sync` DAG (scheduled update):

| Source | DAGs |
|---|---|
| Air quality | `almaty_air_raw_init`, `almaty_air_raw_sync` |
| Open-Meteo | `almaty_weather_openmeteo_init`, `almaty_weather_openmeteo_sync` |
| METAR | `almaty_weather_metar_init`, `almaty_weather_metar_sync` |

Common mechanisms:

- **Independent watermarks** per source, stored in `monitoring.etl_watermarks`.
- **Idempotent writes**: `ON CONFLICT` with unique keys, so repeated loads do not create duplicates.
- **Recheck window** to pick up revised values: 3 days for air quality and METAR, 10 days for Open-Meteo.
- **Atomic load**: data, watermark, and load log are written in a single transaction.
- **Load modes**: `incremental` and `full`.
- **Memory-efficient download**: air quality archives are downloaded in 16 KB chunks to disk. Unchanged archives are skipped by ETag.
- **Schedule**: daily at 16:00 (Asia/Almaty), `catchup=False`.
- **Graceful initialization**: a configurable default watermark is used when no watermark exists yet.
- **Error handling**: controlled exception handling during ingestion and database operations.

### 2. Monitoring — `monitoring` schema

Tables: `etl_watermarks`, `etl_load_log`, `dq_run_results`, `source_coverage`.

Raw-layer checks record their results in `dq_run_results` and do not stop the DAG:

- **Air quality**: empty, negative, and future values; empty cluster; freshness; lag relative to other sources.
- **Open-Meteo**: value ranges, share of empty values, empty tail, gaps, jumps, grid changes, revised values, freshness.
- **METAR**: value ranges, empty key fields, empty wind direction when wind is present, dew point above temperature, observation density, gaps, jumps, freshness.

Alerting on `warn` and `alert` results is not implemented yet.

### 3. Staging Layer — dbt

**Air quality** (9 models, one macro `stg_air_quality_hourly`):

- Materialized as tables with a unique index.
- Invalid records are filtered out (NULL values and values below zero).
- Observations from independent sensors in the same geographical cluster are consolidated.
- Timestamps are truncated to the hour with `date_trunc`. For each pollutant, cluster, and hour, the model computes `MAX`, `MIN`, `AVG`, and the median (`percentile_cont(0.5)`) of `value_ugm3`.
- Keeping all four statistics preserves the spread of readings within a cluster and separates local spikes from cluster-wide pollution.
- Aggregation at this layer prevents row multiplication in later joins.

**Weather**: `stg_weather_openmeteo` and `stg_weather_metar`, materialized as views. METAR reports with obvious typos are nulled and flagged.

### 4. Intermediate Layer — dbt

| Model | Description |
|---|---|
| `int_air_quality_hourly` | Main integration model for air quality, in long format: one row per hour, geographical cluster, and pollutant, with four statistics (max, min, mean, median) and the number of stations. Physical table with a unique index on the grain. Pollutant codes: `pm25`, `pm10`, `so2`, `h2s`, `no2`, `no`, `o3`, `tsp`, `co` |
| `int_almaty_air_quality` | Wide-format layer kept for compatibility. Physical table with one row per hour and cluster, built with Jinja loops over the 9 pollutants, with four statistics per pollutant. Nitrogen oxide columns use the `pm_no_` prefix |
| `int_weather_hourly` | Hourly weather built from the primary model (`ecmwf_ifs`) and METAR |
| `int_almaty_air_quality_weather` | Air quality joined with hourly weather |

**Performance optimization.** The first version of the wide table was built in an uncontrolled and redundant way: a data fan-out in the `JOIN` multiplied rows, and the table grew to 8 GB. The build was redesigned: aggregation was moved to the staging layer, and the table is now assembled from a single grid of keys with joins on a unique grain (hour and cluster). The table shrank from 8 GB to about 57 MB (roughly 140x). Uniqueness and row count tests now guard the grain, so the build no longer produces such an inefficient result.

## Data Quality and Testing

Data quality is checked at two levels: dbt tests on the transformed layers and raw-layer checks in Airflow (see Monitoring).

dbt tests cover:

- uniqueness of the grain and of composite keys, and absence of NULLs in key columns;
- value ranges, non-negativity, and no timestamps in the future;
- consistency of aggregated statistics (`min <= median <= max`, `min <= avg <= max`);
- accepted values for model, station, and pollutant fields;
- key coverage between layers and completeness of the primary weather model;
- continuity of the hourly weather series;
- joins that neither duplicate nor lose rows;
- row count of the long air quality model matching the sum of the pollutant staging models.

Custom generic tests are defined in `climate_data_dbt/tests/generic/` and do not require external dbt packages. Some checks are configured with `warn` severity. Freshness thresholds for raw sources are defined in `sources.yml` and are checked with `dbt source freshness`.

To run the tests:

```bash
dbt test
```

## Additional Documentation

- [Findings from weather data](docs/weather_data_findings.md): comparison of weather models with airport observations and notes on METAR data quality.

## Project Structure

```
.
├── airflow/dags/
│   ├── almaty_air_raw_sync.py
│   ├── almaty_weather_metar_sync.py
│   └── almaty_weather_openmeteo_sync.py
├── climate_data_dbt/
│   ├── macros/stg_air_quality_hourly.sql
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/air_quality/      # 9 pollutant models
│   │   ├── staging/weather/          # Open-Meteo and METAR
│   │   └── intermediate/
│   └── tests/
│       ├── generic/generic_tests.sql # custom generic tests
│       └── *.sql                     # singular tests
├── docs/                             # weather data findings, Airflow configuration screenshot, draft architecture files (not yet updated)
├── superset/                         # Superset configuration for the local environment
├── docker-compose.yaml
├── .env.example
├── environment.yml
└── requirements.txt
```

## How to Run

### 1. Configure and start the environment

Copy `.env.example` to `.env` and fill in the secrets and connection settings required by Airflow and PostgreSQL (including the Airflow Fernet key and secret key). Ports are bound to `127.0.0.1`.

```bash
docker-compose up -d
docker-compose ps
```

### 2. Load data

In the Airflow web UI, unpause and trigger the `_init` DAGs once for the historical load, then leave the `_sync` DAGs on their schedule:

- `almaty_air_raw_init`, `almaty_air_raw_sync`
- `almaty_weather_openmeteo_init`, `almaty_weather_openmeteo_sync`
- `almaty_weather_metar_init`, `almaty_weather_metar_sync`

### 3. Run dbt

From the dbt project directory:

```bash
dbt clean
dbt build                               # all models and tests
dbt build --select staging.air_quality+ # air quality staging and downstream models
dbt build --select staging.weather+     # weather staging and downstream models
```

Make sure the dbt profile points to the project's PostgreSQL database.

## Roadmap

**Next**

- Marts layer: hourly mart, daily mart, weather impact mart, rolling 24-hour averages, daily AQI.
- Run dbt from Airflow, including `dbt source freshness`.
- Incremental dbt models and handling of late-arriving and updated source records.
- Notifications on `warn` and `alert` results.

**BI**

- Apache Superset dashboards for air quality and climate trends, including a data quality dashboard based on the monitoring schema.

**Technical debt**

- Drop leftover legacy database objects (`almaty_hourly_weather`, `stg_almaty_weather`) if they still exist.
- Keep a single dependency source (`environment.yml` or `requirements.txt`) and drop `requests-cache` from `_PIP_ADDITIONAL_REQUIREMENTS`.

## Known Limitations

- The license of the air quality data has not been verified yet.