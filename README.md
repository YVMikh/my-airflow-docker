# Climate and Air Quality Data Pipeline — Almaty

[![CI](https://github.com/YVMikh/my-airflow-docker/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/YVMikh/my-airflow-docker/actions/workflows/python-app.yml)

## Project Overview

This Data Engineering project collects, consolidates, and transforms air quality and weather data for Almaty, Kazakhstan, from publicly available sources.

The project implements an automated, multi-layer ELT pipeline. Airflow loads raw observations from external sources into PostgreSQL, and dbt Core transforms them into analytical datasets. Load monitoring and data quality checks are written to a dedicated `monitoring` schema. The whole stack runs locally in Docker Compose.

**Project status:** Active development. Ingestion, monitoring, staging, intermediate models, dbt tests, and automated CI checks are implemented. The marts layer and BI dashboards are planned.

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
| GitHub Actions | Continuous integration |
| Flake8 / SQLFluff | Python and SQL linting |
| pytest | Python unit testing |

## Data Sources

| Source | Content | Coverage |
|---|---|---|
| [AirData.kz](https://airdata.kz) ([GitHub archives](https://github.com/qazybekb/AirDatakz-OpenData), `csv.gz`) | 9 pollutants: PM2.5, PM10, NO2, NO, SO2, CO, O3, H2S, TSP | Station-level observations, grouped into geographical clusters |
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

Data quality is checked at two levels: dbt tests on the transformed layers and raw-layer checks in Airflow (see Monitoring). Python unit tests complement these checks by validating ingestion logic independently of the database.

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

### Python Unit Tests

The initial test suite (`tests/test_weather_metar.py`) contains 14 test cases for the METAR ingestion logic:

- Time interval splitting, including partial intervals, empty periods, and leap-year boundaries.
- CSV parsing, including empty and invalid responses, missing required columns, and invalid values.
- Data cleaning, including filtering, sorting, and validation of the requested time range.

The tests run without a live PostgreSQL database, an Airflow scheduler, or external data API calls. All 14 cases have passed locally in Docker and in GitHub Actions.

## Continuous Integration (CI)

GitHub Actions runs automated checks on pushes to `main` and pull requests targeting `main`. The workflow (`.github/workflows/python-app.yml`) uses two parallel jobs with separate Python 3.12 environments to avoid dependency conflicts between Airflow and dbt.

| Job | Check | Purpose |
|---|---|---|
| `check-all` | Flake8 (critical checks) | Detect syntax errors and selected critical Python issues |
| `check-all` | Flake8 (code quality) | Report style and complexity issues (currently non-blocking) |
| `check-all` | Docker Compose validation | Validate configuration using placeholder environment values |
| `check-all` | SQLFluff | Lint dbt SQL models using the PostgreSQL dialect and Jinja templating |
| `check-all` | `dbt parse` | Validate dbt project structure and model dependencies without a database connection |
| `python-tests` | Dependency validation | Install Airflow 2.10.5 with official constraints and check dependencies with `pip check` |
| `python-tests` | pytest | Run Python unit tests; fail the job if tests fail or none are collected |

Test dependencies are defined in `requirements-test.txt`, with pytest 8.3.5 installed separately by the workflow. Both jobs run on GitHub-hosted Linux runners.

CI currently validates code, configuration, and selected ingestion logic. It does not execute dbt models or data tests against PostgreSQL, start the full Docker Compose stack, or deploy the application automatically.

## Additional Documentation

- [Findings from weather data](docs/weather_data_findings.md): comparison of weather models with airport observations and notes on METAR data quality.

## Project Structure

```
.
├── .github/workflows/python-app.yml  # CI workflow
├── .flake8                           # Python lint configuration
├── airflow/dags/
│   ├── almaty_air_raw_sync.py
│   ├── almaty_weather_metar_sync.py
│   └── almaty_weather_openmeteo_sync.py
├── climate_data_dbt/
│   ├── .sqlfluff                     # SQL lint configuration
│   ├── macros/stg_air_quality_hourly.sql
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/air_quality/      # 9 pollutant models
│   │   ├── staging/weather/          # Open-Meteo and METAR
│   │   └── intermediate/
│   └── tests/
│       ├── generic/generic_tests.sql # custom generic tests
│       └── *.sql                     # singular tests
├── tests/
│   └── test_weather_metar.py         # Python unit tests
├── docs/                             # weather data findings, Airflow configuration screenshot, draft architecture files (not yet updated)
├── superset/                         # Superset configuration for the local environment
├── docker-compose.yaml
├── .env.example
├── environment.yml
├── requirements-test.txt             # isolated Airflow test dependencies
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
- Extend Python unit tests to Open-Meteo and air quality ingestion; add PostgreSQL integration tests.

**BI**

- Apache Superset dashboards for air quality and climate trends, including a data quality dashboard based on the monitoring schema.

**Technical debt**

- Reduce cyclomatic complexity in the air quality and METAR ingestion DAGs, then make the full Flake8 code quality check blocking.
- Review SQLFluff rule exceptions as the dbt models evolve.

- Drop leftover legacy database objects (`almaty_hourly_weather`, `stg_almaty_weather`) if they still exist.
- Keep a single dependency source (`environment.yml` or `requirements.txt`) and drop `requests-cache` from `_PIP_ADDITIONAL_REQUIREMENTS`.

## Data Sources and Licensing

This project is developed for non-commercial and educational purposes. Source datasets are retrieved by the pipeline at runtime and are not redistributed in this repository.

### Air Quality Data

- **Provider:** [AirData.kz](https://airdata.kz) — Open Air Quality Dataset for Kazakhstan, Global Shapers Almaty Hub.
- **Data repository:** [AirDatakz-OpenData](https://github.com/qazybekb/AirDatakz-OpenData).
- **Licensing terms:** [AirDatakz-OpenData — License](https://github.com/qazybekb/AirDatakz-OpenData/blob/main/README.md#license).
- **License:** [Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0)](https://creativecommons.org/licenses/by-nc/4.0/).
- **Upstream sources:** According to the original dataset documentation, upstream source terms (KazHydroMet, OpenAQ, WAQI) also apply.

The source data are filtered, aggregated by geographical cluster and hour, and transformed into analytical datasets.

### Weather Data — Open-Meteo

- **Provider:** [Open-Meteo](https://open-meteo.com/).
- **Data source:** [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api).
- **Models:** `ecmwf_ifs`, `era5`, `era5_land`.
- **Terms of use:** [Open-Meteo Terms of Service](https://open-meteo.com/en/terms).
- **Data license:** [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
- **Underlying data providers:** [ECMWF](https://www.ecmwf.int/) and [Copernicus Climate Change Service (C3S)](https://climate.copernicus.eu/).

The free API is used for non-commercial purposes. Source data are processed, transformed and integrated with other data sources. Changes have been made to the original data.

### Airport Weather Observations — METAR

- **Data access provider:** [Iowa Environmental Mesonet (IEM), Iowa State University](https://mesonet.agron.iastate.edu/).
- **Data source:** Historical METAR observations from Almaty International Airport (UAAA).
- **Data archive:** [IEM ASOS/AWOS/METAR Archive](https://mesonet.agron.iastate.edu/request/download.phtml).
- **Dataset documentation:** [IEM Datasets](https://mesonet.agron.iastate.edu/info/datasets/).
- **Data usage status:** Public Domain, according to the IEM usage policy.
- **Terms of use:** [IEM Disclaimer — Usage of IEM Products](https://mesonet1.agron.iastate.edu/disclaimer.php).

Attribution to the Iowa Environmental Mesonet of Iowa State University is appreciated.
The source observations are parsed, validated, cleaned and transformed for integration with hourly weather and air quality data.

## Known Limitations

- Air quality data are subject to non-commercial use restrictions under CC BY-NC 4.0 and applicable upstream source terms.
- The free Open-Meteo API is used for non-commercial purposes.
- Individual upstream data provider terms have not been independently verified.
- Public redistribution of combined datasets requires a review of applicable source data usage terms.
