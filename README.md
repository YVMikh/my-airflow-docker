# Climate and Air Quality Data Pipeline — Almaty

##  Project Overview

This Data Engineering project focuses on collecting, consolidating, and transforming weather and air quality data for Almaty, Kazakhstan, from publicly available sources.
The project implements an automated, multi-layer ELT pipeline that retrieves data from external sources, stores raw observations in a centralized PostgreSQL database, and transforms them into analytical datasets using dbt Core.
The pipeline is orchestrated with Apache Airflow and runs in a Docker Compose environment.
Project Status: Active Development — Core Ingestion and Transformation Pipeline Implemented.

## Objectives

- Automate data collection from public weather and air quality sources.
- Consolidate observations from multiple external sources in a centralized PostgreSQL database.
- Implement scheduled data ingestion, incremental loading, and automated update workflows.
- Deduplicate and aggregate multi-station observations into stable geographical clusters, preserving the distribution of values within each cluster (maximum, minimum, mean, and median).
- Optimize data processing and storage through layered transformations.
- Design an analytical data model for future BI visualization using Apache Superset.

## Technology Stack

Python / Pandas — data extraction, chunked file processing, and data manipulation.
Apache Airflow — workflow orchestration, task dependencies, and scheduled pipeline execution.
PostgreSQL — centralized relational database for raw and transformed datasets.
dbt Core — multi-layer data transformations, model management, and automated data testing.
Docker Compose — containerized local development environment.
Git — version control.

## 🏗 Implemented Pipeline Architecture

The project has evolved from basic data collection into a multi-layer ELT architecture consisting of raw, staging, and intermediate layers.

1. Data Ingestion — Airflow Layer

Memory-Efficient Ingestion: Airflow downloads compressed .csv.gz files from GitHub in 16 KB chunks and temporarily stores them on disk, reducing memory consumption during ingestion.
Incremental Loading: Airflow uses the maximum available measured_at timestamp from the dbt staging layer as a loading watermark to identify new records and reduce duplicate processing.
Automated Orchestration: Airflow manages ingestion tasks, dependencies, and scheduled pipeline execution.
Centralized Storage: Raw observations from external sources are loaded into PostgreSQL for subsequent transformations.

2. Staging Layer — dbt Core

Configured as database VIEWs in the dbt_dev_staging schema.
Filters out invalid and incomplete records, including observations with NULL measurement values (WHERE value_ugm3 IS NOT NULL).
Geographical Cluster Aggregation: Consolidates observations from multiple independent sensors located within the same geographical cluster.
Hourly Statistical Aggregation: Timestamps are truncated to hourly intervals using date_trunc, and for each pollutant and geographical cluster the following statistics of value_ugm3 are calculated at the moment of measurement:
- MAX — peak recorded concentration;
- MIN — lowest recorded concentration;
- AVG — mean concentration across the cluster's sensors;
- median — calculated with percentile_cont(0.5), which is robust to outliers from individual sensors.
Preserving the full set of statistics (instead of a single maximum value) retains information about the spread of readings within a cluster and allows the analytical layer to distinguish local spikes from cluster-wide pollution.
Reduces data volume and prevents row multiplication during subsequent JOIN operations.

3. Intermediate Layer — dbt Core

Materialized as a physical database TABLE in the dbt_dev_intermediate schema.
Dynamically processes 9 pollutant metrics (PM2.5, PM10, SO2, H2S, NO2, NO, O3, TSP, CO) using Jinja loops to generate CTEs and build a single wide, de-normalized analytical table. For each pollutant, the table contains four statistics per cluster and hour: maximum, minimum, mean, and median.
Performance Optimization: Identified and resolved a data fan-out issue during JOIN operations. By moving aggregation logic to the Staging layer, the intermediate dataset was reduced from an 8 GB JOIN result to a compact analytical table — approximately a 340x reduction in data volume (measured before adding the additional statistics).
This optimization significantly reduced the size of the transformed dataset and improved query performance in database clients.

📊 Data Quality and Validation

Data completeness and consistency are checked through dbt tests and Airflow task-level validation.
Composite Key Validation: dbt tests verify that the combination of measured_at and cluster_id is unique and contains no NULL values.
Staging Data Validation: Staging models filter out records with missing measurement values before further transformations.
Graceful Initialization: Airflow handles the absence of the staging view during initial deployment by applying a configurable default ingestion watermark and logging the fallback event.
Pipeline Error Handling: Python try-except blocks provide controlled exception handling during ingestion and database operations.

🚀 How to Run the Project

1. Start the Environment

Run Docker Compose from the project root directory:
docker-compose up -d
Check the status of the containers:
docker-compose ps

2. Trigger the Ingestion DAG

Open the Airflow Web UI and unpause the following DAG:
almaty_airdata_all_in_one_update
The DAG sequentially retrieves, processes, and loads new air quality observations into PostgreSQL.

3. Run dbt Transformations
Navigate to the dbt project directory and execute:

# Clean previous dbt build artifacts
dbt clean

# Build staging views (data filtering and hourly aggregation)
dbt run --select path:models/staging

# Build the intermediate analytical table
dbt run --select int_almaty_air_quality

# Run data quality tests
dbt test --select int_almaty_air_quality

Note: Make sure the dbt profile is configured to connect to the PostgreSQL database used by the project.

🔮 Future Development

Weather Data Integration: Integrate the almaty_hourly_weather dataset into the intermediate analytical model to correlate air pollution levels with temperature, wind speed, and humidity.
Data Mart Layer: Develop a dedicated marts layer with business-oriented analytical models, including rolling 24-hour averages and daily Air Quality Index (AQI) calculations.
BI Visualization: Connect Apache Superset to the future data marts and develop interactive dashboards for exploring air quality and climate trends in Almaty.
Data Quality Improvements: Expand dbt test coverage and improve incremental loading logic to handle late-arriving and updated source records.