# Climate and Air Quality Data Pipeline — Almaty

Project Overview
This personal Data Engineering project focuses on collecting and consolidating weather and air quality data for Almaty, Kazakhstan, from publicly available sources.
The goal is to build an automated ETL pipeline that retrieves data from external sources, stores it in a centralized PostgreSQL database, and monitors data completeness.
The long-term objective is to develop an analytical data mart for exploring climate and air quality trends and visualizing the collected data.
Project status: In development.

## Objectives

- Automate data collection from public weather and air quality sources.
- Consolidate data from external sources in a centralized PostgreSQL database.
- Organize daily data ingestion and updates.
- Detect missing observations by comparing API responses with existing database records and checking expected dates or time intervals.
- Develop configurable data quality checks.
- Design an analytical data model and data mart for future analysis and visualization.

## Pipeline description
The project is being developed iteratively, with the following workflow:

1. Data ingestion — retrieve weather and air quality data from external sources through REST APIs.
2. Data processing — parse JSON responses and prepare the extracted data for storage.
3. Data storage — store collected data in PostgreSQL.
4. Daily updates — schedule recurring ingestion jobs.
5. Completeness checks — compare API responses with existing database records and identify missing dates or time intervals.
6. Data quality validation — develop configurable checks for data completeness, consistency, and validity.
7. Analytical data mart (planned) — organize validated data for analytical queries and visualization.

## Data Quality

Data completeness and reliability are key goals of the project.
Current development focuses on identifying missing observations through source-response comparisons and checks against existing database records.
Planned improvements include:
 - Configurable data validation rules.
 - not_null tests for required fields.
 - unique tests for selected keys.
 - Additional checks for data consistency and valid value ranges.
These tests have not yet been implemented.

## Technology Stack

Python — data extraction and processing.
REST APIs / JSON — external data access and data exchange.
Apache Airflow — workflow orchestration.
PostgreSQL — centralized data storage.
dbt Core — planned data transformation and testing.
Docker Compose — local development environment configuration.
Git — version control.

## Planned Architecture

The target architecture will introduce structured data layers and an analytical data mart:
  Staging — initial storage and preparation of source data.
  Core — standardized and consolidated data.
  Data Mart — analytical datasets for queries and visualization.
These layers and the analytical data mart have not yet been implemented.

## Future Development

- Implement and validate the daily ingestion workflow.
- Expand data completeness checks and introduce configurable quality tests.
- Develop the Staging and Core data layers in PostgreSQL.
- Integrate dbt Core for data transformations and automated testing.
- Build an analytical data mart for combined weather and air quality analysis.
- Add a visualization layer to explore trends in Almaty.

## Project Status

The project is under active development. The current focus is on data ingestion, centralized storage, daily updates, and missing-data detection.
The Docker Compose configuration is prepared. The structured data layers, automated dbt tests, analytical data mart, and visualization layer remain planned development stages.

That`s all