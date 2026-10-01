# Weather Prediction Analytics Using Snowflake Airflow and dbt

## Project Overview

This project implements an automated weather analytics pipeline for San Jose, California, and New York, New York. Weather data is collected from the Open-Meteo Forecast API, loaded into Snowflake through Apache Airflow, transformed with dbt, and visualized in Tableau Public.

The project converts raw daily weather values into useful analytical metrics, including moving averages, temperature anomalies, rolling rainfall, and dry spell length.

## Problem Statement

Raw weather data contains daily temperatures, precipitation, and wind measurements, but these values are difficult to interpret directly. This project creates a repeatable pipeline that helps users compare weather conditions between two cities, identify unusual temperatures, analyze recent rainfall, and monitor dry spells.

The pipeline is designed to run automatically, support safe re-runs without duplicate records, validate transformed data, and preserve forecast history as forecasts are revised.

## Project Objectives

- Retrieve daily weather data for two cities from the Open-Meteo API.
- Load raw weather data into Snowflake.
- Orchestrate the pipeline using Apache Airflow.
- Use Airflow Connections for credentials and Airflow Variables for configuration.
- Implement idempotent loading using SQL transactions and `MERGE`.
- Transform weather data using dbt.
- Test transformed data and maintain forecast history using a dbt snapshot.
- Visualize the resulting metrics in Tableau Public.

## System Architecture

The data flow is:

```text
Open-Meteo API
      |
      v
Airflow weather_etl DAG
      |
      v
Snowflake RAW.WEATHER_DAILY
      |
      | Airflow Dataset event
      v
Airflow weather_dbt_transform DAG
      |
      v
dbt staging model, marts, tests, and snapshot
      |
      v
Snowflake ANALYTICS tables
      |
      v
Tableau Public dashboard
```

## Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Data source | Open-Meteo Forecast API | Provides historical and forecast weather data |
| Orchestration | Apache Airflow | Runs ETL and dbt workflows |
| Warehouse | Snowflake | Stores raw and transformed data |
| Transformation | dbt Core with dbt-snowflake | Cleans data, creates metrics, tests models, and snapshots changes |
| Visualization | Tableau Public | Displays weather metrics and comparisons |
| Programming language | Python 3.11 | Implements Airflow DAGs and API extraction |
| Packaging | Docker Compose | Runs the local Airflow and supporting environment |

## Cities and Data Range

| City | Latitude | Longitude |
|---|---:|---:|
| San Jose, California | 37.3382 | -121.8863 |
| New York, New York | 40.7128 | -74.0060 |

The pipeline requests 92 past days and 16 forecast days for each city. Temperature values are stored in degrees Celsius, precipitation is stored in millimeters, and the API automatically determines the local time zone for each city.

## Repository Structure

```text
weather-analytics/
|
|-- dags/
|   |-- weather_etl_dag.py
|   `-- weather_dbt_dag.py
|
|-- dbt/
|   |-- models/
|   |   |-- staging/
|   |   |   `-- stg_weather_daily.sql
|   |   `-- marts/
|   |       |-- fct_weather_metrics.sql
|   |       `-- city_weather_summary.sql
|   |-- snapshots/
|   |   `-- weather_forecast_snapshot.sql
|   |-- tests/
|   |-- dbt_project.yml
|   `-- profiles.yml.example
|
|-- snowflake/
|   `-- setup.sql
|
|-- scripts/
|   |-- generate_keys.py
|   `-- setup_airflow_config.sh
|
|-- docs/
|   |-- system_architecture.png
|   |-- airflow_dag.png
|   |-- airflow_variables.png
|   |-- dbt_commands.png
|   |-- tableau_dashboard_1.png
|   `-- tableau_dashboard_2.png
|
|-- docker-compose.yml
|-- requirements.txt
|-- .env.example
|-- .gitignore
`-- README.md
```

## Database Design

The project uses two Snowflake schemas:

- `RAW`: stores the source weather data loaded by Airflow.
- `ANALYTICS`: stores dbt staging views, analytical tables, and the forecast snapshot.

### Main Tables

| Table | Description |
|---|---|
| `RAW.WEATHER_DAILY` | Raw daily weather data for each city and date |
| `ANALYTICS.STG_WEATHER_DAILY` | Cleaned and renamed staging view |
| `ANALYTICS.FCT_WEATHER_METRICS` | Daily weather metrics for each city |
| `ANALYTICS.CITY_WEATHER_SUMMARY` | Latest and aggregated city-level metrics |
| `ANALYTICS.WEATHER_FORECAST_SNAPSHOT` | Historical versions of forecast records using SCD Type 2 |

The composite key for the raw table is `(CITY, WEATHER_DATE)`. The Airflow `MERGE` statement uses this key to update existing records and insert new records without creating duplicates.

## Weather Metrics

| Metric | Definition | dbt Column |
|---|---|---|
| Moving average temperature | Average daily mean temperature over the previous 3, 7, or 30 days | `temp_ma_3d`, `temp_ma_7d`, `temp_ma_30d` |
| Temperature anomaly | Daily mean temperature minus the average of the previous 30 days | `temp_anomaly` |
| Temperature anomaly z-score | Temperature anomaly measured in standard deviations | `temp_anomaly_zscore` |
| Rolling rainfall | Total precipitation over the previous 7 or 30 days | `rainfall_7d_mm`, `rainfall_30d_mm` |
| Dry spell length | Consecutive days with less than 1 millimeter of precipitation | `dry_spell_length_days` |
| Forecast indicator | Identifies whether a record is observed or forecast | `data_type` |

## Airflow Pipeline

### DAG 1: `weather_etl`

The ETL DAG performs the following tasks:

1. Creates the raw Snowflake table if it does not already exist.
2. Reads cities, API settings, and date ranges from Airflow Variables.
3. Retrieves weather data from Open-Meteo.
4. Loads the data into a temporary staging table.
5. Uses a transactional `MERGE` into `RAW.WEATHER_DAILY`.
6. Commits the transaction when successful.
7. Rolls back the transaction and raises the error if loading fails.
8. Updates an Airflow Dataset after a successful load.

The DAG runs daily at 06:00 UTC.

### DAG 2: `weather_dbt_transform`

The dbt DAG is triggered by the Airflow Dataset updated by `weather_etl`. It runs:

```text
dbt run -> dbt test -> dbt snapshot
```

This ensures that dbt transformations occur only after the ETL pipeline successfully updates Snowflake.

### Airflow Connections and Variables

The implementation uses:

- Airflow Connection: `snowflake_default`
- Airflow Variables:
  - `weather_cities`
  - `weather_api_url`
  - `weather_past_days`
  - `weather_forecast_days`

## dbt Project

The dbt project includes:

- A staging model that cleans and deduplicates raw records.
- A fact model that computes weather metrics.
- A city summary model for dashboard KPI values.
- Generic tests such as `unique`, `not_null`, and accepted-value tests.
- Singular tests for metric rules such as nonnegative dry spell length.
- An SCD Type 2 snapshot that tracks forecast revisions.

The dbt commands are:

```bash
dbt run
dbt test
dbt snapshot
```

## Tableau Dashboard

The Tableau dashboard compares weather trends for San Jose and New York.

### Dashboard Purpose

The dashboard helps users compare temperature trends, temperature anomalies, rolling rainfall, and dry spell length between the two cities.

### Dashboard Usage

Users can select a city and a date range. The charts update to show the selected period.

### Dashboard Dataset

The dashboard uses data from `ANALYTICS.FCT_WEATHER_METRICS`, produced by the dbt models in Snowflake.

The dashboard includes:

- Temperature and moving average trends
- Temperature anomaly charts
- Rolling rainfall charts
- Dry spell length charts
- City comparison filters
- Date range filters

## Setup Instructions

### Prerequisites

- Docker Desktop
- A Snowflake account
- Tableau Public, Tableau Desktop, or access to the provided dashboard extract

### 1. Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Update the Snowflake account and private-key passphrase in `.env`. Do not commit `.env`, private keys, or credentials to GitHub.

### 2. Build the Containers

```bash
docker compose build
```

### 3. Create the Snowflake Key Pair

```bash
docker compose run --rm airflow python /opt/airflow/scripts/generate_keys.py
```

Copy the public key printed by the command and add it to `snowflake/setup.sql`.

### 4. Configure Snowflake

Run `snowflake/setup.sql` in Snowsight. This creates the database, schemas, warehouse, role, service user, and required grants.

### 5. Start Airflow

```bash
docker compose up -d
```

Open Airflow at:

```text
http://localhost:8080
```

### 6. Create the Airflow Connection and Variables

```bash
docker compose exec airflow bash /opt/airflow/scripts/setup_airflow_config.sh
```

Verify the connection and variables in the Airflow web interface under **Admin**.

### 7. Run the Pipeline

Turn on both DAGs in Airflow and trigger `weather_etl`. After the ETL DAG succeeds, the Dataset event automatically triggers `weather_dbt_transform`.

Verify the results in Snowflake:

```sql
SELECT *
FROM WEATHER_DB.RAW.WEATHER_DAILY
ORDER BY CITY, WEATHER_DATE;

SELECT *
FROM WEATHER_DB.ANALYTICS.FCT_WEATHER_METRICS
ORDER BY CITY, WEATHER_DATE;

SELECT *
FROM WEATHER_DB.ANALYTICS.CITY_WEATHER_SUMMARY;
```

## Data Quality and Idempotency

The project uses the following safeguards:

- Airflow retries failed tasks.
- Snowflake transactions prevent partial loads.
- `ROLLBACK` is executed when an exception occurs.
- The exception is raised so Airflow marks the task as failed.
- `MERGE` prevents duplicate city-date records.
- dbt tests validate uniqueness, non-null fields, accepted values, and metric ranges.
- dbt snapshots preserve historical forecast revisions.

## Future Work

- Connect Tableau directly to Snowflake instead of using an exported extract.
- Add additional cities and regions.
- Compare weather metrics with long-term climate normals.
- Measure forecast accuracy by comparing forecasts with observed values.
- Add a machine-learning forecasting model.
- Convert the main fact model into an incremental dbt model.
- Add email or Slack alerts for failed DAGs.
- Add freshness checks for the raw weather data.
- Deploy Airflow with a production executor and metadata database.
- Add continuous integration for dbt tests and DAG linting.

## Conclusion

This project demonstrates an end-to-end weather analytics workflow. Airflow collects weather data from Open-Meteo and loads it into Snowflake using an idempotent transactional `MERGE`. A second Airflow DAG schedules the dbt transformations after the ETL process completes. dbt cleans the data, calculates analytical metrics, tests the results, and tracks forecast revisions with a snapshot. Tableau Public then presents the results through interactive city and date filters.

## References

- Open-Meteo. [Weather Forecast API Documentation](https://open-meteo.com/en/docs)
- Apache Airflow. [Documentation](https://airflow.apache.org/docs/)
- Apache Airflow. [Data-Aware Scheduling with Datasets](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
- Apache Airflow. [Snowflake Provider](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html)
- dbt Labs. [dbt Documentation](https://docs.getdbt.com/)
- dbt Labs. [Snapshots](https://docs.getdbt.com/docs/build/snapshots)
- Snowflake. [Documentation](https://docs.snowflake.com/)
- Tableau. [Tableau Public](https://www.tableau.com/products/public)

## GitHub Repository
```text
https://github.com/ARYANM168/DATA226-LAB.git
```
