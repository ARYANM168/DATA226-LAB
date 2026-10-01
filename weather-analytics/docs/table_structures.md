# Table structures

Snowflake does not enforce PRIMARY KEY / UNIQUE; they are declared for documentation and enforced by the
MERGE (RAW) and dbt `unique` tests (models). Verify the exact types with `DESCRIBE TABLE <name>;` before
pasting into your report.

## RAW.WEATHER_DAILY (Airflow, base table)
| Column | Type | Constraints | Description |
|---|---|---|---|
| city | STRING | NOT NULL, PK (part 1) | City name |
| latitude | FLOAT | | |
| longitude | FLOAT | | |
| weather_date | DATE | NOT NULL, PK (part 2) | Local calendar day |
| temperature_max / _min / _mean | FLOAT | | Daily °C |
| precipitation_sum | FLOAT | | mm |
| precipitation_hours | FLOAT | | Hours with precipitation |
| wind_speed_max | FLOAT | | km/h |
| is_forecast | BOOLEAN | | TRUE if date is after today in the city's time zone |
| ingested_at | TIMESTAMP_NTZ | | Load time (UTC) |

## ANALYTICS.STG_WEATHER_DAILY (dbt view)
| Column | Type | Constraints (dbt tests) | Description |
|---|---|---|---|
| weather_id | VARCHAR | unique, not_null | `city\|weather_date` surrogate key |
| city | VARCHAR | not_null | |
| latitude, longitude | FLOAT | | |
| weather_date | DATE | not_null | |
| temperature_max, temperature_min | FLOAT | | °C |
| temperature_mean | FLOAT | not_null | Falls back to (max+min)/2 |
| precipitation_mm | FLOAT | not_null | NULL → 0 |
| precipitation_hours | FLOAT | | NULL → 0 |
| wind_speed_max_kmh | FLOAT | | |
| is_forecast | BOOLEAN | | |
| ingested_at | TIMESTAMP_NTZ | | |

## ANALYTICS.FCT_WEATHER_METRICS (dbt table, one row per city per day)
| Column | Type | Constraints (dbt tests) | Description |
|---|---|---|---|
| weather_id | VARCHAR | unique, not_null | |
| city | VARCHAR | not_null | |
| latitude, longitude, weather_date, is_forecast | as staging | | |
| data_type | VARCHAR | accepted_values: Observed, Forecast | |
| temperature_max, temperature_min, temperature_mean | FLOAT | | °C |
| diurnal_range | FLOAT | | max − min |
| precipitation_mm, precipitation_hours, wind_speed_max_kmh | FLOAT | | |
| temp_ma_3d, temp_ma_7d, temp_ma_30d | FLOAT | | Moving averages of mean temp |
| temp_anomaly | FLOAT | NULL until 14 days of history | Mean temp − avg of previous 30 days |
| temp_anomaly_zscore | FLOAT | | Anomaly / 30-day stddev |
| temp_anomaly_label | VARCHAR | | Much warmer … Much cooler |
| rainfall_7d_mm, rainfall_30d_mm | FLOAT | | Rolling rainfall |
| is_dry_day | NUMBER | | 1 if precipitation < 1 mm |
| dry_spell_length_days | NUMBER | not_null | Consecutive dry days up to this day |

## ANALYTICS.CITY_WEATHER_SUMMARY (dbt table, one row per city)
| Column | Type | Constraints | Description |
|---|---|---|---|
| city | VARCHAR | unique, not_null | |
| latest_observed_date | DATE | | |
| latest_temp_mean, latest_temp_ma_7d, latest_temp_anomaly | FLOAT | | |
| latest_temp_anomaly_label | VARCHAR | | |
| current_dry_spell_days | NUMBER | | |
| avg_temp_30d, max_temp_30d, min_temp_30d, total_rain_30d_mm | FLOAT | | Last 30 observed days |
| dry_days_30d, longest_dry_spell_30d | NUMBER | | |
| forecast_avg_temp, forecast_max_temp, forecast_total_rain_mm | FLOAT | | Next 16 forecast days |

## ANALYTICS.WEATHER_FORECAST_SNAPSHOT (dbt snapshot, SCD Type 2)
All columns of STG_WEATHER_DAILY plus:
| Column | Type | Description |
|---|---|---|
| dbt_scd_id | VARCHAR | Unique id of the version row |
| dbt_updated_at | TIMESTAMP | When dbt recorded the change |
| dbt_valid_from | TIMESTAMP | Version start |
| dbt_valid_to | TIMESTAMP | Version end (NULL = current) |
