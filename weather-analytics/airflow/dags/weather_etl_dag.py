"""
DAG 1 of 2: weather_etl

  create_raw_table -> extract_load_weather  --(Dataset update)-->  weather_dbt_transform

* Extracts daily weather for each city from Open-Meteo (https://api.open-meteo.com/v1/forecast)
* Loads it into WEATHER_DB.RAW.WEATHER_DAILY inside ONE SQL transaction (BEGIN / COMMIT, ROLLBACK + raise on error)
  using MERGE, so re-running the DAG never creates duplicates (idempotent).

Airflow Connection : snowflake_default            (Admin > Connections)
Airflow Variables  : weather_cities, weather_api_url, weather_past_days, weather_forecast_days (Admin > Variables)
"""
from datetime import datetime, timedelta

import pendulum
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_default"
RAW_TABLE = "WEATHER_DAILY"
# Updating this Dataset is what triggers the dbt DAG after the ETL finishes
RAW_DATASET = Dataset("snowflake://WEATHER_DB/RAW/WEATHER_DAILY")

DEFAULT_CITIES = [
    {"city": "San Jose", "latitude": 37.3382, "longitude": -121.8863},
    {"city": "New York", "latitude": 40.7128, "longitude": -74.0060},
]
DAILY_VARS = [
    "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
    "precipitation_sum", "precipitation_hours", "wind_speed_10m_max",
]


@dag(
    dag_id="weather_etl",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["weather", "etl", "snowflake"],
)
def weather_etl():
    @task
    def create_raw_table():
        conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
        try:
            conn.cursor().execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
                    city                 STRING     NOT NULL,
                    latitude             FLOAT,
                    longitude            FLOAT,
                    weather_date         DATE       NOT NULL,
                    temperature_max      FLOAT,
                    temperature_min      FLOAT,
                    temperature_mean     FLOAT,
                    precipitation_sum    FLOAT,
                    precipitation_hours  FLOAT,
                    wind_speed_max       FLOAT,
                    is_forecast          BOOLEAN,
                    ingested_at          TIMESTAMP_NTZ,
                    PRIMARY KEY (city, weather_date)   -- informational in Snowflake; enforced by the MERGE below
                )
                """
            )
        finally:
            conn.close()

    @task(outlets=[RAW_DATASET])
    def extract_load_weather():
        # Variables are read inside the task (not at parse time) so DAG parsing never hits the metadata DB
        cities = Variable.get("weather_cities", default_var=DEFAULT_CITIES, deserialize_json=True)
        api_url = Variable.get("weather_api_url", default_var="https://api.open-meteo.com/v1/forecast")
        past_days = int(Variable.get("weather_past_days", default_var=92))
        forecast_days = int(Variable.get("weather_forecast_days", default_var=16))

        now = datetime.utcnow().replace(microsecond=0)
        rows = []
        for c in cities:
            resp = requests.get(
                api_url,
                params={
                    "latitude": c["latitude"],
                    "longitude": c["longitude"],
                    "daily": ",".join(DAILY_VARS),
                    "past_days": past_days,
                    "forecast_days": forecast_days,
                    "timezone": "auto",
                    "temperature_unit": "celsius",
                    "precipitation_unit": "mm",
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            d = payload["daily"]
            # "today" in the city's own time zone: later dates are forecasts
            local_today = (now + timedelta(seconds=payload.get("utc_offset_seconds", 0))).date()
            for i, day in enumerate(d["time"]):
                day_date = datetime.strptime(day, "%Y-%m-%d").date()
                rows.append((
                    c["city"], c["latitude"], c["longitude"], day,
                    d["temperature_2m_max"][i], d["temperature_2m_min"][i],
                    d["temperature_2m_mean"][i], d["precipitation_sum"][i],
                    d["precipitation_hours"][i], d["wind_speed_10m_max"][i],
                    day_date > local_today, now,
                ))
            print(f"{c['city']}: {len(d['time'])} days fetched")

        conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
        cur = conn.cursor()
        try:
            # Temp-table DDL goes BEFORE the transaction so no DDL can implicitly commit mid-transaction
            cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {RAW_TABLE}_STAGE LIKE {RAW_TABLE}")
            cur.execute("BEGIN")
            cur.executemany(
                f"INSERT INTO {RAW_TABLE}_STAGE VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                rows,
            )
            cur.execute(
                f"""
                MERGE INTO {RAW_TABLE} t
                USING {RAW_TABLE}_STAGE s
                  ON t.city = s.city AND t.weather_date = s.weather_date
                WHEN MATCHED THEN UPDATE SET
                  latitude = s.latitude, longitude = s.longitude,
                  temperature_max = s.temperature_max, temperature_min = s.temperature_min,
                  temperature_mean = s.temperature_mean,
                  precipitation_sum = s.precipitation_sum,
                  precipitation_hours = s.precipitation_hours,
                  wind_speed_max = s.wind_speed_max,
                  is_forecast = s.is_forecast, ingested_at = s.ingested_at
                WHEN NOT MATCHED THEN INSERT VALUES (
                  s.city, s.latitude, s.longitude, s.weather_date, s.temperature_max,
                  s.temperature_min, s.temperature_mean, s.precipitation_sum,
                  s.precipitation_hours, s.wind_speed_max, s.is_forecast, s.ingested_at)
                """
            )
            cur.execute("COMMIT")
            print(f"Merged {len(rows)} rows into RAW.{RAW_TABLE}")
        except Exception:
            cur.execute("ROLLBACK")  # undo the partial load, then fail the task
            raise
        finally:
            cur.close()
            conn.close()

    create_raw_table() >> extract_load_weather()


weather_etl()
