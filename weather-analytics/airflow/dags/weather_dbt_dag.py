"""
DAG 2 of 2: weather_dbt_transform

  dbt_run -> dbt_test -> dbt_snapshot

Scheduled on the RAW_DATASET that the ETL DAG (weather_etl) updates, so it starts
automatically right after each successful ETL load - no clock-based guessing.
"""
from datetime import timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

RAW_DATASET = Dataset("snowflake://WEATHER_DB/RAW/WEATHER_DAILY")
DBT = "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt {cmd} --profiles-dir /opt/airflow/dbt"


@dag(
    dag_id="weather_dbt_transform",
    schedule=[RAW_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["weather", "dbt"],
)
def weather_dbt_transform():
    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT.format(cmd="run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT.format(cmd="test"))
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=DBT.format(cmd="snapshot"))

    dbt_run >> dbt_test >> dbt_snapshot


weather_dbt_transform()
