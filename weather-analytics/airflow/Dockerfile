FROM apache/airflow:2.10.5-python3.11
# Pre-create the SQLite folder so the Docker volume is owned by the airflow user
RUN mkdir -p /opt/airflow/db
# Snowflake provider (SnowflakeHook + Connection type) and the Python connector, installed with
# Airflow's constraints so they can't break Airflow itself
RUN pip install --no-cache-dir \
      apache-airflow-providers-snowflake snowflake-connector-python requests \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-snowflake>=1.9,<2"
