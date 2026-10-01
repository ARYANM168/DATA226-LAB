#!/usr/bin/env bash
# Creates the Airflow Connection + Variables used by the DAGs. Run once after `docker compose up -d`:
#   docker compose exec airflow bash /opt/airflow/scripts/setup_airflow_config.sh
# Values come from .env (loaded into the container). Nothing secret is stored in the repo.
set -euo pipefail

airflow connections delete snowflake_default >/dev/null 2>&1 || true
airflow connections add snowflake_default \
  --conn-type snowflake \
  --conn-login "$SNOWFLAKE_USER" \
  --conn-password "$SNOWFLAKE_PRIVATE_KEY_PASSPHRASE" \
  --conn-schema "$SNOWFLAKE_RAW_SCHEMA" \
  --conn-extra "{\"account\": \"$SNOWFLAKE_ACCOUNT\", \"warehouse\": \"$SNOWFLAKE_WAREHOUSE\", \"database\": \"$SNOWFLAKE_DATABASE\", \"role\": \"$SNOWFLAKE_ROLE\", \"private_key_file\": \"$SNOWFLAKE_PRIVATE_KEY_PATH\"}"

airflow variables set weather_cities '[{"city":"San Jose","latitude":37.3382,"longitude":-121.8863},{"city":"New York","latitude":40.7128,"longitude":-74.0060}]'
airflow variables set weather_api_url "https://api.open-meteo.com/v1/forecast"
airflow variables set weather_past_days 92
airflow variables set weather_forecast_days 16
echo "Connection snowflake_default and 4 variables created."
