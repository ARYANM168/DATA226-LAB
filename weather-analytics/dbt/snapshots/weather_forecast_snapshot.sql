{# SCD Type 2 history of every city/day. The ETL MERGE overwrites forecast values on each run,
   so this snapshot is what preserves how a forecast changed over time and when a day flipped
   from Forecast to Observed. `check` strategy because ingested_at changes on every load. #}
{% snapshot weather_forecast_snapshot %}

{{
    config(
        target_schema=target.schema,
        unique_key='weather_id',
        strategy='check',
        check_cols=['temperature_max', 'temperature_min', 'temperature_mean',
                    'precipitation_mm', 'wind_speed_max_kmh', 'is_forecast']
    )
}}

select * from {{ ref('stg_weather_daily') }}

{% endsnapshot %}
