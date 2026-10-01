-- Singular test: fails if any row has min temperature above max temperature
select weather_id, temperature_min, temperature_max
from {{ ref('stg_weather_daily') }}
where temperature_min > temperature_max
