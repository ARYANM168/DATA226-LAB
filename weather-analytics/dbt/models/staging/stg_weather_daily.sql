-- One clean row per city per day
with source as (
    select * from {{ source('raw', 'weather_daily') }}
    -- drop days where the API returned no temperature data at all
    where temperature_mean is not null
       or (temperature_max is not null and temperature_min is not null)
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by city, weather_date order by ingested_at desc
    ) = 1
)

select
    city || '|' || to_varchar(weather_date)                   as weather_id,
    city,
    latitude,
    longitude,
    weather_date,
    temperature_max,
    temperature_min,
    -- fall back to the midpoint if the API ever omits the mean
    coalesce(temperature_mean, (temperature_max + temperature_min) / 2) as temperature_mean,
    coalesce(precipitation_sum, 0)                           as precipitation_mm,
    coalesce(precipitation_hours, 0)                         as precipitation_hours,
    wind_speed_max                                           as wind_speed_max_kmh,
    is_forecast,
    ingested_at
from deduped
