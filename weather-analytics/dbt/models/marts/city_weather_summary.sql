-- One row per city: headline numbers for the dashboard's KPI tiles and comparison table
with m as (
    select * from {{ ref('fct_weather_metrics') }}
),

observed as (
    select * from m where not is_forecast
),

latest as (
    select *
    from observed
    qualify row_number() over (partition by city order by weather_date desc) = 1
),

last_30 as (
    select
        o.city,
        round(avg(o.temperature_mean), 2)    as avg_temp_30d,
        max(o.temperature_max)               as max_temp_30d,
        min(o.temperature_min)               as min_temp_30d,
        round(sum(o.precipitation_mm), 2)    as total_rain_30d_mm,
        sum(o.is_dry_day)                    as dry_days_30d,
        max(o.dry_spell_length_days)         as longest_dry_spell_30d
    from observed o
    join latest l on o.city = l.city
    where o.weather_date > dateadd(day, -30, l.weather_date)
    group by o.city
),

forecast as (
    select
        city,
        round(avg(temperature_mean), 2)      as forecast_avg_temp,
        round(sum(precipitation_mm), 2)      as forecast_total_rain_mm,
        max(temperature_max)                 as forecast_max_temp
    from m
    where is_forecast
    group by city
)

select
    l.city,
    l.weather_date                           as latest_observed_date,
    l.temperature_mean                       as latest_temp_mean,
    l.temp_ma_7d                             as latest_temp_ma_7d,
    l.temp_anomaly                           as latest_temp_anomaly,
    l.temp_anomaly_label                     as latest_temp_anomaly_label,
    l.dry_spell_length_days                  as current_dry_spell_days,
    s.avg_temp_30d,
    s.max_temp_30d,
    s.min_temp_30d,
    s.total_rain_30d_mm,
    s.dry_days_30d,
    s.longest_dry_spell_30d,
    f.forecast_avg_temp,
    f.forecast_max_temp,
    f.forecast_total_rain_mm
from latest l
left join last_30 s on l.city = s.city
left join forecast f on l.city = f.city
