-- Daily weather metrics per city: moving averages, temperature anomaly,
-- rolling rainfall and dry-spell length.
{% set dry_day_threshold_mm = 1.0 %}

with daily as (
    select
        *,
        iff(precipitation_mm < {{ dry_day_threshold_mm }}, 1, 0) as is_dry_day
    from {{ ref('stg_weather_daily') }}
),

windows as (
    select
        *,

        -- Moving averages of daily mean temperature
        avg(temperature_mean) over (partition by city order by weather_date
            rows between 2 preceding and current row)            as temp_ma_3d,
        avg(temperature_mean) over (partition by city order by weather_date
            rows between 6 preceding and current row)            as temp_ma_7d,
        avg(temperature_mean) over (partition by city order by weather_date
            rows between 29 preceding and current row)           as temp_ma_30d,

        -- Baseline for the anomaly: the 30 days BEFORE today (today excluded)
        avg(temperature_mean) over (partition by city order by weather_date
            rows between 30 preceding and 1 preceding)           as temp_baseline_30d,
        stddev(temperature_mean) over (partition by city order by weather_date
            rows between 30 preceding and 1 preceding)           as temp_stddev_30d,
        count(temperature_mean) over (partition by city order by weather_date
            rows between 30 preceding and 1 preceding)           as baseline_days,

        -- Rolling rainfall totals
        sum(precipitation_mm) over (partition by city order by weather_date
            rows between 6 preceding and current row)            as rainfall_7d_mm,
        sum(precipitation_mm) over (partition by city order by weather_date
            rows between 29 preceding and current row)           as rainfall_30d_mm,

        -- Every wet day starts a new group; dry days after it belong to that group
        sum(1 - is_dry_day) over (partition by city order by weather_date
            rows between unbounded preceding and current row)    as wet_day_group
    from daily
)

select
    weather_id,
    city,
    latitude,
    longitude,
    weather_date,
    is_forecast,
    iff(is_forecast, 'Forecast', 'Observed')                     as data_type,

    temperature_max,
    temperature_min,
    temperature_mean,
    temperature_max - temperature_min                            as diurnal_range,
    precipitation_mm,
    precipitation_hours,
    wind_speed_max_kmh,

    round(temp_ma_3d, 2)                                         as temp_ma_3d,
    round(temp_ma_7d, 2)                                         as temp_ma_7d,
    round(temp_ma_30d, 2)                                        as temp_ma_30d,

    -- Temperature anomaly: how far today is from the recent normal
    -- (only once there are at least 14 days of history to compare against)
    iff(baseline_days >= 14,
        round(temperature_mean - temp_baseline_30d, 2), null)    as temp_anomaly,
    iff(baseline_days >= 14 and temp_stddev_30d > 0,
        round((temperature_mean - temp_baseline_30d) / temp_stddev_30d, 2),
        null)                                                    as temp_anomaly_zscore,
    case
        when baseline_days < 14 then null
        when temperature_mean - temp_baseline_30d >=  3 then 'Much warmer than normal'
        when temperature_mean - temp_baseline_30d >=  1 then 'Warmer than normal'
        when temperature_mean - temp_baseline_30d <= -3 then 'Much cooler than normal'
        when temperature_mean - temp_baseline_30d <= -1 then 'Cooler than normal'
        else 'Near normal'
    end                                                          as temp_anomaly_label,

    round(rainfall_7d_mm, 2)                                     as rainfall_7d_mm,
    round(rainfall_30d_mm, 2)                                    as rainfall_30d_mm,

    is_dry_day,
    -- Dry spell length: consecutive dry days up to and including this day (0 on a wet day)
    iff(is_dry_day = 1,
        sum(is_dry_day) over (partition by city, wet_day_group order by weather_date
            rows between unbounded preceding and current row),
        0)                                                       as dry_spell_length_days
from windows
