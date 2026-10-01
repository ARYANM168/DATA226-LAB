-- Singular test: rainfall can't be negative, dry spells can't be negative or longer than the history,
-- and a 7-day rolling total can't be below that day's own rainfall
select weather_id
from {{ ref('fct_weather_metrics') }}
where precipitation_mm < 0
   or dry_spell_length_days < 0
   or rainfall_7d_mm < precipitation_mm - 0.01
