{{ config(materialized='table') }}

select 
    temperature_degC as temp_C,
    ROUND( temperature_degC * (9/5) + 32, 1) as temp_F,
    DATETIME(timestamp(DATE), 'UTC') as observation_time_UTC,
    DATETIME(timestamp(DATE), 'America/Denver') as observation_time_MTN

from {{ ref('union_weather_station') }}

WHERE temperature_QC in ('1', '5', '9') AND
      temperature_degC IS NOT NULL

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}