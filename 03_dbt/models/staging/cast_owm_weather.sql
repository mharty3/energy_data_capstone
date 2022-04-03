{{ config(materialized='table') }}

select 
    temp as temp_C,
    ROUND( temp * (9/5) + 32, 1) as temp_F,
    DATETIME(timestamp(timestamp), 'UTC') as observation_time_UTC,
    DATETIME(timestamp(timestamp), 'America/Denver') as observation_time_MTN

from {{ source('staging', 'hourly_updated_weather') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}