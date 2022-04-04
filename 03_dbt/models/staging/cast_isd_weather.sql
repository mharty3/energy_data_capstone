{{ config(materialized='table') }}

with isd as ( 
  SELECT
    -- CAST( CONCAT(USAF, WBAN) AS int) as station_id,
    CONCAT(USAF, WBAN) as station_id,
    LAT,
    LON
  FROM {{ ref('isd_stations') }}
)


select
    isd.LAT as lat,
    isd.LON as lon,
    obs.temperature_degC as temp_C,
    ROUND( obs.temperature_degC * (9/5) + 32, 1) as temp_F,
    DATETIME(timestamp(obs.DATE), 'UTC') as observation_time_UTC,
    DATETIME(timestamp(obs.DATE), 'America/Denver') as observation_time_MTN

from {{ ref('union_weather_station') }} obs
join isd
on CAST(obs.STATION AS STRING) = isd.station_id 

WHERE obs.temperature_QC in ('1', '5', '9') AND
      obs.temperature_degC IS NOT NULL

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}