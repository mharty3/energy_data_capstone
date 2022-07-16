
{{ config(materialized='view') }}

SELECT 
  temp.observation_time_MTN as temp_timestamp_mtn, 
  energy.timestamp_MTN as energy_timestamp_mtn,
  energy.value as energy_demand,
  energy.units as demand_units,
  energy.name as series_name,
  temp_F,
  temp_C,
  lat as temp_location_lat,
  lon as temp_location_lon,
  source as temp_source,
  energy.updated as date_energy_updated

FROM {{ ref('fact_eia_demand_historical') }} energy
LEFT JOIN {{ ref('recorded_temperature') }} temp
  ON DATETIME_TRUNC(temp.observation_time_MTN, HOUR) = DATETIME_TRUNC(energy.timestamp_MTN, HOUR) 
  