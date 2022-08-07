  {{ config(materialized='view') }}
  with 
  forecasts as (
    select 
        DATETIME(energy_timestamp_mtn, "America/Denver") as energy_timestamp_mtn,
        predicted_energy_demand,
        temp_f_forecast,
        model_version,
        DATETIME(prediction_start_date, "America/Denver") as prediction_start_date,
        prediction_creation_date,
        DATETIME_DIFF(DATETIME(energy_timestamp_mtn, "America/Denver"), DATETIME(prediction_start_date, "America/Denver"), HOUR) as hours_from_pred_start

    from {{ source('staging', 'energy_demand_forecasts' ) }}
    ),

  actuals as (
    select *
    from {{ ref('joined_temp_and_demand') }}
    WHERE energy_timestamp_mtn >= (select min(energy_timestamp_mtn) from forecasts)
  )


select a.energy_timestamp_mtn,
       predicted_energy_demand,
       temp_f_forecast,
       model_version,
       prediction_start_date,
       prediction_creation_date,
       hours_from_pred_start,
       temp_timestamp_mtn,
       energy_demand,
       demand_units, 
       series_name,
       temp_F,
       date_energy_updated,
       
       energy_demand - predicted_energy_demand as energy_error,
       temp_F - temp_f_forecast as temp_f_error,
       abs(energy_demand - predicted_energy_demand) as energy_error_abs,
       abs(temp_F - temp_f_forecast) as temp_f_error_abs,
       (energy_demand - predicted_energy_demand) / energy_demand as energy_error_pct,
       (temp_F - temp_f_forecast) / temp_F as temp_f_error_pct,
       abs((energy_demand - predicted_energy_demand) / energy_demand) as energy_error_abs_pct,
       abs((temp_F - temp_f_forecast) / temp_F) as temp_f_error_abs_pct

from forecasts f 
left join actuals a
ON f.energy_timestamp_mtn = a.energy_timestamp_mtn


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}
  limit 100

{% endif %}