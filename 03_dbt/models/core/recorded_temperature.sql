{{ config(materialized='table') }}

{% set models =  ['cast_isd_weather_station_data', 'cast_owm_weather'] %}

{% for model in models %}
  select 
      *,
  from {{ ref(model) }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %} 


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}