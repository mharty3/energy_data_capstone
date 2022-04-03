{% set years =  ['2022', '2021', '2020', '2019', '2018', '2017', '2016', '2015'] %}

{% for year in years %}
  select 
      *,
  from {{ source('staging', year~'_weather_station_native' ) }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %} 


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}