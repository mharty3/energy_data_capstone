--https://discourse.getdbt.com/t/unioning-identically-structured-data-sources/921
{{ config(materialized='table') }}

{% set models =  ['isd', 'owm'] %}

{% for model in models %}
  select 
      *,
      '{{ model }}' as source
  from {{ ref('cast_'~model~'_weather') }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %} 


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}