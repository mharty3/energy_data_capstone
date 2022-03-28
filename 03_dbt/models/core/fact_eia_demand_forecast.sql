{{ config(materialized='table') }}

select 

d.series_id,
d.timestamp,
d.value,
md.units,
md.name,
md.updated
		
from {{ source('staging', 'demand_data_native') }} d 
join {{ source('staging', 'demand_metadata_native') }} md
on d.series_id = md.series_id

where d.series_id = 'EBA_PSCO-ALL_DF_H'
order by d.timestamp
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}