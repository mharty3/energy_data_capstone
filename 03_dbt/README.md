# dbt Tranformation in the Warehouse

Tables within the data warehouse were optimized by not partitioning or clustering. Tables less than 1GB should not be clustered or partitioned due to the overhead created, and my fact tables are under 1GB.

Look inside the [models directory](03_dbt/models) to see the transformations I applied with dbt.

# dbt transformation model for temperature data
![](../img/dbt_temp.PNG)

# dbt transformation for actual energy demand
![](../img/dbt_demand.PNG)

# dbt transformation for forecasted energy demand
![](../img/dbt_demand_forecast.PNG)