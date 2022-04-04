# Batch Processing With Airflow

Airflow was used to create the pipeline that extracts and loads the data to the data lakes and the data warehouse.

Tables within the data warehouse were optimized by not partitioning or clustering. Tables less than 1GB should not be clustered or partitioned due to the overhead created, and my fact tables are under 1GB.

# Ingest Historical Weather Data DAG
![](../img/noaa_dag.PNG)

# Ingest Live Hourly Weather DAG
![](../img/owm_dag.PNG)

# Ingest Raw Electricity DAG
![](../img/eia_dag.PNG)

