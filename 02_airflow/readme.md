# Batch Processing With Airflow

I used Apache Airflow to create and orchestrate the pipeline that extracts and loads the data to the data lakes and the data warehouse and to batch deploy model predictions.



## Ingest Historical Weather Data DAG
[dags/ingest_historical_weather_data_dag.py](./dags/ingest_historical_weather_data_dag.py)

![](../img/noaa_dag.PNG)

## Ingest Live Hourly Weather DAG
[dags/ingest_live_hourly_weather_dag.py](./dags/ingest_live_hourly_weather_dag.py)

![](../img/owm_dag.PNG)

## Ingest Raw Electricity DAG
[dags/ingest_raw_electricity_data_dag.py](./dags/ingest_raw_electricity_data_dag.py)

![](../img/eia_dag.PNG)

## Ingest Weather Forecast DAG
[dags/ingest_weather_forecast_dag.py](./dags/ingest_weather_forecast_dag.py)

![](../img/weather_forecast_dag.PNG)

## Batch Deploy Model Predictions DAG
[dags/batch_predict_dag.py](./dags/batch_predict_dag.py)

![](../img/batch_predict_dag.PNG)
