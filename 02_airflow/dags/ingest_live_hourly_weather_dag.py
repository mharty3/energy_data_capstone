import os
import json
import logging
import requests
from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcloud_helpers import upload_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
OWM_API_KEY = os.environ.get("OWM_API_KEY")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = 'energy_data'
DATASET_FILE_SUFFIX = "{{ execution_date.strftime(\'%Y-%m-%d-%H\') }}"
YEAR = "{{ execution_date.strftime(\'%Y\') }}"

# lat lon of the location that weather data will be downladed from Open Weather Map
# right now the DAG only downloads one location (DIA). This may need to be parameterized better later.
LAT = 39.847
LON = -104.656


def download_current_weather_data(lat, lon, outfile):
    url = 'https://api.openweathermap.org/data/2.5/weather?'

    params = {'appid': OWM_API_KEY,
            'lat': lat,
            'lon': lon,
            'units': 'metric'}


    logging.info("requesting data from OWM API")
    r = requests.get(url, params)

    if r.status_code == 200:
        logging.info(r.status_code)

        with open(outfile, 'w') as f:
            json.dump(r.json(), f)
        logging.info(f'file written to {outfile}')
    
    else:
        logging.info(r.status_code) 
        message = f'OpenWeatherMap API returned value {r.status_code}'
        raise ValueError(message)


def extract_weather_data(file_suffix):

    with open(f"{AIRFLOW_HOME}/{file_suffix}.json") as f:
        j = json.load(f)
    
    # extract metadata table from json
    df = (pd.DataFrame(j['main'], index=[0])
                .assign(timestamp=pd.to_datetime(j['dt'], unit='s').tz_localize('UTC'),
                        **j['coord']
                        )
            )
    
    columns = ['temp', 'feels_like', 'temp_min', 'temp_max', 'pressure',
                 'humidity', 'timestamp', 'lon', 'lat']
                 
    df[columns].to_parquet(f'{AIRFLOW_HOME}/{file_suffix}.parquet')

    logging.info('files converted to parquet')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}



with DAG(
    dag_id="current_weather_owm_dag",
    schedule_interval="@hourly",
    default_args=default_args,
    start_date=datetime(2022, 4, 2),
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de', 'weather'],
) as dag:
    
    download_dataset_task = PythonOperator(
        task_id=f"download_dataset_task",
        python_callable=download_current_weather_data,
        op_kwargs={
            "lat": LAT,
            "lon": LON,
            "outfile": f"{AIRFLOW_HOME}/{DATASET_FILE_SUFFIX}.json"
        },
    )

    local_raw_to_gcs_task = PythonOperator(
        task_id=f"local_raw_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/owm/{LAT}_{LON}/{DATASET_FILE_SUFFIX}.json",
            "local_file": f"{AIRFLOW_HOME}/{DATASET_FILE_SUFFIX}.json",
        }
    )
    
    extract_data_task = PythonOperator(
        task_id=f"extract_eia_series_data_task",
        python_callable=extract_weather_data,
        op_kwargs={
            "file_suffix": DATASET_FILE_SUFFIX 
        }
    )

    local_extracted_to_gcs_task = PythonOperator(
        task_id=f'local_extracted_to_gcs_task',
        python_callable = upload_to_gcs,
        op_kwargs={
            'bucket': BUCKET,
            'object_name': f"staged/owm/{LAT}_{LON}/{DATASET_FILE_SUFFIX}.parquet",
            'local_file': f"{AIRFLOW_HOME}/{DATASET_FILE_SUFFIX}.parquet"
        }
    )

    cleanup_task = BashOperator(
        task_id=f"cleanup_task",
        bash_command=f'rm {AIRFLOW_HOME}/{DATASET_FILE_SUFFIX}.json {AIRFLOW_HOME}/{DATASET_FILE_SUFFIX}.parquet'
    )

    load_to_bq_task = GCSToBigQueryOperator(
        task_id='load_to_bq_task',
        bucket=BUCKET,
        source_objects=f"staged/owm/{LAT}_{LON}/{DATASET_FILE_SUFFIX}.parquet",
        destination_project_dataset_table=f'{BIGQUERY_DATASET}.hourly_updated_weather',
        source_format='parquet',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',

    )



    download_dataset_task >> local_raw_to_gcs_task >> extract_data_task >> local_extracted_to_gcs_task >> load_to_bq_task >> cleanup_task