import os
import json
import logging
import requests
from datetime import date, datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# import pyarrow.csv as pv
# import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
EIA_API_KEY = os.environ.get("EIA_API_KEY")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# EIA_DATE = "{{ execution_date.strftime(\'%Y%m%d\') }}" # 20220314
# YEAR = "{{ execution_date.strftime(\'%Y\') }}"
# MONTH = "{{ execution_date.strftime(\'%m\') }}"

LOCAL_DATASET_FILE_SUFFIX = "{{ execution_date.strftime(\'%Y-%m-%d-%H\') }}.json"
REMOTE_DATASET_FILE_SUFFIX = "{{ execution_date.strftime(\'%Y-%m-%d\') }}.json" 


def extract_energy_demand(series_id, outfile):
    url = 'https://api.eia.gov/series/'
    
    params = {'api_key': EIA_API_KEY,
             'series_id': series_id,
             }

    r = requests.get(url, params)


    with open(outfile, 'w') as f:
        json.dump(r.json(), f)
    logging.info(f'file written to {outfile}')


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


series_list = [
    'EBA.PSCO-ALL.D.H', # Electrical Demand Public Service Company of Colorado in UTC
    'EBA.PSCO-ALL.DF.H' # Day-ahead demand forecast for Public Service Company of Colorado (PSCO), hourly - UTC time
]


with DAG(
    dag_id="raw_electricity_ingestion_dag",
    schedule_interval="@hourly",
    default_args=default_args,
    start_date=datetime(2022, 3, 25),
    catchup=True,
    max_active_runs=5,
    tags=['dtc-de', 'eia'],
) as dag:

    for series_id in series_list:
        local_file_name = f'{series_id}_{LOCAL_DATASET_FILE_SUFFIX}'
        remote_file_name = f'{series_id}_{REMOTE_DATASET_FILE_SUFFIX}'

        download_dataset_task = PythonOperator(
            task_id=f"download_{series_id}_dataset_task",
            python_callable=extract_energy_demand,
            op_kwargs={
                "series_id": series_id,
                "outfile": f"{AIRFLOW_HOME}/{local_file_name}"
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_{series_id}_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/eia/{series_id}/{remote_file_name}",
                "local_file": f"{AIRFLOW_HOME}/{local_file_name}",
            }
        )

        cleanup_task = BashOperator(
            task_id=f"cleanup_{series_id}_task",
            bash_command=f'rm {AIRFLOW_HOME}/{local_file_name}'
        )    
            

        download_dataset_task >> local_to_gcs_task >> cleanup_task