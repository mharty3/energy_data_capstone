import os
import json
import logging
import requests
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcloud_helpers import upload_to_gcs
from batch_predict import apply_model
import logging

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
OWM_API_KEY = os.environ.get("OWM_API_KEY")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = 'energy_data'
date_fmt = '%Y-%m-%d_%H'
# DATE_string = "{{ execution_date.strftime(date_fmt) }}"
DATE_string = "{{ dag_run.logical_date.strftime('%Y-%m-%d_%H') }}"

# DATE = datetime.strptime(DATE, date_fmt)

tz = "US/Mountain"
run_id = '49c833f911ae43488e67063f410b7b5e'
output_file = f'{DATE_string}_{run_id}_output.parquet'

features = ['temp_F', 'year', 'day_of_year', 'hour', 'is_weekend', 
        'is_summer', 'month', 'temp_F_squared', 'hour_squared', 'hour_cubed']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# 2022-07-28T00:00:00+00:00
with DAG(
    dag_id='batch_predict_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 7, 5, 0, 0, 0),
    catchup=True,
    default_args=default_args,
    max_active_runs=3,
    tags=['mlops', 'batch_predict']
) as dag:    



    # make predictions and save locally to parquet file
    predict_task = PythonOperator(
        task_id='predict_task',
        python_callable=apply_model,
        op_kwargs={'run_id': run_id,
                   'features': features,
                   'start_date': DATE_string,
                   'date_fmt': date_fmt,
                   'output_file': f"{AIRFLOW_HOME}/{output_file}",
                   'tz': tz}
    )

    # upload to GCS
    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs_task',
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket': BUCKET,
            'object_name': f"staged/batch_predict/{DATE_string}/{run_id}/{output_file}",
            'local_file': f"{AIRFLOW_HOME}/{output_file}"
        }
    )   

    # load into BigQuery
    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery_task',
        bucket=BUCKET,
        source_objects=f"staged/batch_predict/{DATE_string}/{run_id}/{output_file}",
        destination_project_dataset_table=f'{BIGQUERY_DATASET}.energy_demand_forecasts',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
    )

    # delete local file
    delete_local_file_task = BashOperator(
        task_id='delete_local_file_task',
        bash_command=f"rm -f {AIRFLOW_HOME}/{output_file}"
    )

predict_task >> upload_to_gcs_task >> load_to_bigquery_task >> delete_local_file_task




