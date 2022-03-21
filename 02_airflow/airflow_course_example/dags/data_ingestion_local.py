from datetime import datetime
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# home directory inside our worker is stored as an environment variable
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")



local_workflow = DAG(
    "Local_Ingestion_Dag",
    schedule_interval="0 6 2 * *", # https://crontab.guru/#0_6_2_*_*
    start_date=datetime(2021, 1, 1)
)


# Airflow uses jinja templating. and provides access to certain variables for instance
# the execution_date which we use below along with strftime to convert to the proper format
URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_'
URL_TEMPLATE = URL_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = BashOperator(
        task_id='ingest',
        bash_command=f'ls {AIRFLOW_HOME}'
    )

    wget_task >> ingest_task