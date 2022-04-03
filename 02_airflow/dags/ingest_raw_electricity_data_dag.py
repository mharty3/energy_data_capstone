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
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from gcloud_helpers import upload_to_gcs, upload_multiple_files_to_gcs

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
EIA_API_KEY = os.environ.get("EIA_API_KEY")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = 'energy_data'

LOCAL_DATASET_FILE_SUFFIX= "{{ logical_date.strftime(\'%Y-%m-%d-%H\') }}"
REMOTE_DATASET_FILE_SUFFIX = "{{ logical_date.strftime(\'%Y-%m-%d\') }}" 

# EIA series ID's that will be downloaded by this DAG
SERIES_LIST = [
    'EBA.PSCO-ALL.D.H', # Electrical Demand Public Service Company of Colorado in UTC
    'EBA.PSCO-ALL.DF.H' # Day-ahead demand forecast for Public Service Company of Colorado (PSCO), hourly - UTC time
]

def download_energy_demand_json(series_id, outfile):
    """
    Request data for an EIA series and save the result locally as a json file 
    """

    url = 'https://api.eia.gov/series/'
    params = {'api_key': EIA_API_KEY,
             'series_id': series_id,
             }

    logging.info("requesting data from EIA API")
    r = requests.get(url, params)

    if r.status_code == 200:
        logging.info(r.status_code)
        with open(outfile, 'w') as f:
            json.dump(r.json(), f)
        logging.info(f'file written to {outfile}')

    else:
        error_message = f'EIA API returned value {r.status_code}'
        raise ValueError(error_message)


def extract_energy_demand_data(series_id, local_file_name, local_file_suffix):
    """
    Extract data and metadata of an EIA series json file and store the results locally as a parquet file
    """

    with open(f"{AIRFLOW_HOME}/{local_file_name}") as f:
        j = json.load(f)
    
    # extract metadata table from json
    metadata = pd.DataFrame((j['series'][0])).loc[[0], :].drop('data', axis=1)
    metadata['series_id'] = metadata['series_id'].str.replace('.', '_')
    
    # extract data series 
    data = j['series'][0]['data']
    data_df = (pd.DataFrame(data, columns=['timestamp', 'value'])
                .assign(timestamp=lambda df_:pd.to_datetime(df_['timestamp']),
                        series_id=series_id.replace('.', '_'))
              )
    data_df.columns = data_df.columns.str.replace('.', '_')

    # the schema must be defined for the data_df to store the table as parquet.
    # Pandas/pyarrow can not automatically define the schema. The value field
    # is mostly integers, but does have missing values. Automatically defining
    # the schema will assign integer which cannot contain missing values
    fields = [
    ('timestamp', pa.timestamp(unit='ns')),
    ('value', pa.float64()),
    ('series_id', pa.string()),
    ]
    schema = pa.schema(fields)
    table = pa.Table.from_pandas(data_df, schema=schema)
   
    pq.write_table(table, f'{AIRFLOW_HOME}/data_{local_file_suffix}.parquet')
    metadata.to_parquet(f'{AIRFLOW_HOME}/metadata_{local_file_suffix}.parquet')

    logging.info('files converted to parquet')



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="raw_electricity_ingestion_dag",
    schedule_interval="@hourly",
    default_args=default_args,
    start_date=datetime(2022, 3, 25),
    catchup=True,
    max_active_runs=5,
    tags=['dtc-de', 'eia'],
) as dag:
    with TaskGroup(group_id='download_and_extract') as dl_and_extract_tg:
        for series_id in SERIES_LIST:
            local_file_suffix = f'{series_id}_{LOCAL_DATASET_FILE_SUFFIX}'
            remote_file_suffix= f'{series_id}_{REMOTE_DATASET_FILE_SUFFIX}'

            download_dataset_task = PythonOperator(
                task_id=f"download_{series_id}_dataset_task",
                python_callable=download_energy_demand_json,
                op_kwargs={
                    "series_id": series_id,
                    "outfile": f"{AIRFLOW_HOME}/{local_file_suffix}.json"
                },
            )

            local_raw_to_gcs_task = PythonOperator(
                task_id=f"local_raw_to_gcs_{series_id}_task",
                python_callable=upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": f"raw/eia/{series_id}/{remote_file_suffix}.json",
                    "local_file": f"{AIRFLOW_HOME}/{local_file_suffix}.json",
                }
            )
            
            extract_data_task = PythonOperator(
                task_id=f"extract_eia_series_data_task_{series_id}",
                python_callable=extract_energy_demand_data,
                op_kwargs={
                    "series_id": series_id,
                    "local_file_name": f"{local_file_suffix}.json",
                    "local_file_suffix": local_file_suffix
                }
            )

            local_extracted_to_gcs_task = PythonOperator(
                task_id=f'local_extracted_to_gcs_{series_id}_task',
                python_callable = upload_multiple_files_to_gcs,
                op_kwargs={
                    'bucket': BUCKET,
                    'object_names': [f"staged/eia/data/{series_id}.parquet",
                                    f"staged/eia/metadata/{series_id}.parquet"],
                    'local_files': [f"{AIRFLOW_HOME}/data_{local_file_suffix}.parquet", 
                                    f"{AIRFLOW_HOME}/metadata_{local_file_suffix}.parquet"]
                }
            )

            # delete all of the files downloaded to the worker
            cleanup_task = BashOperator(
                task_id=f"cleanup_{series_id}_task",
                bash_command=f'rm {AIRFLOW_HOME}/{local_file_suffix}.json {AIRFLOW_HOME}/data_{local_file_suffix}.parquet {AIRFLOW_HOME}/metadata_{local_file_suffix}.parquet'
            )    

            download_dataset_task >> local_raw_to_gcs_task >> extract_data_task >> local_extracted_to_gcs_task >> cleanup_task
    
    with TaskGroup(group_id='load_to_bq') as load_to_bq_tg:
        bucket_subfolders = ['data', 'metadata']
        external_table_names = ['demand_data_external', 'demand_metadata_external']
        native_table_names = ['demand_data_native', 'demand_metadata_native']

        for bucket_subfolder, external_table, native_table in zip(bucket_subfolders, 
                                                                  external_table_names, 
                                                                  native_table_names):

            gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
                task_id=f"gcs_to_bq_ext_eia_series_{bucket_subfolder}_task",
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": external_table,
                    },
                    "externalDataConfiguration": {
                        "sourceFormat": "PARQUET",
                        "sourceUris": [f"gs://{BUCKET}/staged/eia/{bucket_subfolder}/*"],
                    },
                },
            )
            
            CREATE_NATIVE_TABLE_QUERY = f"""CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{native_table}
                                            AS SELECT * FROM {BIGQUERY_DATASET}.{external_table};"""

            create_native_bq_table_task = BigQueryInsertJobOperator(
                task_id=f"bq_ext_to_native_{bucket_subfolder}_task",
                configuration={
                    "query": {
                        "query": CREATE_NATIVE_TABLE_QUERY,
                        "useLegacySql": False,
                    }
                },
            )

            gcs_to_bq_ext_task >> create_native_bq_table_task
    

    dl_and_extract_tg >> load_to_bq_tg
    
    