import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

TAXI_TYPES = {'yellow': {'partition_column': 'tpep_pickup_datetime',
                         'cluster_column': 'VendorID'},
              'green': {'partition_column': 'lpep_pickup_datetime',
                         'cluster_column': 'VendorID'},
              'fhv':    {'partition_column': 'dropoff_datetime',
                         'cluster_column': 'dispatching_base_num'}
}

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    for taxi_type, columns in TAXI_TYPES.items():
        partition_column=columns['partition_column']
        cluster_column=columns['cluster_column']

        gcs_to_gcs_task = GCSToGCSOperator(
            task_id=f"gcs_to_gcs_{taxi_type}_task",
            source_bucket=BUCKET,
            source_object=f'raw/{taxi_type}_tripdata_*.parquet',
            destination_bucket=BUCKET,
            destination_object=f"{taxi_type}/",
            move_object=True
        )

        gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id=f"gcs_to_bq_ext_{taxi_type}_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"external_{taxi_type}_tripdata",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{taxi_type}/*"],
                },
            },
        )

        if taxi_type in ['yellow', 'fhv']:
            CREATE_PARTITION_QUERY = f"""CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{taxi_type}_tripdata_partitoned
                                PARTITION BY
                                DATE({partition_column}) 
                                CLUSTER BY {cluster_column} AS
                                SELECT *  FROM {BIGQUERY_DATASET}.external_{taxi_type}_tripdata;"""

        elif taxi_type == 'green':                        
            CREATE_PARTITION_QUERY = f"""CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{taxi_type}_tripdata_partitoned
                                        PARTITION BY
                                        DATE({partition_column}) 
                                        CLUSTER BY {cluster_column} AS
                                        SELECT * EXCEPT (ehail_fee) FROM {BIGQUERY_DATASET}.external_{taxi_type}_tripdata;"""

        bq_ext_to_part_task = BigQueryInsertJobOperator(
            task_id=f"bq_ext_to_part_{taxi_type}_task",
            configuration={
                "query": {
                    "query": CREATE_PARTITION_QUERY,
                    "useLegacySql": False,
                }
            },
        )

        gcs_to_gcs_task >> gcs_to_bq_ext_task >> bq_ext_to_part_task