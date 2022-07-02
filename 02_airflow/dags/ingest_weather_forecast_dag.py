import os
from datetime import datetime
import logging
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET  

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from gcloud_helpers import upload_to_gcs, upload_multiple_files_to_gcs


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
EIA_API_KEY = os.environ.get("EIA_API_KEY")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = 'energy_data'

DATASET_FILE_SUFFIX= "{{ logical_date.strftime(\'%Y-%m-%d-%H\') }}"
# REMOTE_DATASET_FILE_SUFFIX = "{{ logical_date.strftime(\'%Y-%m-%d\') }}" 

# lat lon of the location that weather data will be downladed from Open Weather Map
# right now the DAG only downloads one location (DIA). This may need to be parameterized better later.
LAT = 39.847
LON = -104.656


def download_temperature_forecast(lat, lon, outfile):
    
    params = dict(lat=lat,
              lon=lon,
              product='time-series',
              Unit='e',
              temp='temp',
              )

    r = requests.get(url='https://graphical.weather.gov/xml/sample_products/browser_interface/ndfdXMLclient.php', params=params)

    if r.status_code == 200:
        logging.info(r.status_code)
        # save the xml file
        with open(outfile, 'wb') as f:
            f.write(r.content)
        logging.info(f'file written to {outfile}')
    else:
        error_message = f'NWS API returned value {r.status_code}'
        raise ValueError(error_message)


def extract_temperature_forecast(local_file_name):
    
    tree = ET.parse(local_file_name)
    root = tree.getroot()

    # extract forecast creation time
    for item in root.findall('.head/product/creation-date'):
        creation_time = item.text

    # extract location 
    for item in root.findall('.data/location/point'):
        lat = item.attrib.get('latitude')
        lon = item.attrib.get('longitude')

    # extract forecast times
    fcst_times = list()
    for item in root.findall('.data/time-layout/start-valid-time'):
        fcst_times.append(item.text)

    # extract temperature values  
    temps = list()
    for item in root.findall('.data/parameters/temperature/value'):
        temps.append(item.text)
        

    assert len(temps) == len(fcst_times)

    # create dataframe
    forecast_df = (pd.DataFrame(data = {'lon': lon,
                                        'lat': lat,
                                        'forecast_time': fcst_times,
                                        'temp_F': temps,
                                        'creation_time': creation_time})
                    # assign correct data types
                    .assign(forecast_time=lambda df_: pd.to_datetime(df_['forecast_time']),
                            lon=lambda df_: df_['lon'].astype(float),
                            lat=lambda df_: df_['lat'].astype(float),
                            temp_F=lambda df_: df_['temp_F'].astype(int),
                            creation_time=lambda df_: pd.to_datetime(df_['creation_time']),

                            
                        )
                )

    fields = [('forecast_time', pa.timestamp('s')),
              ('lon', pa.float64()),
              ('lat', pa.float64()),
              ('temp_F', pa.int64()),
              ('creation_time', pa.timestamp('s'))
              ]
    schema = pa.schema(fields)
    table = pa.Table.from_pandas(forecast_df, schema=schema)
    pq.write_table(table, local_file_name.replace('.xml', '.parquet'))

    logging.info('files converted to parquet')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="ingest_weather_forecast_dag",
    schedule_interval="@daily",
    default_args=default_args,
    start_date=datetime(2022, 7, 1),
    catchup=True,
    max_active_runs=5,
    tags=['dtc-de', 'nws']
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_temperature_forecast,
        op_kwargs={
            'lat': LAT, 
            'lon': LON, 
            'outfile': f'{AIRFLOW_HOME}/weather_forecast_{DATASET_FILE_SUFFIX}.xml'
            },
    )


    local_raw_to_gcs_task = PythonOperator(
        task_id="local_raw_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/weather_forecast/{LAT}_{LON}_{DATASET_FILE_SUFFIX}.xml",
            "local_file": f'{AIRFLOW_HOME}/weather_forecast_{DATASET_FILE_SUFFIX}.xml'
        }
    )

    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_temperature_forecast,
        op_kwargs={
            'local_file_name': f'{AIRFLOW_HOME}/weather_forecast_{DATASET_FILE_SUFFIX}.xml'
        }
    )

    local_extracted_to_gcs_task = PythonOperator(
        task_id="local_extracted_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"staged/weather_forecast/{LAT}_{LON}_{DATASET_FILE_SUFFIX}.parquet",
            "local_file": f'{AIRFLOW_HOME}/weather_forecast_{DATASET_FILE_SUFFIX}.parquet'
        }
    
    )

    # delete the files downloaded locally
    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command = f'rm {AIRFLOW_HOME}/weather_forecast_{DATASET_FILE_SUFFIX}.xml && rm {AIRFLOW_HOME}/weather_forecast_{DATASET_FILE_SUFFIX}.parquet'
    )

    upload_to_bigquery_task = GCSToBigQueryOperator(
        task_id="upload_to_bigquery_task",
        bucket=BUCKET,
        source_objects=[f"staged/weather_forecast/{LAT}_{LON}_{DATASET_FILE_SUFFIX}.parquet"],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.weather_forecast",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",      
    )


    (download_dataset_task >> 
        local_raw_to_gcs_task >> 
        extract_data_task >> 
        local_extracted_to_gcs_task >> 
        cleanup_task >> 
        upload_to_bigquery_task
    )