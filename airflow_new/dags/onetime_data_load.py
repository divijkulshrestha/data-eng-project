# import libraries
import os
import logging
import pandas as pd
from urllib.request import urlretrieve

# key airflow libraries
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# airflow-big query libraries
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
#from airflow.contrib.operators.big_query_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = "data_eng_project"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

query = """
SELECT DISTINCT GLOBALEVENTID, _PARTITIONTIME as EventTimestamp, MonthYear, Year, EventCode, Actor1CountryCode, Actor2CountryCode, 
     Actor1Type1Code, Actor2Type1Code 
      FROM `gdelt-bq.gdeltv2.events_partitioned`
      WHERE 
      EXTRACT(YEAR FROM (TIMESTAMP_TRUNC(_PARTITIONTIME, DAY))) = 2024  
      AND EventRootCode='18' 
      AND IsRootEvent=1
"""

def get_lookup_files():
    """
    Function to retrieve the lookup TXT files to the local machine
    """
    # lookup files
    lookup_url1 = "https://www.gdeltproject.org/data/lookups/CAMEO.country.txt"
    lookup_url2 = "https://www.gdeltproject.org/data/lookups/CAMEO.eventcodes.txt"
    lookup_url3 = "https://www.gdeltproject.org/data/lookups/CAMEO.type.txt"
    
    urlretrieve(lookup_url1, "country_codes.txt")
    urlretrieve(lookup_url2, "event_codes.txt")
    urlretrieve(lookup_url3, "type_codes.txt")
    

def convert_to_csv():
    """
    Function to convert the TXT files and generate CSV files
    """    
    df = pd.read_csv('country_codes.txt', delimiter='\t', header = 0) 
    df.to_csv('country_codes.csv', index=False)

    df = pd.read_csv('event_codes.txt', delimiter='\t', header = 0) 
    df.to_csv('event_codes.csv', index=False)

    df = pd.read_csv('type_codes.txt', delimiter='\t', header = 0) 
    df.to_csv('type_codes.csv', index=False)


def upload_to_gcs(bucket, object_name, local_file):
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
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="lookup_data_load",
    schedule_interval="@once",
    default_args=default_args,
    description="One Time Load - GDELT Data",
    catchup=False,
    max_active_runs=1,
    tags=['Divij'],
) as dag:

    download_data_task = PythonOperator(
        task_id="download_data_task",
        python_callable=get_lookup_files
    )

    prepare_csv_task = PythonOperator(
        task_id="prepare_csv_task",
        python_callable=convert_to_csv
    )

    load_country_codes_task = PythonOperator(
        task_id="load_country_codes_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/country_codes.csv",
            "local_file": "country_codes.csv",
        },
    )

    load_event_codes_task = PythonOperator(
        task_id="load_event_codes_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/event_codes.csv",
            "local_file": "event_codes.csv",
        },
    )

    load_type_codes_task = PythonOperator(
        task_id="load_type_codes_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/type_codes.csv",
            "local_file": "type_codes.csv",
        },
    )

    load_country_codes_bq_task = GCSToBigQueryOperator(
    task_id = "load_country_codes_bq_task",
    bucket = BUCKET,
    source_objects = f"raw/country_codes.csv",
    destination_project_dataset_table = f'{PROJECT_ID}:{BIGQUERY_DATASET}.{"country_codes"}',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    source_format = 'csv',
    skip_leading_rows = 1       
    )

    load_event_codes_bq_task = GCSToBigQueryOperator(
    task_id = "load_event_codes_bq_task",
    bucket = BUCKET,
    source_objects = f"raw/event_codes.csv",
    destination_project_dataset_table = f'{PROJECT_ID}:{BIGQUERY_DATASET}.{"event_codes"}',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    source_format = 'csv',
    skip_leading_rows = 1       
    )

    load_type_codes_bq_task = GCSToBigQueryOperator(
    task_id = "load_type_codes_bq_task",
    bucket = BUCKET,
    source_objects = f"raw/type_codes.csv",
    destination_project_dataset_table = f'{PROJECT_ID}:{BIGQUERY_DATASET}.{"type_codes"}',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    source_format = 'csv',
    skip_leading_rows = 1       
    )

    load_events_stg_task = BigQueryExecuteQueryOperator(
        task_id='load_events_stg_task',
        sql=query,
        destination_dataset_table=f'{PROJECT_ID}:{BIGQUERY_DATASET}.{"events_stg"}',
        write_disposition='WRITE_TRUNCATE',  
        use_legacy_sql=False  
    )

    load_events_stg_task >> download_data_task >> prepare_csv_task >> [load_country_codes_task, load_event_codes_task, load_type_codes_task] 
    load_country_codes_task >> load_country_codes_bq_task
    load_event_codes_task >> load_event_codes_bq_task
    load_type_codes_task >> load_type_codes_bq_task