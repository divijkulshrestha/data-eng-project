import os
import logging

import time
import datetime
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from urllib.request import urlretrieve

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = "gdelt2_dataset"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

last_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
file_name = "gdelt_data.txt"
source_pq = "gdelt_data.parquet"

def get_last_hour_files():
    """
    Function to retrieve the four most recent Events GDELT csv sourcefiles. 
    """
    urlretrieve(last_url, source_file)

    f = open(source_file, "r")
    lines = f.readlines()

    #latest file
    file0 = lines[0].split()[2]
    timestamp0 = str_to_date(file0.split('/')[4].split('.')[0])
    
    #get previous hour files
    timestamp1 = timestamp0 - timedelta(minutes=15)
    timestamp2 = timestamp1 - timedelta(minutes=15)
    timestamp3 = timestamp2 - timedelta(minutes=15)

    file3 = f"http://data.gdeltproject.org/gdeltv2/{timestamp3.strftime('%Y%m%d%H%M%S')}.export.CSV.zip"
    file2 = f"http://data.gdeltproject.org/gdeltv2/{timestamp2.strftime('%Y%m%d%H%M%S')}.export.CSV.zip"
    file1 = f"http://data.gdeltproject.org/gdeltv2/{timestamp1.strftime('%Y%m%d%H%M%S')}.export.CSV.zip"


    urlretrieve(file3, "file3.zip")
    urlretrieve(file2, "file2.zip")
    urlretrieve(file1, "file1.zip")
    urlretrieve(file0, "file0.zip")
    #print("Files downloaded!")

    with open(source_file, 'w') as file:
        file.write(f"{timestamp0.strftime('%Y%m%d%H%M%S')}.export.CSV"+'\n')
        file.write(f"{timestamp1.strftime('%Y%m%d%H%M%S')}.export.CSV"+'\n')
        file.write(f"{timestamp2.strftime('%Y%m%d%H%M%S')}.export.CSV"+'\n')        
        file.write(f"{timestamp3.strftime('%Y%m%d%H%M%S')}.export.CSV"+'\n')
        
def read_last_hour_files():
    """
    Function to extract the CSV files and generate a single source file.
    """    
    f = open(source_file, "r")
    lines = f.readlines()

    zip_file_path = "C:/Users/Anantk/AppData/Local/Programs/Python/Python311/"
    df_csv_append = pd.DataFrame()

    with zipfile.ZipFile(zip_file_path+'file0.zip', 'r') as zip_ref:
        zip_ref.extract(lines[0].strip(), path=".")
    #print(lines[0])

    with zipfile.ZipFile(zip_file_path+'file1.zip', 'r') as zip_ref:
        zip_ref.extract(lines[1].strip(), path=".")
    #print(lines[1])

    with zipfile.ZipFile(zip_file_path+'file2.zip', 'r') as zip_ref:
        zip_ref.extract(lines[2].strip(), path=".")
    #print(lines[2])

    with zipfile.ZipFile(zip_file_path+'file3.zip', 'r') as zip_ref:
        zip_ref.extract(lines[3].strip(), path=".")
    #print(lines[3])


    df_csv_append = pd.DataFrame()
             
    # concat the CSV files and generate a single parquet file
    df_csv_concat = pd.concat([pd.read_csv(file.strip(), delimiter='\t') for file in lines], ignore_index=True)
    df_csv_concat.to_parquet(source_pq, index=False)
   
def str_to_date(datetime_str): 
    datetime_object = datetime.strptime(datetime_str, '%Y%m%d%H%M%S')
    return datetime_object
    
def remove_file(filename):
    os.remove(filename)

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
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@hourly",
    default_args=default_args,
    description="DAG to lookup GDELT past hour files and load to GCS Bucket as Parquet File",
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        python_callable=get_last_hour_files
    )

    prepare_parquet_task = PythonOperator(
        task_id="prepare_parquet_task",
        python_callable=read_last_hour_files
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{source_pq}",
            "local_file": source_pq,
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{source_pq}"],
            },
        },
    )

    download_dataset_task >> prepare_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
