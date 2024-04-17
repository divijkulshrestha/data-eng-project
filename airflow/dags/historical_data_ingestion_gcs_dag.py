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
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = "gdelt2_dataset"
STG_TABLE = "events_stg"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

last_url = "http://data.gdeltproject.org/gdeltv2/"
source_file = "gdelt_data.txt"
source_pq = "gdelt_data.parquet"
num_days = 2

# header names for GDELT V2.0 
column_names =['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 
'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 
'Actor2CountryCode', 'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code', 
'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type', 
'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID', 'Actor2Geo_Type', 
'Actor2Geo_FullName', 'Actor2Geo_CountryCode', 'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code', 'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID', 'ActionGeo_Type', 
'ActionGeo_FullName', 'ActionGeo_CountryCode', 'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code', 'ActionGeo_Lat', 'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']

schema = [
{"name":"GLOBALEVENTID", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"SQLDATE", "type": "DATE", "mode": "NULLABLE"},
{"name":"MonthYear", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"Year", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"FractionDate", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"Actor1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Name", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1CountryCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1KnownGroupCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1EthnicCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Religion1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Religion2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Type1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Type2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Type3Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Name", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2CountryCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2KnownGroupCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2EthnicCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Religion1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Religion2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Type1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Type2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Type3Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"IsRootEvent", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"EventCode", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"EventBaseCode", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"EventRootCode", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"QuadClass", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"GoldsteinScale", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"NumMentions", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"NumSources", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"NumArticles", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"AvgTone", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"Actor1Geo_Type", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"Actor1Geo_FullName", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Geo_CountryCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Geo_ADM1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Geo_ADM2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor1Geo_Lat", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"Actor1Geo_Long", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"Actor1Geo_FeatureID", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Geo_Type", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"Actor2Geo_FullName", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Geo_CountryCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Geo_ADM1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Geo_ADM2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"Actor2Geo_Lat", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"Actor2Geo_Long", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"Actor2Geo_FeatureID", "type": "STRING", "mode": "NULLABLE"},
{"name":"ActionGeo_Type", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"ActionGeo_FullName", "type": "STRING", "mode": "NULLABLE"},
{"name":"ActionGeo_CountryCode", "type": "STRING", "mode": "NULLABLE"},
{"name":"ActionGeo_ADM1Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"ActionGeo_ADM2Code", "type": "STRING", "mode": "NULLABLE"},
{"name":"ActionGeo_Lat", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"ActionGeo_Long", "type": "FLOAT", "mode": "NULLABLE"},
{"name":"ActionGeo_FeatureID", "type": "STRING", "mode": "NULLABLE"},
{"name":"DATEADDED", "type": "INTEGER", "mode": "NULLABLE"},
{"name":"SOURCEURL", "type": "STRING", "mode": "NULLABLE"}
]


def get_gdelt_source_data():
    """
    Function to retrieve historical GDELT Data files (CSV) 
    """
    # get current hour timestamp
    current_datetime = datetime.datetime.now()
    current_datetime = current_datetime.strftime("%Y%m%d%H") + "0000"

    past_datetime = (str_to_date(current_datetime) - timedelta(days=num_days)).strftime('%Y%m%d%H%M%S')

    concatenated_df = pd.DataFrame()

    while past_datetime != current_datetime:
        source_file = f"http://data.gdeltproject.org/gdeltv2/{past_datetime}.export.CSV.zip"
        urlretrieve(source_file, "source_file.zip")

        file_name = f"{past_datetime}.export.CSV"

        zip_file_path = "./"

        # zip extract
        with zipfile.ZipFile(zip_file_path+'source_file.zip', 'r') as zip_ref:
            zip_ref.extract(file_name, path=".")
        
        df = pd.read_csv(file_name, delimiter='\t', header=None, names=column_names)
        concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)

        past_datetime = (str_to_date(past_datetime)+timedelta(minutes=15)).strftime('%Y%m%d%H%M%S')

    concatenated_df.to_parquet(source_pq, index=False)

   
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
    schedule_interval="@once",
    default_args=default_args,
    description="One Time Load DAG to lookup GDELT past X days files and load to GCS Bucket and BigQuery",
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    prepare_dataset_task = PythonOperator(
        task_id="prepare_dataset_task",
        python_callable=get_gdelt_source_data
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

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table_task",
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        table_id=STG_TABLE,
        schema_fields=schema
    )

    gcs_to_bq_task = GCSToBigQueryOperator(
    task_id = "gcs_to_bq_task",
    bucket = BUCKET,
    source_objects = f"raw/{source_pq}",
    destination_project_dataset_table = f'{PROJECT_ID}:{BIGQUERY_DATASET}.{STG_TABLE}',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    source_format = 'parquet',
    skip_leading_rows = 1       
    )

    create_table_task >> prepare_dataset_task >> local_to_gcs_task >> gcs_to_bq_task
