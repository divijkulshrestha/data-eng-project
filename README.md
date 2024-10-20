# Visualizing Events from All Over the Globe
## An End-to-End Data Pipeline

The goal of this project is to create an End-to-End batch pipeline, pointing to the GDELT V2.0 data source for all the latest global headlines, and processing this data into the form of a helpful dashboard. 

This is a data engineering project that will use the following tools:
1. Terraform
2. Google Cloud Platform
2. Apache Airflow
4. Google BigQuery
5. DBT Cloud
6. Looker

# About the data source

#### A Global Database of Society
Supported by Google Jigsaw, the GDELT Project monitors the world's broadcast, print, and web news from nearly every corner of every country in over 100 languages and identifies the people, locations, organizations, themes, sources, emotions, counts, quotes, images and events driving our global society every second of every day, creating a free open platform for computing on the entire world.

To learn more about the GDELT Project and the data they make available for us to use, head over to [their](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/) website.


# Project Flow

1. Setup GCP Infrastructure using Terraform
2. Use DAGs to bring in latest and historical GDELT data
3. Populate EVENTS_STG staging table in our data warehouse 
4. Process the data and apply transformations using DBT
5. Visualize the final dataset in the form of a dashboard

![Project Architecture](https://github.com/user-attachments/assets/68c25852-a74c-49de-b69a-c268672cc105)


### Using Terraform (IaC)
We can run the Terraform config scripts and create Google Cloud Storage (GCS) bucket which will serve as a data lake for our project. Raw source files will be loaded here, ready to be ingested into Google BiqQuery. 

The same script also creates a dataset for our use in BigQuery.

### Airflow DAGs 

The GDELT V2.0 sourcefiles are updated every fifteen minutes on their website, in CSV format, from 2015 onwards. That is a lot of data. 

We propose the following Ingestion DAGs to help load this data into our newly created dataset:
1. One Time Load DAG - We can configure this DAG to load the past X days data. It is scheduled to run once and will create the EVENTS_STG staging table that holds the raw data in BigQuery.

airflow/dags/one_time_load_bigquery_load.py

2. Periodic Batch DAG - An hourly DAG to assimilate the four CSV source files and load incremental data to our EVENTS_STG table. After our one-time historical table load, we plan to use this DAG to keep pulling in the latest GDELT data. 

airflow/dags/data_ingestion_bigquery_load.py

[screenshot - my DAGs](https://imgur.com/a/hb2X3pD)

[screenshot - DAG Trees](https://imgur.com/a/BoStxaZ)

[screenshot - GCP](https://imgur.com/a/yzEd99a)

### Transformation Layer using DBT Cloud

Using dbt, we then transform the data from EVENTS_STG and load the final provisioning table, EVENTS_FINAL. This includes lookup for CAMEO codes (country code, actor type codes and event codes) and datatype conversions. The final table contains only the key columns we are interested in, and can be further used as the data source for our visualizations.

### Dashboard using Looker Studio

Using Looker Studio, we should are able to derive insights and create a dashboard to visualize the events data. 

Here is an example dashboard that breaks down the Violent Events of 2024 (so far):

![image](https://github.com/user-attachments/assets/1a05f703-3ea6-44b5-ad1e-025f3b94f845)


![image](https://github.com/user-attachments/assets/7d5409be-b588-4986-8bec-8a96b3a0cbef)


### Potential Enhancements :
1. Using Geospatial data to map out the occurences of the events
2. Including Apache Spark in the transformation layer
3. Scheduling NRT jobs to run periodically and keep the Looker dashboard up-to-date 
