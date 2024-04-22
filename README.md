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

### Transformation Layer (Currently Testing in DBT Cloud)

Using dbt, we plan to transform the data from EVENTS_STG and load the final provisioning table, EVENTS_FINAL. 

EVENTS_FINAL model is created that transforms the data and keeps the key fields necessary for our visualization.

Pending enhancements:

a) Separate lookup table with CAMEO codes to identify the global actors involved would be helpful as we can use this and join with the GDELT dataset and get more insight into whether the actor is a country, state, person, organisation or other entity. 

b) Scheduled dbt jobs need to be set up (currently is manual as of latest commit) for the dashboard to be built on top of latest data

c) Include Spark for other transformations(?)

### Dashboard (Work in Progress)

Using Looker Studio, we should be able to derive insights and create a dashboard to visualize the events data. 

Possible Visualizations (pending):
1. Using geodata, a global map of incidents (arrests, killings, disasters, elections) 
2. Country-wise breakdown of events
3. Occurence of incidents over time (such as terrorist acts)

An insightful dashboard that is updated with latest GDELT data using schedule batch jobs is the end goal of the project. 
