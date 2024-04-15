variable "project" {
  description = "GCP Project ID"
  default     = "divij-zoomcamp-2024"
}

variable "region" {
  description = "Region"
  default     = "asia-south1-c"
}

variable "location" {
  description = "Project Location"
  default     = "ASIA-SOUTH1"
}

variable "bq_dataset_name" {
  description = "Final Project - BigQuery Dataset"
  default     = "gdelt2_dataset"
}

variable "bq_table_name" {
  description = "Final Project - BigQuery Staging Table"
  default     = "events_stg"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket - Zoomcamp Practice"
  default     = "divij-gdelt-2024-gcs-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}