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
  description = "BigQuery Dataset"
  default     = "data_eng_project"
}

variable "gcs_bucket_name" {
  description = "Lookup Data Bucket"
  default     = "lookup_files"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}


#big query tables
variable "bq_stage_table" {
  description = "Staging Layer - Holds Events Data for Specific Root Event Code"
  default     = "events_stg"
}

variable "bq_country_code_lookup" {
  description = "Lookup Country Codes"
  default     = "country_codes"
}

variable "bq_event_code_lookup" {
  description = "Lookup CAMEO Event Codes"
  default     = "event_codes"
}

variable "bq_actor_type_lookup" {
  description = "Lookup CAMEO Actor Codes"
  default     = "type_codes"
}

#schemas
variable "bq_stage_table_schema" {
  description = "Staging Layer - Events Schema"
  default="stage_table_schema.json"  
}

variable "bq_lookup_table_schema" {
  default="lookup_table_schema.json"
}