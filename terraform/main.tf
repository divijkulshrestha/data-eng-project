terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  #credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_bigquery_table" "staging_table" {
 project             = var.project
 dataset_id          = var.bq_dataset_name
 table_id            = var.bq_stage_table
 deletion_protection = false
 depends_on = [
    google_bigquery_dataset.bq_dataset,
  ]

 schema = file(var.bq_stage_table_schema)

 /*time_partitioning {
  # type  = "DAY"
   #field = "MonthYear"
 }

 clustering = ["Year","EventCode"]
*/
}


resource "google_bigquery_table" "lookup_table1" {
 project             = var.project
 dataset_id          = var.bq_dataset_name
 table_id            = var.bq_actor_type_lookup
 deletion_protection = false
 depends_on = [
    google_bigquery_dataset.bq_dataset,
  ]

  schema = file(var.bq_lookup_table_schema)

}

resource "google_bigquery_table" "lookup_table2" {
 project             = var.project
 dataset_id          = var.bq_dataset_name
 table_id            = var.bq_country_code_lookup
 deletion_protection = false
 depends_on = [
    google_bigquery_dataset.bq_dataset,
  ]

  schema = file(var.bq_lookup_table_schema)

}

resource "google_bigquery_table" "lookup_table3" {
 project             = var.project
 dataset_id          = var.bq_dataset_name
 table_id            = var.bq_event_code_lookup
 deletion_protection = false
 depends_on = [
    google_bigquery_dataset.bq_dataset,
  ]

  schema = file(var.bq_lookup_table_schema)

}