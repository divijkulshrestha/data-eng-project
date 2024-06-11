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
 table_id            = "gdelt_events_stg"
 deletion_protection = false
 depends_on = [
    google_bigquery_dataset.bq_dataset,
  ]

 schema = file(var.stage_schema)

 #time_partitioning {
 #  type  = "DAY"
 #  field = "SQLDATE"
# }

 clustering = ["Year"]

}