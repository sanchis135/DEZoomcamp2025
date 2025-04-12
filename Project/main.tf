provider "google" {
  project     = "your-ID-Project"
  region      = "us-central1"
}

resource "google_storage_bucket" "data_lake" {
  name     = "your-name-bucket" 
  location = "US"
}

resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id = "project_zoomcamp"
  location   = "US"
}