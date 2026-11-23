terraform {
  required_version = ">= 1.11.4, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.44.0"
    }
  }
}

# Use the service-account JSON key via GOOGLE_APPLICATION_CREDENTIALS (Part 2).
provider "google" {
  project = "dengtaxi"
}

resource "google_storage_bucket" "raw_taxi" {
  # TODO: use a globally unique name, e.g. YOUR_PROJECT_ID-taxi-raw.
  name                        = "forooz-dengtaxi-taxi-raw"
  location                    = "EU"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
}

# Your turn: add a google_bigquery_dataset resource named taxi_analytics.
# Use dataset_id = "taxi_analytics" and the same location as the bucket.
# Read the documentation linked in Part 3 to write the resource block.
# A dataset groups BigQuery tables and views; it is not a storage bucket.
# The project is inherited from the Google provider above.
