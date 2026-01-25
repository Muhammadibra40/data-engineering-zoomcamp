terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.14.1"
    }
  }
}

provider "google" {
  credentials = file("/workspaces/data-engineering-zoomcamp/01-docker-terraform/terraformdemo/keyys/terrademo-credits.json")
  project     = "airy-sled-482514-m7"
  region      = "us-central1"
}

resource "google_storage_bucket" "demo-terra-bucket" {
  name          = "airy-sled-482514-m7-demo-terra-bucket"
  location      = "US"
  force_destroy = true

    lifecycle_rule {
      condition {
        age = 1
      }
      action {
        type = "Delete"
      }
    }
  }