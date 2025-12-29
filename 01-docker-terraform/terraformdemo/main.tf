terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.14.1"
    }
  }
}

provider "google" {
  credentials = "${file("keys/terrademo-credits.json")}"
  project     = "airy-sled-482514-m7"
  region      = "us-central1"
}
# bucket with lifecycle rules
# bucket variable name: first-demo-bucket 
# "defines the name of the resource in terraform, doesn't need to be globally unique"
# bucket name: airy-sled-482514-m7-terraform-demo-bucket
# "defines the name of the bucket in GCP, needs to be globally unique"
resource "google_storage_bucket" "first-demo-bucket" {
  name          = "airy-sled-482514-m7-terraform-demo-bucket"
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

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}