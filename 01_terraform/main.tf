terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  # credentials =  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}


# A single Compute Engine instance
resource "google_compute_instance" "default" {
 name         = "energy-data-proj-vm"
 machine_type = "e2-standard-4"
 zone         = var.zone

 boot_disk {
   initialize_params {
     image = "ubuntu-1804-lts"
     size  = "30"
   }
 }

 network_interface {
   network = "default"
    access_config {
    # Include this section to give the VM an external IP address
  }
 }

  metadata = {
    ssh-keys = "${var.ssh_user}:${file(var.ssh_public_key_file)}"
  }
}


module "postgres" {

  project = var.project
  region  = var.region
  name    = var.instance_name
  db_name = var.db_name

  engine       = var.postgres_version
  machine_type = var.machine_type

  public_ip = "disabled"
  private_ip = "enabled"
  private_network = "default"


  deletion_protection = true

  # These together will construct the master_user privileges, i.e.
  # 'master_user_name'@'master_user_host' IDENTIFIED BY 'master_user_password'.
  # These should typically be set as the environment variable TF_VAR_master_user_password, etc.
  # so you don't check these into source control."
  master_user_password = var.master_user_password

  master_user_name = var.master_user_name
  master_user_host = "%"

  # Wait for the vpc connection to complete
  dependencies = [google_service_networking_connection.private_vpc_connection.network]

}