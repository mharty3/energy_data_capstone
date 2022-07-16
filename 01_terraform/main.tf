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


resource "google_storage_bucket" "mlflow-runs" {

  name          = "mlflow-runs-${var.project}" # Concatenating DL bucket & Project name for unique naming
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



# resource "google_compute_network" "default" {
#   provider = google-beta

#   name = "default"
# }

# resource "google_sql_database_instance" "main" {
#   name             = "main-instance"
#   database_version = "POSTGRES_14"
#   region           = "us-central1"
#   # depends_on = [google_service_networking_connection.private_vpc_connection]

#   settings {
#     # Second-generation instance tiers are based on the machine
#     # type. See argument reference below.
#     tier = var.machine_type
#     ip_configuration {
#       ipv4_enabled = false
#       private_network = google_compute_network.default.id
#     }
#   }
  


# }


# module "sql-db_postgresql" {
#   # https://registry.terraform.io/modules/GoogleCloudPlatform/sql-db/google/latest/submodules/postgresql
#   source  = "GoogleCloudPlatform/sql-db/google//modules/postgresql"
#   version = "11.0.0"

#   project_id = var.project
#   region  = var.region
#   zone = var.zone
#   name   = var.instance_name
#   db_name = var.db_name

#   database_version      = var.postgres_version
#   tier = var.machine_type



#   ip_configuration = {
#                       authorized_networks = [],
#                       ipv4_enabled        = false,
#                       private_network     = "default"
#                       require_ssl         = false
#                       allocated_ip_range = ""
#                           }


#   deletion_protection = true

#   # These together will construct the master_user privileges, i.e.
#   # 'master_user_name'@'master_user_host' IDENTIFIED BY 'master_user_password'.
#   # These should typically be set as the environment variable TF_VAR_master_user_password, etc.
#   # so you don't check these into source control."


# }