locals {
  data_lake_bucket = "energy_project_bucket"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-central1"
  type = string
}

variable "zone" {
  description = "Zone for compute instance"
  default = "us-central1-c"
  type = string
}
variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "energy_data"
}

variable "ssh_public_key_file" {
  description = "Path to the public ssh key that will be used to connect to the Compute Instance created by terraform"
  type = string
  default = "~/.ssh/gcp2.pub"
}

variable "ssh_user" {
  description = "username to connect to the Compute Instance created by terraform via ssh"
  type = string
  default = "michael"
}

