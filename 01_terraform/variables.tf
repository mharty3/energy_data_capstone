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
  default = "~/.ssh/gcp3.pub"
}

variable "ssh_user" {
  description = "username to connect to the Compute Instance created by terraform via ssh"
  type = string
  default = "michael"
}

variable "instance_name" {
  description = "Name of the Postgres Instance created by terraform"
  type = string
  default = "mlflow-postgres"
}

variable "db_name" {
  description = "Name of the Postgres Database created by terraform"
  type = string
  default = "mlflow"
}

variable "postgres_version" {
  description = "The engine version of the database, e.g. `POSTGRES_9_6`. See https://cloud.google.com/sql/docs/db-versions for supported versions."
  type        = string
  default     = "POSTGRES_14"
}


variable "machine_type" {
  description = "The machine type to use, see https://cloud.google.com/sql/pricing for more details"
  type        = string
  default     = "db-custom-1-3840"
}

variable "user_name" {
  description = "The username for mlflow-postgres the default user credentials, i.e. 'master_user_name'@'master_user_host' IDENTIFIED BY 'master_user_password'. This should typically be set as the environment variable TF_VAR_master_user_name so you don't check it into source control."
  type        = string
  default = "mlflow_user"
}

variable "user_password" {
  description = "The password for ml-flow postgres the default user credentials, i.e. 'master_user_name'@'master_user_host' IDENTIFIED BY 'master_user_password'. This should typically be set as the environment variable TF_VAR_master_user_password so you don't check it into source control."
  type        = string
  default = "mlflow_pass"
}

