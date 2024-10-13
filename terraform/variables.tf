locals {
  data_lake_bucket = "sbti_de_data_lake"
}

variable "project" {
  description = "GCP project name"
  default = "sbti-438203"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west3"
  type = string
}

variable "credentials" {
  description = "Path to the credential json file"
  default = "sbti-service-cred.json"
  type = string
}


variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bq_dataset_staging" {
  description = "BigQuery Dataset that serves as staging layer where data tables are injested into"
  type = string
  default = "de_dataset_staging"
}

variable "bq_dataset_warehouse" {
  description = "BigQuery Dataset that serves as data warehouse layer where transformed data table is stored, ready for analytical use"
  type = string
  default = "de_dataset_warehouse"
}



 # Output section to expose variable names to DAGs via the Airflow environment
output "BQ_DATASET_STAGING" {
  value = var.bq_dataset_staging
}

output "DATASET_WAREHOUSE" {
  value = var.bq_dataset_warehouse
}

