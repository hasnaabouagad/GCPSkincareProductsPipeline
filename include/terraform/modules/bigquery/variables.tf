variable "dataset_name"{
  description = "The name of the BigQuery dataset"
  type        = string
}

variable "project_id" {
    description = "The ID of the GCP project"
    type = string
}

variable "region" {
    description = "The region of the GCP project"
    default = "europe-west1"
    type = string
}
