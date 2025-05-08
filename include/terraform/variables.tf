variable "project_id" {
    description = "The ID of the GCP project"
    type = string
}

variable "region" {
    description = "The region of the GCP project"
    default = "europe-west1"
    type = string
}

variable "project_name" {
  description = "The name of the GCP project"
  type        = string
}

variable "storage_class" {
  description = "The storage class for the bucket"
  type        = string
}


variable "cluster_name" {
  description = "The Dataproc cluster name"
  type        = string
}


variable "machine_type"{
  description = "The master node machine type"
  type        = string
}

variable "dataset_name"{
  description = "The name of the BigQuery dataset"
  type        = string
}

variable "service_account_email" {
  description = "Service account email for Dataproc cluster VMs"
  type        = string
}
