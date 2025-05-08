variable "project_name" {
  description = "The name of the GCP project"
  type        = string
}

variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "The GCP region for the bucket"
  type        = string
}

variable "storage_class" {
  description = "The storage class for the bucket"
  type        = string
}

