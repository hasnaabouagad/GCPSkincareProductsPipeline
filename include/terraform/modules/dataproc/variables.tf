variable "region" {
  description = "The Dataproc cluster region"
  type        = string
  default     = "europe-west1"
}

variable "cluster_name" {
  description = "The Dataproc cluster name"
  type        = string
}


variable "machine_type"{
  description = "The master node machine type"
  type        = string
}

variable "service_account_email" {
  description = "Service account email for Dataproc cluster VMs"
  type        = string
}



