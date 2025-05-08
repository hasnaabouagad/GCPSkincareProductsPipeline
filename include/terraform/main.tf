terraform {
  backend "local" {
    
  }
  required_providers {
    google = {
        source = "hashicorp/google"
    }
  }
}

provider "google" {
    credentials = file("C:/msys64/home/user/SkinCare/include/gcp/service_account.json")
    project = var.project_id
    region = var.region
  
}

module "gcs" {
  source        = "./modules/gcs"  
  project_name  = var.project_name
  project_id    = var.project_id
  region        = var.region
  storage_class = var.storage_class
}

module "dataproc" {
  source        = "./modules/dataproc"
  cluster_name  = var.cluster_name
  region        = var.region
  machine_type  = var.machine_type
  service_account_email = var.service_account_email
}

module "bigquery" {
  source      = "./modules/bigquery"

  dataset_name = var.dataset_name
  project_id   = var.project_id
  region       = var.region
}
