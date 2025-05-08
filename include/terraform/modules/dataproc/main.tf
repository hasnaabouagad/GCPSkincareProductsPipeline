resource "google_dataproc_cluster" "skincare_cluster" {
  name   = var.cluster_name 
  region = var.region
  graceful_decommission_timeout = "120s"

  cluster_config {
    master_config {
      num_instances    = 1
      machine_type     = var.machine_type 
      disk_config {
        boot_disk_size_gb = 30
      }
    }

    gce_cluster_config {
      service_account = var.service_account_email
    
    }
    software_config {
      image_version = "2.1-debian11"
    }
  }
}


