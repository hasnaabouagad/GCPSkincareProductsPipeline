resource "google_storage_bucket" "skin_care_bucket" {
  name          = "${var.project_name}-${var.project_id}"
  location      = var.region
  force_destroy = true
  storage_class = var.storage_class
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }

   
  lifecycle_rule {
    condition {
      age = 40
    }
    action {
      type = "Delete"
    }
  }
}



