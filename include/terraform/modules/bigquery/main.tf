resource "google_bigquery_dataset" "skincare_processed" {
  dataset_id                  = var.dataset_name 
  project                     = var.project_id
  location                    = var.region
  description                 = "Processed skincare data using PySpark"
}
