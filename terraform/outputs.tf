output "bucket_name" {
  description = "The name of the created GCS bucket"
  value       = google_storage_bucket.data_lake.name
}

output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
}
