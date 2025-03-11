resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 365  # Automatically delete files after 1 year
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = true
}



# ✅ Grant Storage Admin Access to Service Account
resource "google_storage_bucket_iam_member" "sa_bucket_access" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${var.service_account_email}"
}
