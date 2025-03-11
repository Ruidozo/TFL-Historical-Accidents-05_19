variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud Region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
}

variable "service_account_email" {
  description = "Email of the service account that needs access to GCS"
  type        = string
}

variable "service_account_key_file" {
  description = "Path to the service account key file"
  type        = string
  default     = "secrets/gcp_credentials.json"
}
