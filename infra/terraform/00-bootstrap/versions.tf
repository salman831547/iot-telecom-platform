terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.10"
    }
  }

  # Partial configuration: bucket/prefix are injected from config/envs/<env>.yaml by
  # scripts/tf.py (`-backend-config`). The very first apply runs with a temporary
  # local-backend override because this layer creates the state bucket itself.
  backend "gcs" {}
}

provider "google" {
  project               = local.project_id
  region                = local.region
  default_labels        = local.labels
  user_project_override = true
  billing_project       = local.project_id
}
