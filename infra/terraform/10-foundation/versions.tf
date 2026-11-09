terraform {
  # >= 1.11 for write-only arguments + ephemeral resources (secret never stored in state).
  required_version = ">= 1.11.0, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.10"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }

  # bucket/prefix injected from config by scripts/tf.py
  backend "gcs" {}
}

provider "google" {
  project               = local.project_id
  region                = local.region
  default_labels        = local.labels
  user_project_override = true
  billing_project       = local.project_id
}
