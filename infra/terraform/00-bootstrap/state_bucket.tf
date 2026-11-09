# Remote Terraform state for every layer (prefix per layer/env). GCS gives native
# state locking; versioning + soft delete allow recovery from a corrupted state.
resource "google_storage_bucket" "tf_state" {
  name          = local.tf.state_bucket
  project       = local.project_id
  location      = upper(local.region)
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  soft_delete_policy {
    retention_duration_seconds = 7 * 24 * 60 * 60
  }

  # Keep the last 20 superseded state versions; prune older ones.
  lifecycle_rule {
    condition {
      with_state         = "ARCHIVED"
      num_newer_versions = 20
    }
    action {
      type = "Delete"
    }
  }

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [google_project_service.this]
}

# --- State access -------------------------------------------------------------
# tf_plan: read-only. PR plans run with -lock=false, so no write access is needed.
resource "google_storage_bucket_iam_member" "state_plan_reader" {
  bucket = google_storage_bucket.tf_state.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.ci["tf_plan"].member
}

# tf_apply: read everything (backend init lists objects) ...
resource "google_storage_bucket_iam_member" "state_apply_reader" {
  bucket = google_storage_bucket.tf_state.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.ci["tf_apply"].member
}

# ... but write ONLY the foundation/composer prefixes. It cannot tamper with bootstrap
# state, which holds its own permissions.
resource "google_storage_bucket_iam_member" "state_apply_writer" {
  bucket = google_storage_bucket.tf_state.name
  role   = "roles/storage.objectUser"
  member = google_service_account.ci["tf_apply"].member

  condition {
    title       = "tf-apply-writable-state-prefixes"
    description = "Only foundation/composer state objects"
    expression = join(" || ", [
      for p in local.apply_writable_state_prefixes :
      "resource.name.startsWith(\"projects/_/buckets/${local.tf.state_bucket}/objects/${p}/\")"
    ])
  }
}
