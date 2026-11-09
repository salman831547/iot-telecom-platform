# --- Staging (raw landing zone) ---------------------------------------------------
resource "google_storage_bucket" "staging" {
  name          = local.storage.staging_bucket.name
  project       = local.project_id
  location      = local.storage.location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = local.storage.staging_bucket.versioning
  }

  # Raw files are immutable and can be replayed. Cool them down, then expire them.
  lifecycle_rule {
    condition {
      age = local.storage.staging_bucket.nearline_after_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = local.storage.staging_bucket.delete_after_days
    }
    action {
      type = "Delete"
    }
  }

  labels = merge(local.labels, { data-layer = "raw" })

  lifecycle {
    prevent_destroy = true
  }
}

# --- Composer environment bucket (owned by us, outlives every Composer instance) ---
resource "google_storage_bucket" "composer" {
  name          = local.storage.composer_bucket.name
  project       = local.project_id
  location      = local.storage.location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = local.storage.composer_bucket.versioning
  }

  # Keep a bounded history of DAG/config versions (fast rollback, bounded cost).
  lifecycle_rule {
    condition {
      with_state         = "ARCHIVED"
      num_newer_versions = local.storage.composer_bucket.noncurrent_versions_to_keep
    }
    action {
      type = "Delete"
    }
  }

  labels = merge(local.labels, { data-layer = "orchestration" })

  lifecycle {
    prevent_destroy = true
  }
}
