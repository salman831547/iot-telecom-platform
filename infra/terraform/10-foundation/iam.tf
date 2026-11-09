# =============================================================================
# Composer runtime identity: what the Airflow scheduler and workers run as.
# =============================================================================
resource "google_service_account" "composer_runtime" {
  project      = local.project_id
  account_id   = local.sa.composer_runtime
  display_name = local.sa.composer_runtime
  description  = "Cloud Composer runtime (Airflow scheduler/workers) for ${local.cfg.env}"
}

# Project-level roles the runtime needs. These are all on tf_apply's grantable allow-list.
# roles/composer.worker is required by Composer and is broad (project-wide storage object
# access). The bucket-level grants below record the intended data access explicitly.
resource "google_project_iam_member" "composer_runtime" {
  for_each = toset([
    "roles/composer.worker",
    "roles/bigquery.jobUser",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])

  project = local.project_id
  role    = each.value
  member  = google_service_account.composer_runtime.member
}

# The Composer service agent must be able to manage the runtime SA. This is granted on
# the SA itself, not project-wide.
resource "google_service_account_iam_member" "composer_agent_ext" {
  service_account_id = google_service_account.composer_runtime.name
  role               = "roles/composer.ServiceAgentV2Ext"
  member             = "serviceAccount:${local.composer_service_agent}"
}

# CI (tf_apply) creates the Composer environment, which runs AS the runtime SA.
resource "google_service_account_iam_member" "tf_apply_acts_as_runtime" {
  service_account_id = google_service_account.composer_runtime.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${local.sa_email.tf_apply}"
}

# --- Data access: scoped to individual resources -------------------------------------
resource "google_storage_bucket_iam_member" "runtime_staging" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectUser"
  member = google_service_account.composer_runtime.member
}

resource "google_storage_bucket_iam_member" "runtime_composer_bucket" {
  bucket = google_storage_bucket.composer.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.composer_runtime.member
}

resource "google_bigquery_dataset_iam_member" "runtime_editor" {
  for_each = google_bigquery_dataset.this

  project    = local.project_id
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.composer_runtime.member
}

resource "google_secret_manager_secret_iam_member" "runtime_pii_salt" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.pii_hash_salt.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.composer_runtime.member
}

# =============================================================================
# Cloud Build deployer: may write only the code/config prefixes of the Composer
# bucket. It cannot touch logs or any data.
# =============================================================================
resource "google_storage_bucket_iam_member" "deployer_read" {
  bucket = google_storage_bucket.composer.name
  role   = "roles/storage.objectViewer" # list + get (rsync diffing)
  member = "serviceAccount:${local.sa_email.cloudbuild_deployer}"
}

resource "google_storage_bucket_iam_member" "deployer_write" {
  bucket = google_storage_bucket.composer.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${local.sa_email.cloudbuild_deployer}"

  condition {
    title       = "deploy-prefixes-only"
    description = "dags/, plugins/ and data/ only (no logs)"
    expression = join(" || ", [
      for p in ["dags/", "plugins/", "data/"] :
      "resource.name.startsWith(\"projects/_/buckets/${google_storage_bucket.composer.name}/objects/${p}\")"
    ])
  }
}

# --- Cloud Build sources: submitters write, deployer reads -----------------------
resource "google_storage_bucket_iam_member" "cloudbuild_sources_deployer_read" {
  bucket = google_storage_bucket.cloudbuild.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${local.sa_email.cloudbuild_deployer}"
}

resource "google_storage_bucket_iam_member" "cloudbuild_sources_ci_write" {
  bucket = google_storage_bucket.cloudbuild.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${local.sa_email.tf_apply}"
}

# Composer service agent manages the environment's folders in OUR bucket
# (custom storage_config.bucket). Bucket-scoped, not project-wide.
resource "google_storage_bucket_iam_member" "composer_agent_bucket" {
  bucket = google_storage_bucket.composer.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.composer_service_agent}"
}
