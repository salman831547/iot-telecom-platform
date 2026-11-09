output "staging_bucket" {
  value = google_storage_bucket.staging.name
}

output "composer_bucket" {
  value = google_storage_bucket.composer.name
}

output "composer_runtime_sa" {
  value = google_service_account.composer_runtime.email
}

output "network" {
  value = {
    vpc    = google_compute_network.vpc.id
    subnet = google_compute_subnetwork.composer.id
  }
}

output "datasets" {
  value = { for k, d in google_bigquery_dataset.this : k => d.dataset_id }
}

output "tables" {
  value = sort([for t in google_bigquery_table.this : "${t.dataset_id}.${t.table_id}"])
}

output "pii_salt_secret" {
  value = google_secret_manager_secret.pii_hash_salt.secret_id
}
