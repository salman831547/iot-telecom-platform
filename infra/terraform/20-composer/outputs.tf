output "environment_name" {
  value = google_composer_environment.this.name
}

output "airflow_uri" {
  value = google_composer_environment.this.config[0].airflow_uri
}

output "dag_gcs_prefix" {
  value = google_composer_environment.this.config[0].dag_gcs_prefix
}
