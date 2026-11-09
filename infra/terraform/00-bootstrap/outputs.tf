# These values are not secret. They are published as GitHub repository variables
# by `scripts/tf.py gh-vars`.
output "project_id" {
  value = local.project_id
}

output "region" {
  value = local.region
}

output "state_bucket" {
  value = google_storage_bucket.tf_state.name
}

output "workload_identity_provider" {
  description = "Full resource name used by google-github-actions/auth."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "service_accounts" {
  value = { for k, sa in google_service_account.ci : k => sa.email }
}

output "github_variables" {
  description = "Repository variables consumed by the GitHub Actions workflows."
  value = {
    GCP_PROJECT_ID             = local.project_id
    GCP_REGION                 = local.region
    GCP_WIF_PROVIDER           = google_iam_workload_identity_pool_provider.github.name
    GCP_SA_TF_PLAN             = google_service_account.ci["tf_plan"].email
    GCP_SA_TF_APPLY            = google_service_account.ci["tf_apply"].email
    GCP_SA_CLOUDBUILD_DEPLOYER = google_service_account.ci["cloudbuild_deployer"].email
    TF_STATE_BUCKET            = google_storage_bucket.tf_state.name
  }
}
