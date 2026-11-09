# Keyless auth for GitHub Actions: GitHub OIDC token -> STS -> short-lived SA token.
resource "google_iam_workload_identity_pool" "github" {
  project                   = local.project_id
  workload_identity_pool_id = local.wif.pool_id
  display_name              = "GitHub Actions"
  description               = "OIDC federation for ${local.gh_repository}"

  depends_on = [google_project_service.this]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  project                            = local.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = local.wif.provider_id
  display_name                       = "GitHub OIDC"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  attribute_mapping = {
    "google.subject"             = "assertion.sub"
    "attribute.actor"            = "assertion.actor"
    "attribute.repository"       = "assertion.repository"
    "attribute.repository_owner" = "assertion.repository_owner"
    "attribute.ref"              = "assertion.ref"
    "attribute.deploy"           = "(has(assertion.environment) ? assertion.environment : 'none') + '@' + assertion.ref"
  }

  # Hard gate: reject every token that is not from this exact repo AND owner id.
  attribute_condition = join(" && ", [
    "assertion.repository_owner_id == '${local.gh.owner_id}'",
    "assertion.repository == '${local.gh_repository}'",
  ])
}

# tf_plan: any workflow in the repo (incl. pull requests) may impersonate.
resource "google_service_account_iam_member" "wif_plan" {
  service_account_id = google_service_account.ci["tf_plan"].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${local.pool_name}/attribute.repository/${local.gh_repository}"
}

# tf_apply: only jobs on the deploy branch running inside the gated GitHub Environment.
resource "google_service_account_iam_member" "wif_apply" {
  service_account_id = google_service_account.ci["tf_apply"].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${local.pool_name}/attribute.deploy/${local.apply_deploy_attribute}"
}
