# Project-level role sets come from config (iam.*); changing access is a reviewed PR.
locals {
  project_role_bindings = merge(
    { for r in local.iam.tf_plan_project_roles : "tf_plan/${r}" => { sa = "tf_plan", role = r } },
    { for r in local.iam.tf_apply_project_roles : "tf_apply/${r}" => { sa = "tf_apply", role = r } },
    { for r in local.iam.cloudbuild_deployer_project_roles : "cloudbuild_deployer/${r}" => { sa = "cloudbuild_deployer", role = r } },
  )
}

resource "google_project_iam_member" "ci" {
  for_each = local.project_role_bindings

  project = local.project_id
  role    = each.value.role
  member  = google_service_account.ci[each.value.sa].member
}

# tf_apply may manage project IAM, but ONLY grant/revoke the allow-listed roles.
# This blocks privilege escalation (e.g. granting itself roles/owner).
resource "google_project_iam_member" "tf_apply_constrained_iam_admin" {
  project = local.project_id
  role    = "roles/resourcemanager.projectIamAdmin"
  member  = google_service_account.ci["tf_apply"].member

  condition {
    title       = "only-allowlisted-roles"
    description = "tf_apply can only grant roles listed in config iam.tf_apply_grantable_roles"
    expression  = "api.getAttribute('iam.googleapis.com/modifiedGrantsByRole', []).hasOnly(${jsonencode(local.iam.tf_apply_grantable_roles)})"
  }
}

# tf_apply submits Cloud Build jobs that run AS the deployer SA (actAs on that SA only).
resource "google_service_account_iam_member" "tf_apply_acts_as_deployer" {
  service_account_id = google_service_account.ci["cloudbuild_deployer"].name
  role               = "roles/iam.serviceAccountUser"
  member             = google_service_account.ci["tf_apply"].member
}
