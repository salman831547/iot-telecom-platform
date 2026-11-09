# Identities used by automation. The Composer runtime SA lives in 10-foundation,
# next to the resources it is granted access to.
locals {
  ci_service_accounts = {
    tf_plan = {
      id          = local.sa.tf_plan
      description = "GitHub Actions: read-only terraform plan on pull requests"
    }
    tf_apply = {
      id          = local.sa.tf_apply
      description = "GitHub Actions: terraform apply for foundation/composer (deploy branch + environment only)"
    }
    cloudbuild_deployer = {
      id          = local.sa.cloudbuild_deployer
      description = "Cloud Build: syncs DAGs/SQL/config to the Composer bucket"
    }
  }
}

resource "google_service_account" "ci" {
  for_each = local.ci_service_accounts

  project      = local.project_id
  account_id   = each.value.id
  display_name = each.value.id
  description  = each.value.description

  depends_on = [google_project_service.this]
}
