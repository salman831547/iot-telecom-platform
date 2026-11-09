locals {
  config_dir = coalesce(var.config_dir, "${path.module}/../../../config/envs")
  cfg        = yamldecode(file("${local.config_dir}/${var.env}.yaml"))

  project_id = local.cfg.project.id
  region     = local.cfg.project.region
  labels     = local.cfg.labels

  gh            = local.cfg.github
  gh_repository = "${local.gh.owner}/${local.gh.repo}"

  sa        = local.cfg.service_accounts
  iam       = local.cfg.iam
  tf        = local.cfg.terraform
  wif       = local.cfg.workload_identity
  pool_name = google_iam_workload_identity_pool.github.name

  # Custom WIF attribute "<github-environment>@<git-ref>". tf_apply is bound to exactly
  # "<env>@refs/heads/<deploy_branch>", so only jobs on the deploy branch that run in the
  # gated GitHub Environment can impersonate it. PR jobs (refs/pull/*) never match.
  apply_deploy_attribute = "${local.gh.environment}@refs/heads/${local.gh.deploy_branch}"

  # State object prefixes tf_apply may write (it must NOT be able to rewrite bootstrap state).
  apply_writable_state_prefixes = [local.tf.state_prefixes.foundation, local.tf.state_prefixes.composer]
}
