locals {
  config_dir = coalesce(var.config_dir, "${path.module}/../../../config/envs")
  cfg        = yamldecode(file("${local.config_dir}/${var.env}.yaml"))

  project_id = local.cfg.project.id
  region     = local.cfg.project.region
  labels     = merge(local.cfg.labels, { layer = "composer" })
  composer   = local.cfg.composer
  w          = local.composer.workloads

  runtime_sa = "${local.cfg.service_accounts.composer_runtime}@${local.project_id}.iam.gserviceaccount.com"

  # Everything this layer depends on lives in 10-foundation and is referenced by NAME
  # (from config), not via remote state. Destroying this layer can never touch it.
  network_id    = "projects/${local.project_id}/global/networks/${local.cfg.network.vpc_name}"
  subnetwork_id = "projects/${local.project_id}/regions/${local.region}/subnetworks/${local.cfg.network.subnet_name}"
}
