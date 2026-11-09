data "google_project" "this" {
  project_id = local.project_id
}

locals {
  repo_root  = "${path.module}/../../.."
  config_dir = coalesce(var.config_dir, "${local.repo_root}/config/envs")
  schema_dir = coalesce(var.schema_dir, "${local.repo_root}/schemas/bigquery")
  cfg        = yamldecode(file("${local.config_dir}/${var.env}.yaml"))

  project_id = local.cfg.project.id
  region     = local.cfg.project.region
  labels     = merge(local.cfg.labels, { layer = "foundation" })

  sa       = local.cfg.service_accounts
  storage  = local.cfg.storage
  bq       = local.cfg.bigquery
  network  = local.cfg.network
  composer = local.cfg.composer

  sa_email = { for k, id in local.sa : k => "${id}@${local.project_id}.iam.gserviceaccount.com" }

  # Normalise to one shape (YAML entries have different optional keys).
  datasets = {
    for k, d in local.bq.datasets : k => {
      id                        = d.id
      table_expiration_hours    = try(d.default_table_expiration_hours, null)
      partition_expiration_days = try(d.default_partition_expiration_days, null)
    }
  }

  # Google-managed Composer service agent (exists once composer.googleapis.com is enabled).
  composer_service_agent = "service-${data.google_project.this.number}@cloudcomposer-accounts.iam.gserviceaccount.com"

  # --- Tables as code ---------------------------------------------------------
  # Every schemas/bigquery/<dataset_key>/<table>.json becomes a table in that dataset.
  # The "landing" dataset is excluded: its tables are transient (created per load job).
  table_files = [
    for f in fileset(local.schema_dir, "*/*.json") : f
    if split("/", f)[0] != "landing"
  ]
  tables = {
    for f in local.table_files :
    "${split("/", f)[0]}.${trimsuffix(split("/", f)[1], ".json")}" => merge(
      jsondecode(file("${local.schema_dir}/${f}")),
      {
        dataset_key = split("/", f)[0]
        table_id    = trimsuffix(split("/", f)[1], ".json")
      }
    )
  }
}
