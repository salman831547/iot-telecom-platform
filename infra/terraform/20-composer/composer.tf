# The ONLY resource in this layer: a disposable Cloud Composer 3 environment.
#
# It is safe to destroy and recreate because nothing it holds is the source of truth:
#   - DAGs, library, SQL and config live in git and are synced into OUR bucket
#     (storage_config.bucket, created in 10-foundation), which outlives every
#     environment instance.
#   - Data lives in GCS staging and BigQuery; pipeline progress lives in iot_ops.watermarks.
#   - Only Airflow's own run history (its metadata DB) is lost.
resource "google_composer_environment" "this" {
  name    = local.composer.environment_name
  project = local.project_id
  region  = local.region
  labels  = local.labels

  storage_config {
    bucket = local.cfg.storage.composer_bucket.name
  }

  config {
    environment_size           = local.composer.environment_size
    resilience_mode            = local.composer.resilience_mode
    enable_private_environment = local.composer.private_environment

    software_config {
      image_version            = local.composer.image_version
      airflow_config_overrides = local.composer.airflow_config_overrides
      env_variables            = local.composer.env_variables
    }

    workloads_config {
      scheduler {
        cpu        = local.w.scheduler.cpu
        memory_gb  = local.w.scheduler.memory_gb
        storage_gb = local.w.scheduler.storage_gb
        count      = local.w.scheduler.count
      }
      web_server {
        cpu        = local.w.web_server.cpu
        memory_gb  = local.w.web_server.memory_gb
        storage_gb = local.w.web_server.storage_gb
      }
      triggerer {
        cpu       = local.w.triggerer.cpu
        memory_gb = local.w.triggerer.memory_gb
        count     = local.w.triggerer.count
      }
      worker {
        cpu        = local.w.worker.cpu
        memory_gb  = local.w.worker.memory_gb
        storage_gb = local.w.worker.storage_gb
        min_count  = local.w.worker.min_count
        max_count  = local.w.worker.max_count
      }
    }

    node_config {
      service_account = local.runtime_sa
      network         = local.composer.attach_vpc ? local.network_id : null
      subnetwork      = local.composer.attach_vpc ? local.subnetwork_id : null
    }
  }

  timeouts {
    create = "75m"
    update = "75m"
    delete = "45m"
  }
}
