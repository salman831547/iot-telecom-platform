resource "google_bigquery_dataset" "this" {
  for_each = local.datasets

  dataset_id    = each.value.id
  project       = local.project_id
  location      = local.bq.location
  friendly_name = each.value.id
  description   = "IoT telecom platform: ${each.key} layer (${local.cfg.env})."

  default_table_expiration_ms = (
    each.value.table_expiration_hours == null ? null : each.value.table_expiration_hours * 3600 * 1000
  )
  default_partition_expiration_ms = (
    each.value.partition_expiration_days == null ? null : each.value.partition_expiration_days * 86400 * 1000
  )

  delete_contents_on_destroy = false
  labels                     = merge(local.labels, { data-layer = each.key })

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_table" "this" {
  for_each = local.tables

  project     = local.project_id
  dataset_id  = google_bigquery_dataset.this[each.value.dataset_key].dataset_id
  table_id    = each.value.table_id
  description = each.value.description
  schema      = jsonencode(each.value.schema)
  clustering  = try(each.value.clustering, null)

  dynamic "time_partitioning" {
    for_each = try(each.value.partitioning, null) == null ? [] : [each.value.partitioning]
    content {
      type  = time_partitioning.value.type
      field = time_partitioning.value.field
    }
  }
  require_partition_filter = try(each.value.require_partition_filter, false)

  # Guard rail: Terraform refuses to drop a table that holds data.
  deletion_protection = true

  labels = merge(local.labels, { data-layer = each.value.dataset_key })
}
