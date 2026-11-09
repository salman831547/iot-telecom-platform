# Every API the platform needs is declared in config; none is enabled by hand.
resource "google_project_service" "this" {
  for_each = toset(local.cfg.apis)

  project = local.project_id
  service = each.value

  # Never disable APIs on destroy: other workloads in the project may depend on them,
  # and disabling can cascade-delete resources.
  disable_on_destroy         = false
  disable_dependent_services = false
}
