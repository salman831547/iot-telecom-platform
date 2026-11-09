# Private network for Composer 3 workloads. No Cloud NAT on purpose: Composer runs as a
# private environment with no internet egress. Google APIs are reached through
# Private Google Access.
resource "google_compute_network" "vpc" {
  name                            = local.network.vpc_name
  project                         = local.project_id
  auto_create_subnetworks         = false
  routing_mode                    = "REGIONAL"
  delete_default_routes_on_create = false
}

resource "google_compute_subnetwork" "composer" {
  name                     = local.network.subnet_name
  project                  = local.project_id
  region                   = local.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = local.network.subnet_cidr
  private_ip_google_access = true

  log_config {
    aggregation_interval = "INTERVAL_10_MIN"
    flow_sampling        = 0.1
    metadata             = "INCLUDE_ALL_METADATA"
  }
}

# Explicit deny-all ingress at the lowest priority: documents intent and guards against
# someone adding a permissive rule without noticing the default.
resource "google_compute_firewall" "deny_all_ingress" {
  name      = "${local.network.vpc_name}-deny-all-ingress"
  project   = local.project_id
  network   = google_compute_network.vpc.id
  direction = "INGRESS"
  priority  = 65534

  deny {
    protocol = "all"
  }
  source_ranges = ["0.0.0.0/0"]

  log_config {
    metadata = "EXCLUDE_ALL_METADATA"
  }
}
