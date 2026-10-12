# Conductor on Google Cloud: a GKE Autopilot cluster, a highly available
# Cloud SQL Postgres reachable only on a private IP, KEDA for worker
# autoscaling, Conductor itself (the chart in ../../helm/conductor), and
# Google Cloud Managed Service for Prometheus scraping it.

locals {
  labels = { app = "conductor", managed-by = "terraform" }
}

resource "google_project_service" "apis" {
  for_each = toset([
    "container.googleapis.com",
    "sqladmin.googleapis.com",
    "servicenetworking.googleapis.com",
    "monitoring.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# --- Network ---

resource "google_compute_network" "vpc" {
  name                    = "${var.name}-vpc"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "nodes" {
  name                     = "${var.name}-nodes"
  network                  = google_compute_network.vpc.id
  region                   = var.region
  ip_cidr_range            = "10.10.0.0/20"
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.20.0.0/14"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.30.0.0/20"
  }
}

# Private nodes reach the internet (images, http tasks) through Cloud NAT.
resource "google_compute_router" "nat" {
  name    = "${var.name}-router"
  network = google_compute_network.vpc.id
  region  = var.region
}

resource "google_compute_router_nat" "nat" {
  name                               = "${var.name}-nat"
  router                             = google_compute_router.nat.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# Cloud SQL gets a private IP in a range peered with the VPC.
resource "google_compute_global_address" "sql" {
  name          = "${var.name}-sql-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "sql" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.sql.name]
}

# --- Cluster ---

# Autopilot: Google runs the nodes, with Dataplane V2 (which enforces the
# chart's NetworkPolicies), Workload Identity and Managed Prometheus on.
resource "google_container_cluster" "conductor" {
  name                = var.name
  location            = var.region
  enable_autopilot    = true
  network             = google_compute_network.vpc.id
  subnetwork          = google_compute_subnetwork.nodes.id
  deletion_protection = var.deletion_protection
  resource_labels     = local.labels

  release_channel {
    channel = "REGULAR"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  dynamic "master_authorized_networks_config" {
    for_each = length(var.api_authorized_networks) > 0 ? [1] : []
    content {
      dynamic "cidr_blocks" {
        for_each = var.api_authorized_networks
        content {
          cidr_block = cidr_blocks.value
        }
      }
    }
  }

  monitoring_config {
    managed_prometheus {
      enabled = true
    }
  }

  depends_on = [google_project_service.apis]
}

# --- Database ---

resource "google_sql_database_instance" "conductor" {
  name                = "${var.name}-pg"
  region              = var.region
  database_version    = "POSTGRES_16"
  deletion_protection = var.deletion_protection

  settings {
    tier              = var.database_tier
    availability_type = var.database_high_availability ? "REGIONAL" : "ZONAL"
    user_labels       = local.labels

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "02:00"
    }

    insights_config {
      query_insights_enabled = true
    }

    database_flags {
      # Coordinators, the API and workers' pools add up; leave headroom.
      name  = "max_connections"
      value = "400"
    }
  }

  depends_on = [google_service_networking_connection.sql]
}

resource "google_sql_database" "conductor" {
  name     = "conductor"
  instance = google_sql_database_instance.conductor.name
}

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "google_sql_user" "conductor" {
  name     = "conductor"
  instance = google_sql_database_instance.conductor.name
  password = random_password.db.result
}

# --- In the cluster ---

resource "kubernetes_namespace_v1" "conductor" {
  metadata {
    name = "conductor"
  }
}

resource "kubernetes_secret_v1" "db" {
  metadata {
    name      = "conductor-db"
    namespace = kubernetes_namespace_v1.conductor.metadata[0].name
  }
  data = {
    password = random_password.db.result
  }
}

resource "helm_release" "keda" {
  name             = "keda"
  repository       = "https://kedacore.github.io/charts"
  chart            = "keda"
  version          = "2.18.1"
  namespace        = "keda"
  create_namespace = true
}

resource "helm_release" "conductor" {
  name      = "conductor"
  chart     = "${path.module}/../../helm/conductor"
  namespace = kubernetes_namespace_v1.conductor.metadata[0].name
  timeout   = 900

  values = [yamlencode({
    image = { repository = var.conductor_image, tag = var.image_tag }
    postgres = {
      mode     = "external"
      database = google_sql_database.conductor.name
      external = {
        host           = google_sql_database_instance.conductor.private_ip_address
        user           = google_sql_user.conductor.name
        existingSecret = kubernetes_secret_v1.db.metadata[0].name
        sslMode        = "require"
      }
    }
    worker = {
      autoscaling = { enabled = true, minReplicas = 0, maxReplicas = var.max_workers }
    }
    kubernetesTasks = { enabled = true }
    operator        = { enabled = true, image = { repository = var.operator_image, tag = var.image_tag } }
    metrics = {
      googleManagedPrometheus = { enabled = true }
    }
    networkPolicy = { kedaNamespace = helm_release.keda.namespace }
  })]

  depends_on = [helm_release.keda, google_sql_user.conductor]
}
