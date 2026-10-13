variable "project_id" {
  description = "Google Cloud project to create everything in."
  type        = string
}

variable "region" {
  description = "Region for the cluster and the database."
  type        = string
  default     = "europe-west1"
}

variable "name" {
  description = "Prefix for every resource's name."
  type        = string
  default     = "conductor"
}

variable "conductor_image" {
  description = "Conductor image repository (the tag is image_tag)."
  type        = string
  default     = "ghcr.io/chinmaynoob/conductor"
}

variable "operator_image" {
  description = "Operator image repository (the tag is image_tag)."
  type        = string
  default     = "ghcr.io/chinmaynoob/conductor-operator"
}

variable "image_tag" {
  description = "Conductor version to run."
  type        = string
  default     = "dev"
}

variable "database_tier" {
  description = "Cloud SQL machine type. db-custom-<vCPUs>-<MB>."
  type        = string
  default     = "db-custom-2-7680"
}

variable "database_high_availability" {
  description = "Run Cloud SQL with a standby in another zone (REGIONAL)."
  type        = bool
  default     = true
}

variable "max_workers" {
  description = "Most workers KEDA may scale the default pool to (it scales down to zero when idle)."
  type        = number
  default     = 20
}

variable "api_authorized_networks" {
  description = "CIDR ranges allowed to reach the cluster's control plane, e.g. your office."
  type        = list(string)
  default     = []
}

variable "deletion_protection" {
  description = "Refuse to destroy the cluster and the database. Turn off only to tear down."
  type        = bool
  default     = true
}
