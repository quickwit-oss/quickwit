variable "service_name" {
  description = "One of indexer, metastore, searcher, control_plane, janitor"
}

variable "service_discovery_registry_arn" {}

variable "sidecar_container_definitions" {}

variable "sidecar_container_dependencies" {
  type = list(object({
    containerName = string
    condition     = string
  }))
  default = []
}

variable "log_configuration" {}

variable "enable_cloudwatch_logging" {
  type = bool
}

variable "cluster_arn" {}

variable "ingress_cidr_blocks" {
  type = list(string)
}

variable "quickwit_cluster_member_sg_id" {}

variable "subnet_ids" {
  type = list(string)
}

variable "postgres_credential_arn" {}

variable "quickwit_image" {}

variable "service_config" {
  type = object({
    desired_count = optional(number, 1)
    memory        = number
    cpu           = number
    storage       = optional(number, 21)
  })
}

variable "quickwit_index_s3_prefix" {}

variable "quickwit_peer_list" {
  type = list(string)
}

variable "s3_access_policy_arn" {}

variable "task_execution_policy_arn" {}

variable "quickwit_cpu_architecture" {}

variable "module_id" {}
