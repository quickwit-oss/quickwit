## REQUIRED VARIABLES

variable "vpc_id" {
  description = "VPC ID of the cluster"
}

variable "subnet_ids" {
  description = "Subnet(s) where quickwit will be deployed"
  type        = list(string)
}



## OPTIONAL VARIABLES

variable "module_id" {
  description = "Identifier for the module, e.g the stage. If not specified, a random string is generated."
  default     = ""
}

variable "quickwit_ingress_cidr_blocks" {
  description = "CIDR blocks (private) that should have access to the Quickwit cluster"
  type        = list(string)
  default     = []
}


variable "quickwit_index_s3_prefix" {
  description = "S3 bucket name and prefix for the Quickwit data, e.g. my-bucket-name/my-prefix. Quickwit will only have access to this S3 location. Leave empty to create a new bucket."
  default     = ""
}

variable "quickwit_domain" {
  description = "Local domain for quickwit service discovery"
  default     = "quickwit"
}

variable "quickwit_image" {
  description = "Quickwit docker image"
  default     = "quickwit/quickwit:latest"
}

variable "quickwit_cpu_architecture" {
  description = "One of X86_64 / ARM64. Must match the arch of the provided image (var.quickwit_image)."
  default     = "ARM64"
}

variable "sidecar_container_definitions" {
  description = "Sidecar containers to be attached to Quickwit tasks"
  default     = {}
}

variable "sidecar_container_dependencies" {
  description = "Specify the Quickwit container's dependencies on sidecars"
  type = list(object({
    containerName = string
    condition     = string
  }))
  default = []
}

variable "enable_cloudwatch_logging" {
  description = "Cloudwatch logging for Quickwit tasks. Usually disabled when using a custom log configuration."
  default     = true
}

variable "log_configuration" {
  description = "Custom log configuraiton for Quickwit tasks"
  default     = {}
}

variable "quickwit_indexer" {
  description = "Indexer service sizing configurations"
  type = object({
    desired_count = optional(number, 1)
    memory        = optional(number, 4096)
    cpu           = optional(number, 1024)
  })
  default = {}
}

variable "quickwit_metastore" {
  description = "Metastore service sizing configurations"
  type = object({
    desired_count = optional(number, 1)
    memory        = optional(number, 512)
    cpu           = optional(number, 256)
  })
  default = {}
}

variable "quickwit_searcher" {
  description = "Searcher service sizing configurations"
  type = object({
    desired_count = optional(number, 1)
    memory        = optional(number, 2048)
    cpu           = optional(number, 1024)
  })
  default = {}
}

variable "quickwit_control_plane" {
  description = "Control plane service sizing configurations"
  type = object({
    # only 1 task is necessary
    memory = optional(number, 512)
    cpu    = optional(number, 256)
  })
  default = {}
}

variable "quickwit_janitor" {
  description = "Janitor service sizing configurations"
  type = object({
    # only 1 task is necessary
    memory = optional(number, 512)
    cpu    = optional(number, 256)
  })
  default = {}
}

variable "rds_config" {
  description = "Configurations of the metastore RDS database. Enable multi_az to ensure high availability."
  type = object({
    instance_class = optional(string, "db.t4g.micro")
    multi_az       = optional(bool, false)
  })
  default = {}
}

variable "external_postgres_uri_parameter_arn" {
  description = "ARN of the SSM parameter containing the URI of a Postgres instance (postgres://{user}:{password}@{address}:{port}/{db_instance_name}). The Postgres instance should allow indbound connections from the subnets specified in `variable.subnet_ids`. If provided, the internal RDS will not be created and `var.rds_config` is ignored."
  default     = ""
}
