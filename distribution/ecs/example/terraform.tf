terraform {
  backend "local" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.39.1"
    }
  }
}

provider "aws" {
  region = "eu-west-1"
  default_tags {
    tags = {
      provisioner = "terraform"
    }
  }
}

module "quickwit" {
  source                       = "../quickwit"
  vpc_id                       = module.vpc.vpc_id
  subnet_ids                   = module.vpc.private_subnets
  quickwit_ingress_cidr_blocks = [module.vpc.vpc_cidr_block]

  ## Optional configurations:

  # quickwit_index_s3_prefix  = "my-bucket/my-prefix"
  # quickwit_domain           = "quickwit"
  # quickwit_image            = "quickwit/quickwit:latest"
  # quickwit_cpu_architecture = "ARM64"

  # quickwit_indexer = {
  #   desired_count = 1
  #   memory        = 2048
  #   cpu           = 1024
  # }

  # quickwit_metastore = {
  #   desired_count = 1
  #   memory        = 512
  #   cpu           = 256
  # }

  # quickwit_searcher = {
  #   desired_count = 1
  #   memory        = 2048
  #   cpu           = 1024
  # }

  # quickwit_control_plane = {
  #   memory = 512
  #   cpu    = 256
  # }

  # quickwit_janitor = {
  #   memory = 512
  #   cpu    = 256
  # }

  # rds_config = {
  #   instance_class = "db.t4g.micro"
  #   multi_az       = false
  # }

  # sidecar_container_definitions  = local.example_sidecar_container_definitions
  # sidecar_container_dependencies = local.example_sidecar_container_dependencies
  # log_configuration              = local.example_log_configuration
  # enable_cloudwatch_logging      = true
}


output "indexer_service_name" {
  value = module.quickwit.indexer_service_name
}

output "searcher_service_name" {
  value = module.quickwit.searcher_service_name
}
