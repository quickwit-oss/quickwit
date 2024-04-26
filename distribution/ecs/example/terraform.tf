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

# resource "aws_ecr_repository" "quickwit" {
#   name                 = "quickwit"
#   force_delete         = true
#   image_tag_mutability = "MUTABLE"
# }

module "quickwit" {
  source                       = "../quickwit"
  vpc_id                       = module.vpc.vpc_id
  subnet_ids                   = module.vpc.private_subnets
  quickwit_ingress_cidr_blocks = [module.vpc.vpc_cidr_block]

  ## Optional configurations:

  # quickwit_index_s3_prefix  = "my-bucket/my-prefix"
  # quickwit_domain           = "quickwit"
  # quickwit_image = aws_ecr_repository.quickwit.repository_url
  # quickwit_cpu_architecture = "ARM64"

  # quickwit_indexer = {
  #   desired_count         = 3
  #   memory                = 8192
  #   cpu                   = 4096
  #   ephemeral_storage_gib = 50
  #   extra_task_policy_arns = ["arn:aws:iam::aws:policy/AmazonKinesisFullAccess"]
  # }

  # quickwit_metastore = {
  #   desired_count = 1
  #   memory        = 512
  #   cpu           = 256
  # }

  # quickwit_searcher = {
  #   desired_count         = 1
  #   memory                = 2048
  #   cpu                   = 1024
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

  # external_postgres_uri_ssm_parameter_arn = aws_ssm_parameter.postgres_uri.arn

  ## Example logging configuration 
  # sidecar_container_definitions  = {
  #   my_sidecar_container = see http://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerDefinition.html
  # }
  # sidecar_container_dependencies = [{condition = "START", containerName = "my_sidecar_container"}]
  # log_configuration              = see https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/ecs_service#log_configuration
  # enable_cloudwatch_logging      = false

  ## Example Kafka key injection (see kafka.tf)
  # sidecar_container_definitions  = local.example_kafka_sidecar_container_definitions
  # sidecar_container_dependencies = local.example_kafka_sidecar_container_dependencies
}


output "indexer_service_name" {
  value = module.quickwit.indexer_service_name
}

output "searcher_service_name" {
  value = module.quickwit.searcher_service_name
}
