variable "dockerhub_pull_through_creds_secret_arn" {
  description = "If left empty, image is pulled directly from Docker Hub, which might be throttled."
  default     = ""
}

locals {
  ecr_repository_prefix = "quickwit-ecs-example"
}

# This repo is populated by the pull through cache below
resource "aws_ecr_repository" "quickwit" {
  count                = var.dockerhub_pull_through_creds_secret_arn == "" ? 0 : 1
  name                 = "${local.ecr_repository_prefix}/quickwit/quickwit"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = false
  }
}

resource "aws_ecr_pull_through_cache_rule" "docker_hub" {
  count                 = var.dockerhub_pull_through_creds_secret_arn == "" ? 0 : 1
  ecr_repository_prefix = local.ecr_repository_prefix
  upstream_registry_url = "registry-1.docker.io"
  credential_arn        = var.dockerhub_pull_through_creds_secret_arn
}


locals {
  ecr_domain     = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${data.aws_region.current.name}.amazonaws.com"
  image_prefix   = var.dockerhub_pull_through_creds_secret_arn == "" ? "" : "${local.ecr_domain}/${local.ecr_repository_prefix}/"
  quickwit_image = "${local.image_prefix}quickwit/quickwit"
}
