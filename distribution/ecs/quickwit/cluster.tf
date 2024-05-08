module "ecs_cluster" {
  source  = "terraform-aws-modules/ecs/aws//modules/cluster"
  version = "5.9.3"

  cluster_name = "quickwit-${local.module_id}"
}

resource "aws_service_discovery_private_dns_namespace" "quickwit_internal" {
  name        = var.quickwit_domain
  description = "Internal quickwit domain"
  vpc         = var.vpc_id
}

resource "aws_security_group" "quickwit_cluster_member_sg" {
  name        = "quickwit-cluster-member-${local.module_id}"
  description = "Security group for members of the Quickwit cluster"
  vpc_id      = var.vpc_id
}
