module "quickwit_searcher" {
  source                         = "./service"
  service_name                   = "searcher"
  service_discovery_registry_arn = aws_service_discovery_service.searcher.arn
  cluster_arn                    = module.ecs_cluster.arn
  postgres_credential_arn        = local.postgres_uri_parameter_arn
  quickwit_peer_list             = local.quickwit_peer_list
  s3_access_policy_arn           = aws_iam_policy.quickwit_task_permission.arn
  task_execution_policy_arn      = aws_iam_policy.quickwit_task_execution_permission.arn
  module_id                      = local.module_id
  quickwit_cluster_member_sg_id  = aws_security_group.quickwit_cluster_member_sg.id

  subnet_ids                     = var.subnet_ids
  ingress_cidr_blocks            = var.quickwit_ingress_cidr_blocks
  quickwit_image                 = var.quickwit_image
  quickwit_cpu_architecture      = var.quickwit_cpu_architecture
  sidecar_container_definitions  = var.sidecar_container_definitions
  sidecar_container_dependencies = var.sidecar_container_dependencies
  log_configuration              = var.log_configuration
  enable_cloudwatch_logging      = var.enable_cloudwatch_logging
  service_config                 = var.quickwit_searcher
  quickwit_index_s3_prefix       = local.quickwit_index_s3_prefix
}

resource "aws_service_discovery_service" "searcher" {
  name = "searcher"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.quickwit_internal.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }
}
