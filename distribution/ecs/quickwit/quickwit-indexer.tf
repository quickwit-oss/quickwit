module "quickwit_indexer" {
  source                         = "./service"
  service_name                   = "indexer"
  service_discovery_registry_arn = aws_service_discovery_service.indexer.arn
  cluster_arn                    = module.ecs_cluster.arn
  postgres_uri_secret_arn        = local.postgres_uri_secret_arn
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
  service_config                 = var.quickwit_indexer
  quickwit_index_s3_prefix       = local.quickwit_index_s3_prefix
  # Longer termination grace period for indexers because we are waiting for the
  # data persisted in the ingesters to be indexed and committed. Should be
  # larger than the largest commit timeout.
  stop_timeout = 120
}

resource "aws_service_discovery_service" "indexer" {
  name = "indexer"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.quickwit_internal.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }
}
