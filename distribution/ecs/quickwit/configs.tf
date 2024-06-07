locals {
  quickwit_peer_list = [
    "${aws_service_discovery_service.metastore.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}",
    "${aws_service_discovery_service.control_plane.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}",
    "${aws_service_discovery_service.janitor.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}",
    "${aws_service_discovery_service.indexer.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}",
    "${aws_service_discovery_service.searcher.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}",
  ]

  # id to avoid conflicts when deploying this module multiple times (random by default)
  module_id = var.module_id == "" ? random_id.module.hex : var.module_id
  s3_id     = var.module_id == "" ? random_id.module.hex : "${var.module_id}-${random_id.module.hex}"

  quickwit_index_s3_prefix = var.quickwit_index_s3_prefix == "" ? aws_s3_bucket.index[0].id : var.quickwit_index_s3_prefix

  use_external_rds        = var.external_postgres_uri_secret_arn != ""
  postgres_uri_secret_arn = var.external_postgres_uri_secret_arn != "" ? var.external_postgres_uri_secret_arn : aws_ssm_parameter.postgres_credential[0].arn
}

resource "random_id" "module" {
  byte_length = 3
}
