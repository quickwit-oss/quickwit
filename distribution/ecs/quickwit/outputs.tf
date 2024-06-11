output "indexer_service_name" {
  value = "${aws_service_discovery_service.indexer.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}"
}

output "searcher_service_name" {
  value = "${aws_service_discovery_service.searcher.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}"
}

output "janitor_service_name" {
  value = "${aws_service_discovery_service.janitor.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}"
}

output "control_plane_service_name" {
  value = "${aws_service_discovery_service.control_plane.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}"
}

output "metastore_service_name" {
  value = "${aws_service_discovery_service.metastore.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}"
}
