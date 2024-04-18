output "indexer_service_name" {
  value = "${aws_service_discovery_service.indexer.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}"
}

output "searcher_service_name" {
  value = "${aws_service_discovery_service.searcher.name}.${aws_service_discovery_private_dns_namespace.quickwit_internal.name}"
}
