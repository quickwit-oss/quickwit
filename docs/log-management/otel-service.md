---
title: OTEL service
sidebar_position: 4
---

Quickwit natively supports the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/reference/specification/protocol/otlp/) and provides a gRPC endpoint to receive spans from an OpenTelemetry collector. This endpoint is enabled by default.

When enabled, Quickwit will start the gRPC service ready to receive logs from an OpenTelemetry collector. The logs are indexed in the `otel-logs-v0_7` index by default, and this index will be automatically created if not present. The index doc mapping is described in the next [section](#trace-and-span-data-model).

If for any reason, you want to disable this endpoint, you can:
- Set the `QW_ENABLE_OTLP_ENDPOINT` environment variable to `false` when starting Quickwit.
- Or [configure the node config](/docs/configuration/node-config.md) by setting the indexer setting `enable_otlp_endpoint` to `false`.

```yaml title=node-config.yaml
# ... Indexer configuration ...
indexer:
    enable_otlp_endpoint: false
```

## Sending logs in your own index

You can send logs in the index of your choice by setting the header `qw-otel-logs-index` of your gRPC request to the targeted index ID.


## OpenTelemetry logs data model

Quickwit sends OpenTelemetry logs into the `otel-logs-v0_7` index by default which is automatically created if you enable the OpenTelemetry service.
The doc mapping of this index described below is derived from the [OpenTelemetry logs data model](https://opentelemetry.io/docs/reference/specification/logs/data-model/).

```yaml

version: 0.7

index_id: otel-logs-v0_7

doc_mapping:
  mode: strict
  field_mappings:
    - name: timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
      indexed: false
      fast: true
      fast_precision: milliseconds
    - name: observed_timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
    - name: service_name
      type: text
      tokenizer: raw
      fast: true
    - name: severity_text
      type: text
      tokenizer: raw
      fast: true
    - name: severity_number
      type: u64
      fast: true
    - name: body
      type: json
      tokenizer: default
    - name: attributes
      type: json
      tokenizer: raw
      fast: true
    - name: dropped_attributes_count
      type: u64
      indexed: false
    - name: trace_id
      type: bytes
      input_format: hex
      output_format: hex
    - name: span_id
      type: bytes
      input_format: hex
      output_format: hex
    - name: trace_flags
      type: u64
      indexed: false
    - name: resource_attributes
      type: json
      tokenizer: raw
      fast: true
    - name: resource_dropped_attributes_count
      type: u64
      indexed: false
    - name: scope_name
      type: text
      indexed: false
    - name: scope_version
      type: text
      indexed: false
    - name: scope_attributes
      type: json
      indexed: false
    - name: scope_dropped_attributes_count
      type: u64
      indexed: false

  timestamp_field: timestamp_nanos

indexing_settings:
  commit_timeout_secs: 10

search_settings:
  default_search_fields: [body.message]
```

## UI Integration

Currently, Quickwit provides a simplistic UI to get basic information from the cluster, indexes and search documents.
If a simple UI is not sufficient for you and you need additional features, Grafana and Elasticsearch query API support are planned for Q2 2023, stay tuned!

You can also send traces to Quickwit that you can visualize in Jaeger UI, as explained in the following [tutorial](../distributed-tracing/send-traces/using-otel-sdk-python.md).


## Known limitations

There are a few limitations on the log management setup in Quickwit 0.7:
- The ingest API does not provide High-Availability and High-Durability. This will be fixed in 0.9.
- OTLP HTTP is only available with the Binary Protobuf Encoding. OTLP HTTP with JSON encoding is not planned yet, but this can be easily fixed in the next version. Please open an issue if you need this feature.

If you are interested in new features or discover other limitations, please open an issue on [GitHub](https://github.com/quickwit-oss/quickwit).
