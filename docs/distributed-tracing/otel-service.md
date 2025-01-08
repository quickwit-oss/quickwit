---
title: OTEL service
sidebar_position: 5
---

Quickwit natively supports the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/reference/specification/protocol/otlp/) and provides a gRPC endpoint to receive spans from an OpenTelemetry collector, or from your application directly, via an exporter. This endpoint is enabled by default.

When enabled, Quickwit will start the gRPC service ready to receive spans from an OpenTelemetry collector. The spans are indexed in the `otel-trace-v0_7` index by default, and this index will be automatically created if not present. The index doc mapping is described in the next [section](#trace-and-span-data-model).

If for any reason, you want to disable this endpoint, you can:
- Set the `QW_ENABLE_OTLP_ENDPOINT` environment variable to `false` when starting Quickwit.
- Or [configure the node config](/docs/configuration/node-config.md) by setting the indexer setting `enable_otlp_endpoint` to `false`.

```yaml title=node-config.yaml
# ... Indexer configuration ...
indexer:
    enable_otlp_endpoint: false
```

## Sending spans in your own index

You can send spans in the index of your choice by setting the header `qw-otel-traces-index` of your gRPC request to the targeted index ID.


## Trace and span data model

A trace is a collection of spans that represents a single request. A span represents a single operation within a trace. OpenTelemetry collectors send spans, Quickwit then indexes them in the `otel-trace-v0_7` index by default that maps OpenTelemetry span model to an indexed document in Quickwit.

The span model is derived from the [OpenTelemetry specification](https://opentelemetry.io/docs/reference/specification/trace/api/).

Below is the doc mapping of the `otel-trace-v0_7` index:

```yaml

version: 0.7

index_id: otel-trace-v0_7

doc_mapping:
  mode: strict
  field_mappings:
    - name: trace_id
      type: bytes
      input_format: hex
      output_format: hex
      fast: true
    - name: trace_state
      type: text
      indexed: false
    - name: service_name
      type: text
      tokenizer: raw
      fast: true
    - name: resource_attributes
      type: json
      tokenizer: raw
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
    - name: span_id
      type: bytes
      input_format: hex
      output_format: hex
    - name: span_kind
      type: u64
    - name: span_name
      type: text
      tokenizer: raw
      fast: true
    - name: span_fingerprint
      type: text
      tokenizer: raw
    - name: span_start_timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
      indexed: false
      fast: true
      fast_precision: milliseconds
    - name: span_end_timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
      indexed: false
      fast: false
    - name: span_duration_millis
      type: u64
      indexed: false
      fast: true
    - name: span_attributes
      type: json
      tokenizer: raw
      fast: true
    - name: span_dropped_attributes_count
      type: u64
      indexed: false
    - name: span_dropped_events_count
      type: u64
      indexed: false
    - name: span_dropped_links_count
      type: u64
      indexed: false
    - name: span_status
      type: json
      indexed: true
    - name: parent_span_id
      type: bytes
      input_format: hex
      output_format: hex
      indexed: false
    - name: events
      type: array<json>
      tokenizer: raw
      fast: true
    - name: event_names
      type: array<text>
      tokenizer: default
      record: position
      stored: false
    - name: links
      type: array<json>
      tokenizer: raw

  timestamp_field: span_start_timestamp_nanos

indexing_settings:
  commit_timeout_secs: 10

search_settings:
  default_search_fields: []
```

## Known limitations

There are a few limitations on the current distributed tracing setup in Quickwit 0.7:
- The OTLP gRPC service does not provide High-Availability and High-Durability. This will be fixed in 0.9.
- OTLP HTTP is only available with the Binary Protobuf Encoding.  OTLP HTTP with JSON encoding is not planned yet, but this can be easily fixed in the next version. Please open an issue if you need this feature.

If you are interested in new features or discovered other limitations, please open an issue on [GitHub](https://github.com/quickwit-oss/quickwit).
