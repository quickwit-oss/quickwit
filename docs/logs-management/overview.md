---
title: Overview
sidebar_position: 1
---

Quickwit is built from the ground up to [index efficiently unstructured data](link to schemaless) and search directly your data sitting on cloud storage.
**This makes Quickwit a perfect fit for logs!**
Moreover, Quickwit is OpenTelemetry native and provides a REST API ready to ingest any JSON formatted logs, this
makes Quickwit well integrated in the observabitility ecosystem.

Learn how Quickwit can help you manage your logs:

- [Supported agents](#supported-agents): OpenTelemetry, Vector, Fluentbit and more.
- [Enabling OLTP gRPC service](#oltp-grpc-service)
- Tutorials
  - [Sending logs with Vector](./send-logs-from-vector-to-quickwit.md)
  - [Sending Kubernetes logs with OTEL collectors](./deploy-quickwit-otel-with-helm.md)
  - More soon!
- [Logs data model](#logs-data-model)
- [UI Integration](#ui-integration)
- [Limitations](#limitations)


![Quickwit Logs Management Overview](../assets/images/logs-management-overview.svg)


## Supported agents

### gRPC-based agent: OpenTelemetry agent

Before using an [OpenTelemety collector](https://opentelemetry.io/docs/collector/), you need to enable [Quickwit OTEL gRPC service](#otel-grpc-service).
Once enabled, Quickwit is ready to receive OpenTelemetry gRPC requests.

To configure an OpenTelemetry agent, here is a typical configuration file to send logs to Quickwit:

```yaml
mode: daemonset
presets:
  logsCollection:
    enabled: true
  kubernetesAttributes:
    enabled: true
config:
  exporters:
    otlp:
      # Kubernetes endpoint as here or directly to your local instance localhost:7281
      endpoint: quickwit-indexer.qw-tutorial.svc.cluster.local:7281
      # Quickwit OTEL gRPC endpoint does not support compression yet.
      compression: none
      tls:
        insecure: true
  service:
    pipelines:
      logs:
        exporters:
          - otlp
```

Find more configuration details on the [OpenTelemetry documentation](https://opentelemetry.io/docs/collector/configuration/).


### HTTP-based agents

It's also possible to use other agents that sends HTTP requests to Quickwit Ingest API. Quickwit also partially support Elasticseardch `_bulk` API. Thus, any agent using this API to send logs to Elasticsearch have a good chance to be Quickwit-compatible.
Currently, we tested the following HTTP-based agents:

- [Vector](./send-logs-from-vector-to-quickwit.md)
- Fluentbit (tutorial coming soon)
- Logstash (tutorial coming soon)

## OLTP gRPC service

Quickwit natively support the [OpenTelemetry Protocol (OLTP)](https://opentelemetry.io/docs/reference/specification/protocol/otlp/). OTLP defines the API interface and the [logs data model](#logs-data-model).

Enabling Quickwit OLTP service is done in the [node configuration file](/docs/configuration/node-config.md) by configuring the indexer setting `enable_otlp_endpoint` to `true`. This gives the following lines in your node config file:

```yaml title=node-config.yaml
# ... Indexer configuration ...
indexer:
    enable_otlp_endpoint: true
```

On starting, Quickwit will create automatically an index `otel-logs-v0` described in the [logs data model section](#logs-data-model). Quickwit will be then ready to receive gRPC requests.

## Logs data model

Quickwit logs data model is 

```yaml

version: 0.4

index_id: otel-logs-v0

doc_mapping:
  mode: strict
  field_mappings:
    - name: timestamp_secs
      type: datetime
      input_formats: [unix_timestamp]
      indexed: false
      fast: true
      precision: seconds
      stored: false
    - name: timestamp_nanos
      type: u64
      indexed: false
    - name: observed_timestamp_nanos
      type: u64
      indexed: false
    - name: service_name
      type: text
      tokenizer: raw
    - name: severity_text
      type: text
      tokenizer: raw
    - name: severity_number
      type: u64
    - name: body
      type: json
    - name: attributes
      type: json
      tokenizer: raw
    - name: dropped_attributes_count
      type: u64
      indexed: false
    - name: trace_id
      type: text
      tokenizer: raw
    - name: span_id
      type: text
      tokenizer: raw
    - name: trace_flags
      type: u64
      indexed: false
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

  timestamp_field: timestamp_secs

  partition_key: hash_mod(service_name, 100)
  tag_fields: [service_name]

indexing_settings:
  commit_timeout_secs: 30

search_settings:
  default_search_fields: []
```

## UI Integration

Currenlty Quickwit provides a simplistic UI to get basic information from the cluster, indexes and search documents.
If a simple UI is sufficient for you but you need additional features, please open an issue on [GitHub]()!

Grafana and Elasticsearch query DSL support are planned for Q2 2023, stay tuned!

## Limitations
