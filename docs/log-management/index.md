---
title: Log management with Quickwit
sidebar_position: 1
---

import DocCardList from '@theme/DocCardList';

Quickwit is built from the ground up to [efficiently index unstructured data](../guides/schemaless.md), and search it effortlessly on cloud storage.
Moreover, Quickwit supports OpenTelemetry out of the box and provides a REST API ready to ingest any JSON formatted logs.
**This makes Quickwit a perfect fit for logs!**.

![Quickwit Log Management](../assets/images/log-management-overview-light.svg#gh-light-mode-only)![Quickwit Log Management](../assets/images/log-management-overview-dark.svg#gh-dark-mode-only)

## Sending logs to Quickwit

<DocCardList />

## Supported agents

### OpenTelemetry agent

Before using an [OpenTelemetry collector](https://opentelemetry.io/docs/collector/), check that [Quickwit OpenTelemetry service](#opentelemetry-service) is enabled.
Once started, Quickwit is then ready to receive and ingest OpenTelemetry gRPC requests.

Here is a configuration example of an OpenTelemetry agent that sends logs into Quickwit:

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
      # Replace quickwit-host with the hostname of your Quickwit node/service.
      # On k8s, it should be of the form `{quickwit-indexer-service-name}.{namespace}.svc.cluster.local:7281
      endpoint: quickwit-host:7281
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

Find more configuration details on the [OpenTelemetry documentation](https://opentelemetry.io/docs/collector/configuration/). You can also check out our [tutorial to send Kubernetes logs](deploy-quickwit-otel-with-helm.md) to Quickwit.

### HTTP-based agents

It's also possible to use other agents that send HTTP requests to Quickwit Ingest API. Quickwit also partially supports Elasticseardch `_bulk` API. Thus, there is a good chance that your agent is already compatible with Quickwit.
Currently, we have tested the following HTTP-based agents:

- [Vector](./send-logs-from-vector-to-quickwit.md)
- [Fluentbit](./send-logs-from-fluentbit-to-quickwit.md)
- FluentD (tutorial coming soon)
- Logstash: Quickwit does not support the Elasticsearch output. However, it's possible to send logs with the HTTP output but with `json` [format](https://www.elastic.co/guide/en/logstash/current/plugins-outputs-http.html) only.

Quickwit natively supports the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/reference/specification/protocol/otlp/) and provides a gRPC endpoint to receive logs from an OpenTelemetry collector by default.

The logs received by this endpoint are indexed on  the `otel-logs-v0` index. This index will be automatically created if not present. The index doc mapping is described in this [section](#opentelemetry-logs-data-model).

You can also send your logs directly to this index by using the [ingest API](/docs/reference/rest-api.md#ingest-data-into-an-index).

## OpenTelemetry service

Quickwit natively supports the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/reference/specification/protocol/otlp/) and provides a gRPC endpoint to receive spans from an OpenTelemetry collector. This endpoint is enabled by default.

When enabled, Quickwit will start the gRPC service ready to receive spans from an OpenTelemetry collector. The spans are indexed on  the `otel-trace-v0` index, and this index will be automatically created if not present. The index doc mapping is described in the next [section](#trace-and-span-data-model).

If for any reason, you want to disable this endpoint, you can:
- Set the `QW_ENABLE_OTLP_ENDPOINT` environment variable to `false` when starting Quickwit.
- Or [configure the node config](/docs/configuration/node-config.md) by setting the indexer setting `enable_otlp_endpoint` to `false`.

```yaml title=node-config.yaml
# ... Indexer configuration ...
indexer:
    enable_otlp_endpoint: false
```

## OpenTelemetry logs data model

Quickwit sends OpenTelemetry logs into the `otel-logs-v0` index which is automatically created if you enable the OpenTelemetry service.
The doc mapping of this index described below is derived from the [OpenTelemetry logs data model](https://opentelemetry.io/docs/reference/specification/logs/data-model/).

```yaml

version: 0.5

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

Currently, Quickwit provides a simplistic UI to get basic information from the cluster, indexes and search documents.
If a simple UI is not sufficient for you and you need additional features, Grafana and Elasticsearch query API support are planned for Q2 2023, stay tuned!

You can also send traces to Quickwit that you can visualize in Jaeger UI, as explained in the following [tutorial](./distributed-tracing/instrument-python-and-send-traces-to-quickwit).


## Known limitations

There are a few limitations on the log management setup in Quickwit 0.5:
- Aggregations are not available on sparse fields and JSON field, this will be fixed in 0.6. This means that only the timestamp field can support aggregations.
- The ingest API does not provide High-Availibility and High-Durability, this will be fixed in Q2/Q3.
- Grafana and Elasticsearch query API support are planned for Q2 2023.
- OTLP gRPC service index documents only in the `otel-logs-v0` index.
- OTLP HTTP is not available but it should be easy to add.

If you are interested in new features or discover other limitations, please open an issue on [GitHub](https://github.com/quickwit-oss/quickwit).
