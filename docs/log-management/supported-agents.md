---
title: Supported agents
sidebar_position: 3
---

Quickwit is compatible with the following agents:

## OpenTelemetry agent

Before using an [OpenTelemetry collector](https://opentelemetry.io/docs/collector/), check that [Quickwit OpenTelemetry service](otel-service.md) is enabled.
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

Find more configuration details on the [OpenTelemetry documentation](https://opentelemetry.io/docs/collector/configuration/). You can also check out our [tutorial to send logs with OTEL collector](send-logs/using-otel-collector.md) to Quickwit.

## HTTP-based agents

It's also possible to use other agents that send HTTP requests to Quickwit Ingest API. Quickwit also partially supports Elasticseardch `_bulk` API. Thus, there is a good chance that your agent is already compatible with Quickwit.
Currently, we have tested the following HTTP-based agents:

- [Vector](send-logs/using-vector.md)
- [Fluentbit](send-logs/using-fluentbit.md)
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
