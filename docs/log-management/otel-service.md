---
title: OTEL service
sidebar_position: 4
---

Quickwit natively supports the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/reference/specification/protocol/otlp/) and provides a gRPC endpoint to receive spans from an OpenTelemetry collector. This endpoint is enabled by default.

When enabled, Quickwit will start the gRPC service ready to receive spans from an OpenTelemetry collector. The spans are indexed on  the `otel-traces-v0_6` index, and this index will be automatically created if not present. The index doc mapping is described in the next [section](#trace-and-span-data-model).

If for any reason, you want to disable this endpoint, you can:
- Set the `QW_ENABLE_OTLP_ENDPOINT` environment variable to `false` when starting Quickwit.
- Or [configure the node config](/docs/configuration/node-config.md) by setting the indexer setting `enable_otlp_endpoint` to `false`.

```yaml title=node-config.yaml
# ... Indexer configuration ...
indexer:
    enable_otlp_endpoint: false
```

## OpenTelemetry logs data model

Quickwit sends OpenTelemetry logs into the `otel-logs-v0_6` index which is automatically created if you enable the OpenTelemetry service.
The doc mapping of this index is derived from the [OpenTelemetry logs data model](https://opentelemetry.io/docs/reference/specification/logs/data-model/). It can be accessed along with the index configuration by sending a HTTP GET request to Quickwit API.

```bash
curl -X GET http://localhost:7280/api/v1/indexes/otel-logs-v0_6
```

## UI Integration

Currently, Quickwit provides a simplistic UI to get basic information from the cluster, indexes and search documents.
If a simple UI is not sufficient for you and you need additional features, Grafana and Elasticsearch query API support are planned for Q2 2023, stay tuned!

You can also send traces to Quickwit that you can visualize in Jaeger UI, as explained in the following [tutorial](../distributed-tracing/send-traces/using-otel-sdk-python.md).


## Known limitations

There are a few limitations on the log management setup in Quickwit 0.6:
- The ingest API does not provide High-Availability and High-Durability, this will be fixed in Q3/Q4.
- OTLP gRPC service index documents only in the `otel-logs-v0_6` index.
- OTLP HTTP is not available but it should be easy to add.

If you are interested in new features or discover other limitations, please open an issue on [GitHub](https://github.com/quickwit-oss/quickwit).
