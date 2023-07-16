---
title: OTEL service
sidebar_position: 5
---

Quickwit natively supports the [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/reference/specification/protocol/otlp/) and provides a gRPC endpoint to receive spans from an OpenTelemetry collector, or from your application directly, via an exporter. This endpoint is enabled by default.

When enabled, Quickwit will start the gRPC service ready to receive spans from an OpenTelemetry collector. The spans are indexed on  the `otel-traces-v0_6` index, and this index will be automatically created if not present. The index doc mapping is described in the next [section](#trace-and-span-data-model).

If for any reason, you want to disable this endpoint, you can:
- Set the `QW_ENABLE_OTLP_ENDPOINT` environment variable to `false` when starting Quickwit.
- Or [configure the node config](/docs/configuration/node-config.md) by setting the indexer setting `enable_otlp_endpoint` to `false`.

```yaml title=node-config.yaml
# ... Indexer configuration ...
indexer:
    enable_otlp_endpoint: false
```

## Trace and span data model

A trace is a collection of spans that represents a single request. A span represents a single operation within a trace. OpenTelemetry collectors send spans, Quickwit then indexes them in the `otel-traces-v0_6` index that maps OpenTelemetry span model to an indexed document in Quickwit.

The span model is derived from the [OpenTelemetry specification](https://opentelemetry.io/docs/reference/specification/trace/api/). It can be accessed along with the index configuration by sending a HTTP GET request to Quickwit API.

```bash
curl -X GET http://localhost:7280/api/v1/indexes/otel-traces-v0_6
```

## Known limitations

There are a few limitations on the current distributed tracing setup in Quickwit 0.6:
- The OTLP gRPC service does not provide High-Availability and High-Durability, this will be fixed in Q3/Q4.
- OTLP gRPC service index documents only in the `otel-traces-v0_6` index.
- OTLP HTTP is not available but it should be easy to add.

If you are interested in new features or discovered other limitations, please open an issue on [GitHub](https://github.com/quickwit-oss/quickwit).
