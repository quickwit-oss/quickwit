---
title: Using OTEL Collector
description: Using OTEL Collector
tags: [otel, collector, traces]
sidebar_position: 1
---


If you already have your own OpenTelemetry Collector and want to export your traces to Quickwit, you need a new OLTP gRPC exporter in your config.yaml:

```yaml title="otel-collector-config.yaml"
receivers:
  otlp:
    protocols:
      grpc:
      http:

processors:
  batch:

exporters:
  otlp/quickwit:
    endpoint: localhost:7281
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp/quickwit]
```

To test your setup, start your collector with docker:

```bash
docker run -v ${PWD}/otel-collector-config.yaml:/etc/otelcol/config.yaml -p 4317:4317 -p 7281:7281 otel/opentelemetry-collector
```

Now you're ready to send traces to your collector. Follow our tutorial on [how to send traces from your python app](using-otel-sdk-python.md) to test it.
