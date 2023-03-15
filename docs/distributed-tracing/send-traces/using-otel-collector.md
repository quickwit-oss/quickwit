---
title: Using OTEL Collector
description: Using OTEL Collector
tags: [otel, collector, traces]
sidebar_position: 1
---


If you already have your own OpenTelemetry Collector and want to export your traces to Quickwit, you need a new OLTP gRPC exporter in your config.yaml:

```yaml
exporters:
  otlp:
    endpoint: http://quickwit-host:7281
    
service:
  pipelines:
    traces:
      receivers: [...]
      processors: [...]
      exporters: [otlp, ...]

```
