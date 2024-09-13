---
title: Distributed Tracing with Quickwit
sidebar_label: Overview
sidebar_position: 1
---

Distributed Tracing is a process that tracks your application requests flowing through your different services: frontend, backend, databases and more. It's a powerful tool to understand how your application works and to debug performance issues.

Quickwit is a cloud-native engine to index and search unstructured data which makes it a perfect fit for a traces backend.

Moreover, Quickwit supports natively the [OpenTelemetry gRPC and HTTP (protobuf only) protocol](https://opentelemetry.io/docs/reference/specification/protocol/otlp/) and the [Jaeger gRPC API (SpanReader only)](https://www.jaegertracing.io/). **This means that you can use Quickwit to store your traces and to query them with Jaeger UI**.

![Quickwit Distributed Tracing](../assets/images/distributed-tracing-overview-light.png#gh-light-mode-only)![Quickwit Distributed Tracing](../assets/images/distributed-tracing-overview-dark.png#gh-dark-mode-only)

## Plug Quickwit to Jaeger

Quickwit implements a gRPC service compatible with Jaeger UI. All you need is to configure Jaeger with a (span) storage type `grpc`[^1] and you will be able to visualize your traces in Jaeger that are stored in any Quickwit's indexes matching the pattern `otel-traces-v0_*`.

We made a tutorial on [how to plug Quickwit to Jaeger UI](plug-quickwit-to-jaeger.md) that will guide you through the process.

[^1]: It was `grpc-plugin` until the version 1.58 of Jaeger.

## Send traces to Quickwit

- [Using OTEL collector](send-traces/using-otel-collector.md)
- [Using python OTEL SDK](send-traces/using-otel-sdk-python.md)

