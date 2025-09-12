---
title: What is Quickwit?
sidebar_position: 1
---

Quickwit is the first engine to execute complex search and analytics queries directly on cloud storage with sub-second latency. Powered by Rust and its decoupled compute and storage architecture, it is designed to be resource-efficient, easy to operate, and scale to petabytes of data.

Quickwit is a great fit for log management, distributed tracing, and generally immutable data such as conversational data (emails, texts, messaging platforms) and event-based analytics.


## Why Quickwit is different from other search engines?

Quickwit is designed for sub-second search straight from object storage allowing true decoupled compute and storage. And it means a lot for your infrastructure:

- You store once for all your data on cheap, safe and unlimited storage.
- You scale out your cluster in seconds, no need to move data around.
- Indexing and search workloads are decoupled, you can scale them independently.
- Your tenants are easily isolated and you can charge them for their usage.

Quickwit is also designed to index and search semi-structured data. Its schemaless indexing allows you to index JSON document with an arbitrary amount of field without heavily impacting your performance. Aggregation are not yet supported but we are working on it, stay tuned!

## When to use Quickwit

Quickwit is a great fit for log management, distributed tracing, and generally immutable data such as conversational data (emails, texts, messaging platforms), event-based analytics,  audit logs, security logs, and more.

Check out our guides to see how you can use Quickwit:

- [Log management](../log-management/overview.md)
- [Distributed Tracing](../distributed-tracing/overview.md)


## Key features

- Full-text search and aggregation queries
- Elasticsearch query language support
- Sub-second search on cloud storage (Amazon S3, Azure Blob Storage, …)
- Decoupled compute and storage, stateless indexers & searchers
- [Schemaless](https://quickwit.io/docs/guides/schemaless) or strict schema indexing
- Schemaless analytics
- [Grafana data source](https://github.com/quickwit-oss/quickwit-datasource)
- [Jaeger-native](https://quickwit.io/docs/distributed-tracing/plug-quickwit-to-jaeger)
- OTEL-native for [logs](https://quickwit.io/docs/log-management/overview) and [traces](https://quickwit.io/docs/distributed-tracing/overview)
- Kubernetes ready - See our [helm-chart](https://quickwit.io/docs/deployment/kubernetes)
- RESTful API

### Enterprise-grade features

- Multiple [data sources](../ingest-data/index.md) Kafka / Kinesis / Pulsar native
- Multi-tenancy: indexing with many indexes and partitioning
- Retention policies
- Delete tasks (for GRPR use cases)
- Distributed and highly available* engine that scales out in seconds (HA indexing only with Kafka)

## When not to use Quickwit

Use cases where you would likely *not* want to use Quickwit include:

- You need a low-latency search for e-commerce websites.
- Your data is mutable.

## Time to discover Quickwit

- [Quickstart](../get-started/quickstart.md)
- [Concepts](architecture.md)
- [Last release blog post](https://quickwit.io/blog/quickwit-0.7)
