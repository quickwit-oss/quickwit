---
title: Deployment modes
sidebar_position: 1
---

As an application, Quickwit is built out of multiple services and is designed to run as a horizontally-scalable distributed system. Currently, Quickwit supports four core services (indexer, searcher, metastore, control plane) and one maintenance service (janitor):

- Indexers ingest documents from data sources and build indexes.
- Searchers execute search queries submitted via the REST API.
- The Metastore stores index metadata in a PostgreSQL-compatible database or cloud-hosted file.
- The Control Plane distributes and coordinates indexing workloads on indexers.
- The Janitor performs periodic maintenance tasks.

Quickwit is distributed as a single binary or Docker image. The behavior of that executable file or image is controlled with the `--service` option of the `quickwit run` command and defines which services run on a node. You may start one service, multiple, or all of them. Nodes always serve the REST API and the search and admin UI. In addition, they will redirect requests that they cannot satisfy to the appropriate nodes in the cluster. Finally, each service can run on one or several nodes depending on the expected load on the system.

## Standalone mode (single node)

This deployment mode is the simplest way to get started with Quickwit. Launch all the services with the `quickwit run` [command](../reference/cli.md), and you are now ready to ingest data and search your indexes.

## Cluster mode (multi-node)

You can deploy Quickwit on multiple nodes. We provide a [Helm chart](./kubernetes/helm.md) to help you deploy Quickwit on Kubernetes. In cluster mode, you must store your index data on a shared storage backend such as Amazon S3 or MinIO.

## One indexer, multiple searchers

One indexer running on a small instance (4 vCPUs) can ingest documents at a throughput of 20-40MB/s (1-3+ TB/day). A deployment with one indexer is thus an excellent place to start. However, you may need several searchers to handle large datasets or serve many resource-intensive requests such as aggregation queries.

## Multiple indexers, multiple searchers

Indexing a single [data source](../configuration/source-config.md) on several indexers is only possible with a [Kafka source](../configuration/source-config.md#kafka-source).
Support for native distributed indexing was added with Quickwit 0.9.

## File-backed metastore limitations

The file-backed metastore is a good fit for standalone and small deployments. However, it does not support multiple instances running at the same time. As long as you can guarantee that no more than one metastore is running at any given time, the file-backed metastore is safe to use. For heavy workloads, we recommend using a PostgreSQL metastore.
