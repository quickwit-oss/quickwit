---
title: Cluster sizing
sidebar_position: 4
---

In this guide, we discuss how to size your Quickwit cluster and nodes. As shown
in the [architecture section](../overview/architecture.md), a Quickwit cluster
has 5 main components: the Indexers, the Searchers, the Control Plane, the
Metastore and the Janitor. Each component has different resource requirements
and can be scaled independently. We will also discuss how to size the metastore
PostgreSQL database.

:::note

This guide provides general guidelines. The actual resource requirements depend
strongly on the workload. We recommend monitoring the resource usage and
adjusting the cluster size accordingly.

:::

## Quickwit services

### Indexers

Here are some high-level guidelines to size your Indexer nodes:
- Quickwit can index at around **7.5MB per second per core**
- For the general use case, configure 4GB of RAM per core
  - Workloads with a large number of indexes or data sources consume more RAM
    <!-- TODO: revisit this when cooperative indexing becomes the default -->
  - Don't use instances with less than 8GB of RAM
    <!-- Note: 2GB for the heap size (per pipeline) and 2GB for ingest queues -->
- Mount the data directory to a volume of at least 110GB to store the [split
  cache](../configuration/node-config.md#Indexer-configuration) and the [ingest
  queue](../configuration/node-config.md#ingest-api-configuration).
  <!-- Note: 4GB max_queue_disk_usage and 100GB split_store_max_num_bytes -->

:::note

To utilize all CPUs on Indexer nodes that have more than 4 cores, your indexing
workload needs to be broken down into multiple indexing pipelines. This can be
achieved by creating multiple indexes or by using a [partitioned data
source](../configuration/source-config.md#number-of-pipelines) such as
[Kafka](../configuration/source-config.md#kafka-source).

<!-- TODO: change this note when releasing ingest v2 -->

:::


### Searchers

Search performance is highly dependent on the workload. For example, term queries
are usually cheaper than aggregations. A good starting point for dimensioning
Searcher nodes:
- Configure 8GB of RAM per core when using a high latency / low bandwidth object
  store like AWS S3
- Increase the CPU / RAM ratio (e.g 4GB/core) when using a faster object store
- Provision more RAM if you expect many concurrent aggregation requests. By
  default, each request can use up to 500MB of RAM on each node.
- Avoid instances with less than 4GB of RAM
<!-- 1GB fast_field_cache_capacity + 0.5GB split_footer_cache_capacity + 0.5GB/req aggregation_memory_limit -->
- Searcher nodes don't use disk unless the [split
  cache](../configuration/node-config.md#Searcher-split-cache-configuration) is
  explicitely enabled

One strength of Quickwit is that its Searchers are stateless, which makes it
easy to scale them up and down based on the workload. Scale the number of
Searcher nodes based on:
- the number of concurrent requests expected
- aggregations that run on large amounts of data (without
  [time](../overview/concepts/querying.md#time-sharding) or
  [tag](../overview/concepts/querying.md#tag-pruning) pruning)

### Other services

The Control Plane, the Metastore and the Janitor are lightweight components.
Each of these services requires 1 replica. 

The Control Plane only needs a single core and 2GB of RAM. It doesn't require any disk.

The Metastore also requires a single core and 2GB of RAM. For clusters handling
hundreds of indexes, you might increase the size to 2 cores and 4GB of RAM. It
doesn't write to disk.

In general the Janitor requires 1 core and 2GB of RAM and doesn't use the disk.
If you use the [delete API](https://quickwit.io/docs/overview/concepts/deletes),
the Janitor should be dimensioned like an indexer.

### Single node deployments

For experimentations and small scale POCs, it is possible to deploy all the
services on a single node (see
[tutorial](../get-started/tutorials/tutorial-hdfs-logs.md)). We recommend at
least 2 cores and 8GB of RAM. 

## Postgres Metastore backend

For clustered deployments, we recommend using Postgres as a Metastore backend.

For most use cases, a Postgres instance with 4GB of RAM and 1 core is
sufficient:
- with the AWS RDS managed service, use the t4g.medium instance type. Enable
  multi-AZ with one standby for high availability.
