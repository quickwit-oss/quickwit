---
title: Node sizing
sidebar_position: 4
---

In this guide, we discuss how to size your Quickwit nodes. As presented in the
[architecture](../overview/architecture.md) section, a Quickwit cluster has 5
main components: the indexers, the searchers, the control plane, the metastore
and the janitor. Each of these components has different resource requirements
and can be scaled independently. We will also discuss how to size the Postgres
metastore backend.

## Indexers

Here are some high level guidelines to size your indexer nodes:
- Quickwit can index at around **7.5MB per second per core**
- For the general usecase, configure 4GB of RAM per core
  - Workloads with a large number of indexes or data sources consume more RAM
    <!-- TODO: Mention cooperative indexing here ? -->
  - Don't use instances with less than 8GB of RAM
    <!-- Note: 4GB for the heap size (per pipeline), and 2GB for ingest queues -->
- Mount the data directory to a volume of at least 100GB to match the [default
  split store size](../configuration/node-config.md#indexer-configuration).

:::note

To utilize all CPUs on indexer nodes that have more than 4 cores, your indexing
workload needs to be broken down into multiple indexing pipelines. This can be
achieved by creating multiple indexes or by using a [partitioned data
source](../configuration/source-config.md#number-of-pipelines) such as
[Kafka](../configuration/source-config.md#kafka-source).

<!-- TODO: change this note when releasing ingest v2 -->

:::


## Searchers

Search performance is highly dependent the workload. For example term queries
are usually cheaper than aggregations. A good starting point for dimensioning
searcher nodes:
- Configure 8GB of RAM per core when using a high latency / low bandwidth object
  store like AWS S3.
- Increase the CPU / RAM ratio (e.g 4GB/core) when using a faster object store
- Provision more RAM if you expect many concurrent aggregation requests
- Avoid instances with less than 4GB of RAM

The strength of Quickwit is that searchers are stateless, which makes it
possible to scale them up and down easily based on the workload. Scale the
number of searcher nodes based on:
- the number of concurrent requests expected
- aggregations that run on large amounts of data (without
  [time](../overview/concepts/querying.md#time-sharding) or
  [tag](../overview/concepts/querying.md#tag-pruning) pruning)
