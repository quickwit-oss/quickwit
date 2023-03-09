---
title: Architecture
sidebar_position: 2
---

Quickwit distributed search engine relies on 4 major services and one maintenance service:

- The Searchers for executing search queries from the REST API.
- The Indexers that index data from data sources.
- The Metastore that stores the index metadata in a PostgreSQL-like database or in a cloud storage file.
- The Control plane that schedules indexing tasks to the indexers.
- The Janitor that executes periodic maintenance tasks.

Moreover, Quickwit leverages existing infrastructure by relying on battled-tested technologies for index storage, metadata storage, and ingestion:

- Cloud storage like AWS S3, Google Cloud Storage, Azure Blob Storage or other S3 compatible storage for index storage.
- Postgresql for metadata storage.
- Distributed queues like Kafka and Pulsar for ingestion.

## Architecture diagram

The following diagram shows a Quickwit cluster with its four major components and the janitor whose role is to execute periodic maintenance tasks, see the [Janitor section](#janitor) for more details.

![Quickwit Architecture](../assets/images/quickwit-architecture-light.svg#gh-light-mode-only)![Quickwit Log Management](../assets/images/quickwit-architecture-dark.svg#gh-dark-mode-only)

## Index & splits

A Quickwit index stores documents and makes it possible to query them efficiently. The index organizes documents into a collection of smaller independent indexes called **splits**.

A document is a collection of fields. Fields can be stored in different data structures:

- an inverted index, which enables fast full-text search.
- a columnar storage called `fast field`. It is the equivalent of doc values in [Lucene](https://lucene.apache.org/). Fast fields are required to compute aggregates over the documents matching a query. They can also allow some advanced types of filtering.
- a row-storage called the doc store. It makes it possible to get the content of the matching documents.

You can configure your index to control how to map your JSON object to a Quickwit document and, for each field, define whether it should be stored, indexed, or be a fast field. [Learn how to configure your index](../configuration/index-config.md)

### Splits

A split is a small piece of an index identified by a UUID. For each split, Quickwit adds up a `hotcache` file along with index files. This **hotcache** is what makes it possible for Searchers to open a split in less than 60ms, even on high latency storage.

The Quickwit index is aware of its splits by keeping splits metadata, notably:

- the split state which indicates if the split is ready for search
- the min/max time range computed on the timestamp field if present.

This timestamp metadata can be handy at query time. If the user specifies a time range filter to their query, Quickwit will use it to **prune irrelevant splits**.

Index metadata needs to be accessible by every instance of the cluster. This is made possible thanks to the `metastore`.

### Index storage

Quickwit stores the indexes data (splits files) on cloud storage (AWS S3, Google Cloud Storage, Azure Blob Storage or other S3 compatible storage) and also on local disk for single-server deployment.

## Metastore

Quickwit gathers index metadata into a metastore to make them available across the cluster. 

On the write path, indexers push index data on the index storage and publish metadata to the metastore.

On the read path, for a given query on a given index, a search node will ask the metastore for the index metadata and then use it to do the query planning and finally execute the plan.

In a clustered deployment, the metastore is typically a traditional RDBMS like PostgreSQL which we only support today. In a single-server deployment, it’s also possible to rely on a local file or on Amazon S3.

## Quickwit cluster and services

### Cluster formation

Quickwit uses [chitchat](https://github.com/quickwit-oss/chitchat), a cluster membership protocol with failure detection implemented by Quickwit. The protocol is inspired by Scuttlebutt reconciliation and phi-accrual detection, ideas borrowed from Cassandra and DynamoDB.

[Learn more on chitchat](https://github.com/quickwit-oss/chitchat).

### Indexers

See [dedicated indexing doc page](./concepts/indexing.md).

### Searchers

Quickwit's search cluster has the following characteristics:

- It is composed of stateless nodes: any node can answer any query about any splits.
- A node can distribute search workload to other nodes.
- Load-balancing is made with rendezvous hashing to allow for efficient caching.

This design provides high availability while keeping the architecture simple.

**Workload distribution: root and leaf nodes**

Any search node can handle any search request. A node that receives a query will act as the root node for the span of the request. It will then process it in 3 steps:

- Get the index metadata from the metastore and identify the splits relevant to the query.
- Distributes the split workload among the nodes of the cluster. These nodes are assuming the role of leaf nodes.
- Returns aggregated results.

**Stateless nodes**

Quickwit cluster distributes search workloads while keeping nodes stateless.

Thanks to the hotcache, opening a split on Amazon S3 only takes 60ms. It makes it possible to remain totally stateless: a node does not need to know anything about the indexes. Adding or removing nodes takes seconds and does not require moving data around.

**Rendezvous hashing**

The root node uses [Rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) to distribute the workload among leaf nodes. Rendez-vous hashing makes it possible to define a node/split affinity function with excellent stability properties when a node joins or leaves the cluster. This trick unlocks efficient caching.

Learn more about query internals on the [querying doc page](./concepts/querying.md).


### Control plane

The control plane service schedules indexing tasks to indexers. The scheduling is executed when the scheduler receives external or internal events and on certains conditions:

- The scehduler listens to metastore events source create, delete, toggle, or index delete. On each of these events, it will schedule a new plan, named the `desired plan` and send indexing tasks to indexers.
- Every `HEARTBEAT` (3 seconds), the scheduler controls if the `desired plan` and the indexing tasks running on indexers are in sync. If not, it will reapply the desired plan to indexers.
- Every minute, the scheduler rebuilds a plan with the latest metastore state, and if it differs from the last applied plan, it will apply the new one. This is necessary as the scheduler may have not received all metastore events due to network issues.

### Janitor

The Janitor service runs maintenance tasks on indexes: garbage collection, delete query tasks, and retention policy tasks.

## Data sources

Quickwit supports [multiple sources](../ingest-data/) to ingest data from.

A file is ideal for a one-time ingestion like an initial load, the ingest API or a message queue are ideal to continuously feed data into the system. 

Quickwit indexers connect directly to external message queues like Kafka, Pulsar or Kinesis and guarantee the exactly-once semantics. If you need support for other distributed queues, please vote for yours [here](https://github.com/quickwit-oss/quickwit/issues/1000).
