---
title: Architecture
sidebar_position: 2
---

Quickwit is built on three pillars:
- the index: a set of data structures tailored to be queried, even on high latency storage;
- the metastore: stores index metadata and makes them available to all search nodes;
- the search cluster: provides high availability search, workload distribution, and efficient caching.

[//]: # (Add space with '---' and align image for docusaurus)

---
<div style={{textAlign: 'center'}}>

![Quickwit Architecture](../assets/images/quickwit-architecture.svg)

</div>

## The index

A quickwit index stores documents and makes it possible to query them efficiently.
The index organizes documents into a collection of smaller independent indexes called splits.  Behind the scenes, a split is just a [tantivy index](https://github.com/tantivy-search/tantivy/blob/main/ARCHITECTURE.md#index-and-segments) with a custom index format.

You have total control over how document fields are stored and indexed, thanks to the `Index config`.

### Index config

A document is a collection of fields and associated values. Fields can be stored in different data structures:
- an inverted index, which enables fast full-text search;
- a column-oriented storage called `fast field`, which is the equivalent of `DocValues` in Lucene. Fast fields are required for computing aggregates over documents matching a query. They also allow some advanced types of filtering;
- a row-store, called the `doc store` making it possible to retrieve the content of matching documents.

The index config controls how to map JSON objects to Quickwit documents and, for each field, defines whether it should be stored, indexed, or encoded as a fast field.
You can define one with a `JSON file`, [see docs](../reference/index-config.md).

### Splits

A split is a small piece of an index identified by a UUID. For each split, Quickwit creates a `hotcache` file along with the index files. This hotcache enables the search nodes to open a split in less than 60ms, even on high latency storage.

The quickwit index is aware of its splits by keeping splits metadata, notably:
- the split state, which indicates whether the split is ready for search:
- the min/max time range computed on the timestamp field if present.

This timestamp metadata can be handy at query time. If the user specifies a time range filter in their query, Quickwit will use it to **prune irrelevant splits**.

Index metadata needs to be accessible by every instance of the cluster. This is made possible thanks to the `metastore`.


## The metastore

Quickwit gathers index metadata into a metastore to make them available across the cluster.

For a given query on a given index, a search node will ask the metastore for the index metadata and then use it to do the query planning and finally execute the plan.

Currently, the only implementation of the `metastore` is a JSON file based store: it writes a `quickwit.json` on disk or in an Amazon S3 bucket. We plan on supporting other backends such as Postgresql and other popular databases soon.


## The search cluster

Quickwit's search cluster has the following characteristics:
- It is composed of stateless nodes: any node can answer any query about any splits.
- A node can distribute search workload to other nodes.
- Cluster membership is based on the SWIM gossip protocol.
- Load-balancing is made with rendezvous hashing to allow for efficient caching.

This design provides high availability while keeping the architecture simple.

### Stateless nodes

Quickwit cluster distributes search workloads while keeping nodes stateless.

Thanks to the hotcache, opening a split on Amazon S3 only takes 60ms. It makes it possible to remain totally stateless: a node does not need to know anything about the indexes. Adding or removing nodes takes seconds and does not require moving data around.

### Workload distribution: root and leaf nodes

Any search node can handle any search request. A node that receives a query will acts as the root node for the span of the request. It will then process it in 3 steps:
- Get the index metadata from the metastore and identify the splits relevant for the query.
- Distributes the split workload among the nodes of the cluster. These nodes are assuming the role of leaf nodes.
- Returns aggregated results.

### Cluster discovery

Quickwit uses a gossip protocol to manage membership and broadcast messages to the cluster provided by [artillery project](https://github.com/bastion-rs/artillery/). The gossip protocol is based on "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol" with a few minor adaptations.


### Rendezvous hashing

The root node uses [Rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) to distribute the workload among leaf nodes. Rendez-vous hashing makes it possible to define a node/split affinity function with excellent stability properties when a node joins or leaves the cluster. This trick unlocks efficient caching. In Quickwit v0.1, however, caching is limited to the hotcache files.
