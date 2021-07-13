---
title: Architecture
sidebar_position: 2
---

Quickwit is built on three big components:
- the index: its a set of data structures tailored to be queried, even on high latency storage.
- the metastore: it stores index metadata and makes them available to all search nodes.
- the search cluster: provides high availability search, workload distribution, and efficient caching.

[//]: # (Add space with '---' and align image for docusaurus)

---
<div style={{textAlign: 'center'}}>

![Quickwit Architecture](../assets/images/quickwit-architecture.svg)

</div>

## The index

A quickwit index stores documents and makes it possible to query them efficiently.
The index organizes documents into a collection of smaller independent indexes called splits.  Behind the scenes, a split is just [tantivy index](https://github.com/tantivy-search/tantivy/blob/main/ARCHITECTURE.md#index-and-segments) with a custom index format.

You have total control over how document fields are stored and indexed, thanks to the `Index config`.

### Index config

A document is a collection of fields. Fields can be stored in different data structures:
- an inverted index, which enables fast full-text search.
- a columnar storage called `fast field`.  It is the equivalent of doc values in Lucene. Fast fields are required to compute aggregates over the documents matching a query. They can also allow some advanced types of filtering.
- a row-storage, called the doc store. It makes it possible to get the content of the matching documents.

The index config controls how to map your JSON object to a Quickwit document and, for each field, define whether it should be stored, indexed, or be a fast field.
You can define it with a `json file`, [see docs](../reference/index-config.md).

### Splits

A split is a small piece of an index identified by a UUID. For each split, Quickwit adds up a `hotcache` file along with index files. This hotcache is what makes it possible for the searchers to open a split in less than 60ms, even on high latency storage.

The quickwit index is aware of its splits by keeping splits metadata, notably:
- the split state which indicates if the split is ready for search
- the min/max time range computed on the timestamp field if present.

This timestamp metadata can be handy at query time. If the user specifies a time range filter to their query, Quickwit will use it to **prune irrelevant splits**.

Index metadata needs to be accessible by every instance of the cluster. This is made possible thanks to the `metastore`.


## The metastore

Quickwit gathers index metadata into a metastore to make them available across the cluster.

For a given query on a given index, a search node will ask the metastore for the index metadata and then use it to do the query planning and finally execute the plan.

Currently, the only implementation of the `metastore` is a JSON file based store: it writes a `quickwit.json` on your disk or on an AWS S3 bucket. We plan to add other backends such as Postgresql and other popular databases soon.


## The search cluster

Quickwit's search cluster has the following characteristics:
- it is made of stateless nodes: any node can answer any query about any splits.
- a node can distribute search workload to other nodes.
- cluster membership is based on a gossip protocol
- load-balancing is made with rendezvous hashing to allow for efficient caching.

This design provides a high availability while keeping the architecture simple.

### Stateless nodes

Quickwit cluster distributes searches workload while keeping nodes stateless.

Thanks to the hotcache, opening a split from Amazon S3 only takes  60ms. It makes it possible to remain totally stateless: a node does not need to know anything about the indexes. Adding or removing nodes is subsecond and requires moving data around.

### Workload distribution: root and leaf nodes

Any search node can handle any search request. The node that receives your query will act as the root node for the span of this request. It will then process it in 3 steps:
- Get the index metadata from the metastore and identify the splits which are relevant for the query
- Distributes the split workload among the nodes of the cluster. These nodes are assuming the role of leaf nodes.
- Returns aggregated results.

### Cluster discovery

Quickwit uses a gossip protocol to manage membership and broadcast messages to the cluster provided by [artillery project](https://github.com/bastion-rs/artillery/). The gossip protocol is based on "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol" with a few minor adaptations.


### Rendezvous hashing

To distribute workload to leaf nodes, the root node uses [Rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing)
to select the most qualified node to work on a given split. This notion of node/split affinity unlocks efficient caching. In Quickwit 0.1, however, caching is limited to the hotcache files.
