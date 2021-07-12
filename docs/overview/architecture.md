---
title: Architecture
sidebar_position: 2
---

Quickwit is built on 3 pillars:
- the index: it stores documents with data structures making very efficient query execution even on high latency storage
- the metastore: it stores index metadata and make them available to all search nodes
- the search cluster: provides high availability search, workload distribution, and efficient caching.

[//]: # (Add space with '---' and align image for docusaurus)

---
<div style={{textAlign: 'center'}}>

![Quickwit Architecture](../assets/images/quickwit-architecture.svg)

</div>

## The index

A quickwit index is the entity that stores documents and makes it possible to query and collect information (statistics, for example) about them efficiently.
The index organizes documents into a collection of smaller independent indexes called splits; each split is defined by its UUID and, behind the scenes, is equivalent to a customized [tantivy index](https://github.com/tantivy-search/tantivy/blob/main/ARCHITECTURE.md#index-and-segments) plus some metadata such as min/max timestamp to enable efficient time pruning at query time.

Last but not least, you have total control over how document fields are stored and indexed thanks to the `Index config`.

### Index config

A document is a collection of fields; every field is stored with optimized data structures: an inverted index but also a columnar storage called `fast field` (the equivalent of doc values in Lucene) useful when you need to filter or collect some data like statistics on this field.

You can define it with a `json file`, [see docs](../reference/index-config.md).


### Splits

A split is a small piece of an index, an independent collection of documents with its own (tantivy) index files. Along with index files, Quickwit adds up a `hotcache` file which enables high-speed index opening even on high latency storage.

The quickwit index is aware of its own splits by keeping splits metadata, notably:
- the split state which indicates if the split is ready for search
- the min/max time range computed on the timestamp field if present. 
  
This timestamp field can be very useful at query time because Quickwit will use it for **pruning splits** whose time ranges
are out of the time range defined by the query.  

Index metadata needs to be accessible by every instance of the cluster; this is made possible thanks to the `metastore`.


## The metastore

Quickwit gathers index metadata (such as split metadata) into a metastore to make them available across the cluster.

For a given query on a given index, a search node will ask the metastore the index metadata, which contains splits metadata, and then use it to do the query planning and finally execute the plan.

Currently, the only implementation of the `metastore` is a json file based store: it writes a `quickwit.json` on your disk or
on an AWS S3 bucket. We plan to add other backends such as Postgresql and other popular databases soon.


## The search cluster

Quickwit cluster have the following characteritics:
- it is made of stateless nodes: any node can answer to any query
- a node can distribute search workload to other nodes
- cluster formation is based on a gossip protocol  
- load balancing is made with rendezvous hashing

This design provides a high availability while keeping the architecture simple.

### Stateless nodes

Quickwit cluster distributes searches workload while keeping nodes stateless.

The ability to open an index in less than 70ms makes it possible to remain totally stateless: a node knows nothing about the indexes. Adding or removing nodes is subsecond and requires no data moves.

### Workload distribution: root and leaf nodes

Any search node can handle a query. When receiving one, the node acts as a root node for this particular query and processes it in 3 steps:
- does the query planning which corresponds to finding the relevant splits for the query
- distributes the split workload on itself and/or other nodes called the leaf nodes
- returns aggregated results. 

### Cluster discovery

Quickwit uses a gossip protocol to manage membership and broadcast messages to the cluster provided by [artillery project](https://github.com/bastion-rs/artillery/). The gossip protocol is based on "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol", with a few minor adaptations.


### Rendezvous hashing

To distribute workload to leaf nodes, a root uses [Rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) 
to select the relevant nodes to query data by split. This unlocks caching capabilities. Another benefit from Rendezvous hashing is its stability when you add or remove nodes.
