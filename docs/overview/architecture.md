---
title: Architecture
sidebar_position: 2
---

## What is Quickwit?

Quickwit is a distributed search engine designed to be fast on high latency storage and easy to operate. This is notably achieved by separating compute & storage and revamping the read path of search indexes to drastically reduce random seeks.

Quickwit provide a CLI to create / delete indexes, search and start a search cluster, it consumes ndjson documents and produces indexes that can be stored locally or remotely on an object storage such as Amazon S3 and queried with subsecond latency.

Quickwit CLI relies on 3 pillars:
- the index: stores documents with datastructures making very efficient query execution even on high latency storage
- the metastore: stores index metadata and make them available to all search nodes
- the search cluster: provides high availibity search, workload distribution and efficient caching.


![Quickwit Architecture](../assets/images/quickwit-architecture.svg)


## The index

A quickwit index is the entity that stores documents and makes possible to query and collect information (statistics for example) about them efficiently.
The index organizes documents into collection of smaller independent index called splits, each split is defined by its UUID and behind the scenes is equivalent to a customized [tantivy index](https://github.com/tantivy-search/tantivy/blob/main/ARCHITECTURE.md#index-and-segments) plus some metadata such as min/max timestamp to enable efficient time pruning at query time.
Last but not least, you have total control on how fields are stored and indexed thanks to the `Doc Mapper`.

### The split
A split is a small piece of an index, an independant collection of documents with its own tantivy index files. Along with tantivy fiels, Quickwit adds up a `hotcache` file which enable very fast index opening even on high latency storage.

The quickwit index is aware of its splits by keeping metadata of splits, notably:
- the split state which indicates notably if the split is ready for search [cf]() 
- the min/max time range of the timestamp field if present, useful for pruning splits when filtering on timestamp field.


### The document mapper
A document is a collection of fields, every field is stored with optimized data structures: an inverted index but also n columnar storage called `fast field` (equivalent of doc values in Lucene) when you need to filter or collect some data like statistics on this field.

[See the doc mapper documentation](../reference/doc-mapper.md).


## The metastore
Quickwit gathers index metadata (such as split metadata) into a metastore to make them available accross the cluster.

For a given query on a given index, a search node will ask the metastore the splits metadata, the doc mapper and then use it to do the query planning and finally execute the plan.

Currently, the metastore is a simple json file stored on your local machine or on a AWS S3 bucket. But we plan to add backends such as Postgresql and other popular databases.

## Search cluster

A quickwit cluster distributes queries and search workload while keeping nodes stateless. The s 
The ability to open an index in less than 70ms makes it possible to remain totally stateless (=a node knows nothing about the index). Moreover Adding or removing nodes is subsecond and requires no data moves. 

The cluster makes also use of [rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) to cache relevants pieces of data such as the `hotcache`, `terms dictionnary` and `fast fields` accross nodes. The main advantage of Rendezvous hashing for our use case is that small changes in the set of available nodes only affects a small fraction of keys (=split id).

This design provides high availability thanks to stateless instance and little caching sensitiveness to adding and removing nodes.


### Workload distribution: root and leaf nodes

Any search node can handle a query. When receiving one, the node acts as a root node for this particular query and process it in 3 steps:
- do the query planning which corresponds to finding the relevant splits for the query
- distribute the split workload on itself or other nodes called the leaf nodes
- return aggregated results. 


### Cluster discovery

Quickwit uses a gossip protocol to manage membership and broadcast messages to the cluster provided by [artillery project](https://github.com/bastion-rs/artillery/). The gossip protocol is based on "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol", with a few minor adaptations.



