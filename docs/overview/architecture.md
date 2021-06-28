---
title: Architecture
sidebar_position: 2
---

## What is Quickwit?

Quickwit is a distributed search engine designed to be fast on high latency storage and easy to operate. This is notably achieved by separating compute & storage and revamping the read path of search indexes to drastically reduce random seeks.

Quickwit provide a CLI to create / delete indexes, search and start a search cluster, it consumes ndjson documents and produces indexes that can be stored locally or remotely on an object storage such as Amazon S3 and queried with subsecond latency.

Quickwit CLI relies on 3 pillars:
- the index: stores documents with datastructures that make very efficient query execution even on high latency storage
- the metastore: stores index metadata and making them available to all search nodes
- the search cluster: provides high availibity search, workload distribution and efficient caching.


![Quickwit Architecture](../assets/images/quickwit-architecture.svg)


## The index

A quickwit index is a collection of smaller independent immutable segments. Each segment contains its own independent set of data structures.

### The index directory


### The metatata


### The doc mapper



## The metastore


## Search cluster

### Workload distribution: root and leaf nodes

### Cluster discovery

- SWIM protocol

### Caching policy

- rendez-vous hashing


