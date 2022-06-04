---
title: Deployment modes
sidebar_position: 1
---

Quickwit is composed of 3 services:
- the indexer service: it starts the indexing pipelines and serves the [Ingest API](../reference/rest-api.md);
- the searcher service: it serves the [Search and Aggregation API](../reference/rest-api.md);
- the UI service: it serves the static assets required by the UI React app.

Quickwit is compiled as a single binary or Docker image, and you can choose to start the indexer or the searcher or both of them. As for the UI service, it is always launched.
You can deploy Quickwit on a single node or multiple nodes. However, please note that some deployment configurations are still rough around the edges, so read the current limitations carefully.

## Single-node

This is the simplest way to get started with Quickwit. First, launch all the services with the `quickwit run` [command](../reference/cli.md), and then you're ready to ingest and search data.

## Multi nodes: single indexer, multiple searchers

One Quickwit node running on a decent instance can ingest data at speeds up to 40 MB/sec from Kafka. A deployment with one indexer is thus a good start. However, you may need several searchers for handling large datasets or serving many resource-intensive queries such as aggregation queries.

As soon as you are running at least 2 nodes, there are several restrictions:
- you have to use a distributed storage like AWS S3 or MinIO for storing your index, a local file system storage will not work;
- if you use the [Ingest API](../reference/rest-api.md), you must send your queries directly to the indexer. If sent to a searcher, you will get a 404 response. Note that you can send search queries to an indexer, it will act as a root searcher node and send leaf requests to searchers;

## Multiple indexers, multiple searchers

Coming soon :)

## General limitations
### On the CLI

While running one or several nodes, we strongly discourage you to use the [CLI](../reference/cli.md) to manage indexes (add/delete) as we do not notify the running services of the modifications.

For example, if you create an index with the CLI while an indexer is running, it will not be notified by the creation and it won't accept ingest requests on this new index. You will need to restart the indexer to be able to ingest documents on this index.
A searcher will behave similarly on index creation but not on index deletion. For example, a file-backed metastore server will not be aware of the index deletion and will continue trying to serve search queries and return 500 errors.

Generally speaking:
- when you create/delete indexes, you should restart your indexer;
- when you delete indexes, you should restart your searchers if you use a file-backed metastore.


### On the file-backed metastore

The file-backed metastore is mainly useful for testing purposes. Though it may be very practical in specific use cases, we strongly encourage you to use a Postgresql metastore in production.
The main limitations of the file-backed metastore are:
- it does not support concurrent writes;
- it caches metastore data and polls regularly files to update its cache. Thus it has a delayed view on the metastore.