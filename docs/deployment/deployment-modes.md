---
title: Deployment modes
sidebar_position: 1
---


Quickwit distributed search engine relies on 4 major services and one maintenance service:

- The Searchers for executing search queries from the REST API.
- The Indexers that index data from data sources.
- The Metastore that stores the index metadata in a PostgreSQL-like database or in a cloud storage file.
- The Control plane that schedules indexing tasks to the indexers.
- The Janitor that executes periodic maintenance tasks.

Quickwit is compiled as a single binary or Docker image, and you can choose to start one, several services or all of them. Each node also always serves the UI static assets required by the UI React app.

You can deploy Quickwit on a single node or multiple nodes.

## Single-node

This is the simplest way to get started with Quickwit. First, launch all the services with the `quickwit run` [command](../reference/cli.md), and then you're ready to ingest and search data.

## Multi nodes

You can deploy Quickwit on multiple nodes. We provide a [helm-chart](kubernetes.md) to help you deploy Quickwit on Kubernetes.

As soon as you are running at least 2 nodes, there are several restrictions:
- you have to use a distributed data store such as Amazon S3 or MinIO for storing your index; a local file system storage will not work;
- if you use the [Ingest API](../reference/rest-api.md), you must send your queries directly to the indexer. When sent to a searcher, you will get a 404 response. Note that when search queries are addressed to an indexer, it acts as a root searcher node and dispatches leaf requests to searchers;

## One indexer, multiple searchers

One Quickwit node running on a decent instance can ingest data at speeds up to 40 MB/sec from Kafka. A deployment with one indexer is thus a good start. However, you may need several searchers for handling large datasets or serving many resource-intensive queries such as aggregation queries.

## Multiple indexers, multiple searchers

Indexing a single [data source](../configuration/source-config.md) on several indexers is currently only possible with a [Kafka source](../configuration/source-config.md#kafka-source).
Support distributed indexing for Pulsar and the Ingest API sources is planned for Q2, stay tuned!

## File-backed metastore limitations

The file-backed metastore is mainly useful for testing purposes. Though it may be convenient for some specific use cases, we strongly encourage you to use a PostgreSQL metastore in production.
The main limitations of the file-backed metastore are:
- it does not support concurrent writes;
- it caches metastore data and polls files regularly to update its cache. Thus it has a delayed view on the metastore.
