---
title: Introduction
slug: /
sidebar_position: 1
---

Quickwit is a distributed search engine designed from the ground up to offer cost-efficiency and high reliability on large data sets.

Quickwit is particularly well-suited for dealing with large, immutable datasets and relatively low average QPS$^1$. Its benefits are most apparent in multi-tenancy or multi-index settings.

Common use cases for Quickwit include:

- Searching through logs, from small amounts of data to terabytes.
- Adding full-text search capabilities to [OLAP databases such as ClickHouse](./guides/add-full-text-search-to-your-olap-db.md).
- Searching through backups sitting on your cloud storage by adding Quickwit index files on your same storage.

# Key features of Quickwit

Quickwit is designed to search straight from object storage allowing true decoupled compute and storage. Here is a non-exhaustive list of Quickwit’s key features:

- **Scalable distributed search:** Host an arbitrary number of indexes on Amazon S3 and answer search queries in less than a second with a small pool of stateless search instances.
- **Stream indexing:** Ingest TB of data from your favorite distributed event streaming service. As of today, Quickwit supports Apache Kafka natively. The next releases will bring support for more platforms.
- **Fault-tolerant architecture that won't lose data:** Quickwit achieves **exactly-once** processing for indexing and safely stores your data on highly reliable object storage services such as Amazon S3.
- **Cloud-native, easy to operate:** Thanks to true decoupled compute and storage, search instances are stateless, add or remove search nodes within seconds.
- **Sub-second full-text search on cloud / distributed storage:** Quickwit Search re-designed indexing and index data structure to open it in less than 60ms on Amazon S3**.**
- **Time-based sharding:** Quickwit shards data by time when enabled. And you can use a second dimension to shard data thanks to our [tags feature](../design/querying.md). Time-based queries only access splits (a data piece of the index) that match the time range of the query which leads to significant performance improvements.
- **Painless multi-tenant search:** Create indexes for each tenant without hurting query performance. Or group tenants into one index and use tagging to prune irrelevant splits for your tenant query to improve significantly performance.

# When to use Quickwit

Quickwit should be a good match if your use case has some of the following characteristics:

- Your documents are immutable.
- You are targeting query latencies of 100ms to a few seconds.
- You have a low average QPS$^1$, typically < 10 QPS on average over the month. This is the case for most search use cases as long as search is not public: enterprise search, log search, email search, security search, ...
- Your data has a time component. Quickwit includes optimizations and design choices specifically related to time.
- You want to load data from Kafka, local files (and soon directly from object storage like Amazon S3).
- You want full-text search in a multi-tenant environment.

Use cases where you would likely *not* want to use Quickwit include:

- You need a low-latency search for e-commerce websites.
- Your data are mutable.

# Learn more

- [Quickstart](./get-started/quickstart.md)
- [Architecture](./design/architecture.md)
- [0.2 Release](https://quickwit.io/blog/quickwit-0.2)

1: QPS stands for Queries per second. It is a standard measure of the amount of search traffic. Low average QPS is typically under 10. This is the case for most search use cases as long as search is not public: enterprise search, log search, email search, security search, ...