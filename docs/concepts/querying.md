---
title: Querying
sidebar_position: 3
---

Quickwit provides two endpoints with full-text search queries identified by the `query` parameter:

- A search endpoint that returns a [JSON](../reference/rest-api.md)
- A search stream endpoint that returns a stream of the requested [field values](../reference/rest-api.md)

A search query received by a searcher will be executed using a map-reduce approach following these steps:

1. the Searcher identifies relevant splits based on the request’s [timestamp interval](#Time-sharding) and [tags](#Tag-pruning).
2. It distributes the splits workload among other searchers available in the cluster using *[rendez-vous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing)* to optimize caching and load.
3. It finally waits for all results, merges them, and returns them to the client.

A search stream query follows the same execution path as for a search query except for the last step: instead of waiting for each Searcher's result, the searcher streams the results as soon as it starts receiving some from a searcher.

### **Time sharding**

On datasets with a time component, Quickwit will shard data into timestamp-aware splits. With this feature, Quickwit is capable of filtering out most splits before they can make it to the query processing stage, thus reducing drastically the amount of data needed to process the query.

The following query parameters are available to apply timestamped pruning to your query:

- `startTimestamp`: restricts search to documents with a `timestamp >= start_timestamp`
- `endTimestamp`: restricts search to documents with a `timestamp < end_timestamp`

### Tag pruning

Quickwit also provides pruning on a second dimension called `tags`. By [setting a field as tagged](../configuration/index-config.md) Quickwit will generate split metadata at indexing in order to filter splits that match requested tags at query time. Note that this metadata is only generated when the cardinality of the field is less than 1,000.

Tag pruning is notably useful on multi-tenant datasets.

### Partitioning

Quickwit makes it possible to route documents into different splits based on a partitioning key.

This feature is especially useful in a context where documents with different
tags are all mixed together in the same source (usually a Kafka topic).

In that case, simply marking the field as tag will have no positive effect on search, as all produced splits will contain almost all tags.

The `partition_key` attributes (defined in the doc mapping) lets you configure the logic used by Quickwit to route documents into isolated splits.
Quickwit will also enforce this isolation during merges. This functionality is, in a sense, similar to sharding.

Currently, quickwit only support partitioning over a single specific field.
Partition & tags are often used to:

- separate `tenants` in a multi-tenant application
- separate `team` or `application` in an observation logging case.

Emitting many splits can heavily stress an `indexer`. For this reason,
another parameter of the doc mapping called `max_num_partitions` acts as a safety valve. If the number of partitions is
about to exceed `max_num_partitions`, a single extra partition is created
and all extra partitions will be grouped together into this special partition.

If you are expecting 20 partitions, we strongly recommend you to not set
`max_num_partitions` to 20, but instead use a larger value (200 for instance).
Quickwit should handle that number of partitions smoothly, and it will avoid documents belonging to different partitions from being grouped together due to
a few faulty documents.

### Search stream query limits

Search stream queries can take a huge amount of RAM. Quickwit limits the number of concurrent search streams per split to 100 by default. You can adjust this limit by setting the value of the searcher configuration property called `max_num_concurrent_split_streams` in the configuration file.

### Caching

Quickwit does caching in many places to deliver a highly performing query engine.

- Hotcache caching: A static cache that holds information about a split file internal representation. It helps speed up the opening of a split file. Its size can be defined via the `split_footer_cache_capacity` configuration parameter.
- Fast field caching: Fast fields tend to be accessed very frequently by users especially for stream requests. They are cached in a RAM whose size can be limited by the `fast_field_cache_capacity` configuration value.

### Scoring

Quickwit supports sorting docs by their BM25 scores. In order to query by score, [fieldnorms](../configuration/index-config.md#Text-type) must be enabled for the field. By default BM25 scoring is disabled to improve query times but it can be opt-in by setting `sort_by_field` option to `_score` in queries.
