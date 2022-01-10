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

Quickwit also provides pruning on a second dimension called `tags`. By [setting a field as tagged](../reference/index-config.md) Quickwit will generate split metadata at indexing in order to filter splits that match requested tags at query time. Note that this metadata is only generated when the cardinality of the field is less than 1 000.  

Tag pruning is notably useful on multi-tenant datasets. 

### Search stream query limits

Search stream queries can take a huge amount of RAM. Quickwit limits the number of concurrent search streams per split to 100 by default. You can adjust this limit by setting the value of the searcher configuration property called `max_num_concurrent_split_streams` in the configuration file.

### Caching

Quickwit does caching in many places to deliver a highly performing query engine.

- Hotcache caching: A static cache that holds information about a split file internal representation. It helps speed up the opening of a split file. Its size can be defined via the `split_footer_cache_capacity` configuration parameter.
- Fast field caching: Fast fields tend to be accessed very frequently by users especially for stream requests. They are cached in a RAM whose size can be limited by the `fast_field_cache_capacity` configuration value.
