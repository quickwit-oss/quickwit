# Ingest V2

Ingest V2 is the latest ingestion API that is designed to be more efficient and scalable for thousands of indexes than the previous version. It is the default since 0.9.

## Architecture

Just like ingest V1, the new ingest uses [`mrecordlog`](https://github.com/quickwit-oss/mrecordlog) to persist ingested documents that are waiting to be indexed. But unlike V1, which always persists the documents locally on the node that receives them, ingest V2 can dynamically distribute them into WAL units called _shards_. Here are a few key behaviors of this new mechanism:
- When an indexer receives a document for ingestion, the assigned shard can be local or on another indexer.
- The control plane is in charge of distributing the shards to balance the indexing work as well as possible across all indexer nodes.
- Each shard has a throughput limit (5MB). If the ingest rate on an index is becoming greater than the cumulated throughput of all its shards, the control plane schedules the creation of new shards. Note that when the cumulated throughput is exceeded on an index, the ingest API returns "too many requests" errors until the new shards are effectively created.
- The progress within each shard is tracked in a dedicated metastore `shards` table (instead of the index metadata checkpoint like for other sources).

In the future, the shard based ingest will also be capable of writing a replica for each shard, thus ensuring a high durability of the documents that are waiting to be indexed (durability of the indexed documents is guarantied by the object store).

## Toggling between ingest V1 and V2

Variables driving the ingest configuration are documented [here](../ingest-data/ingest-api.md#ingest-api-versions).

With ingest V2, you can also activate the `enable_cooperative_indexing` option in the indexer configuration. This setting is useful for deployments with very large numbers (dozens) of actively written indexers, to limit the indexing workbench memory consumption. The indexer configuration is in the node configuration:

```yaml
version: 0.8
# [...]
indexer:
  enable_cooperative_indexing: true
```

See [full configuration example](https://github.com/quickwit-oss/quickwit/blob/main/config/quickwit.yaml).

## Differences between ingest V1 and V2

- V1 uses the `queues/` directory whereas V2 uses the `wal/` directory
- both V1 and V2 are configured with:
  - `ingest_api.max_queue_memory_usage` 
  - `ingest_api.max_queue_disk_usage` 
- but ingest V2 can also be configured with:
  - `ingest_api.replication_factor`, not working yet
- ingest V1 always writes to the WAL of the node receiving the request, V2 potentially forwards it to another node, dynamically assigned by the control plane to distribute the indexing work more evenly.
- ingest V2 parses and validates input documents synchronously. Schema and JSON formatting errors are returned in the ingest response (for ingest V1 those errors were available in the server logs only).
- ingest V2 returns transient 429 (too many requests) errors when the ingestion rate is too fast
