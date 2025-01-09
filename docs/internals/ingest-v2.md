# Ingest V2

Ingest V2 is the latest ingestion API that is designed to be more efficient and scalable for thousands of indexes than the previous version. It is the default since 0.9.

## Architecture

Just like ingest V1, the new ingest uses [`mrecordlog`](https://github.com/quickwit-oss/mrecordlog) to persist ingested documents that are waiting to be indexed. But unlike V1, which always persists the documents locally on the node that receives them, ingest V2 can dynamically distribute them into WAL units called _shards_. The assigned shard can be local or on another indexer. The control plane is in charge of distributing the shards to balance the indexing work as well as possible across all indexer nodes. The progress within each shard is not tracked as an index metadata checkpoint anymore but in a dedicated metastore `shards` table.

In the future, the shard based ingest will also be capable of writing a replica for each shard, thus ensuring a high durability of the documents that are waiting to be indexed (durability of the indexed documents is guarantied by the object store).

## Toggling between ingest V1 and V2

Variables driving the ingest configuration are documented [here](../ingest-data/ingest-api.md#ingest-api-versions).

With ingest V2, you can also activate the `enable_cooperative_indexing` option in the indexer configuration. The indexer configuration is in the node configuration:

```yaml
version: 0.8
# [...]
indexer:
  enable_cooperative_indexing: true
```

See [full configuration example](https://github.com/quickwit-oss/quickwit/blob/main/config/quickwit.yaml).

## Differences between ingest V1 and V2

- V1 uses the `queues/` directory whereas V2 uses the `wal/` directory
- both V1 and V2 are configured with
  - `ingest_api.max_queue_memory_usage` 
  - `ingest_api.max_queue_disk_usage` 
- but ingest V2 can also be configured with 
  - `ingest_api.shard_throughput_limit`, between 1MB and 20MB (/s), used to determine when to create new shards to distribute the indexing load. Should not be tweaked in 99% of the setups.
  - `ingest_api.replication_factor`, not working yet
- ingest V1 always writes to the WAL of the node receiving the request, V2 forwards it to
