# Ingest V2

Ingest V2 is the latest ingestion API that is designed to be more efficient and scalable for thousands of indexes than the previous version. It is the default since 0.9.

## Architecture

Just like ingest V1, the new ingest uses [`mrecordlog`](https://github.com/quickwit-oss/mrecordlog) to persist ingested documents that are waiting to be indexed. But unlike V1, which always persists the documents locally on the node that receives them, ingest V2 can dynamically distribute them into WAL units called _shards_. The assigned shard can be local or on another indexer. The control plane is in charge of distributing the shards to balance the indexing work as well as possible across all indexer nodes. The progress within each shard is not tracked as an index metadata checkpoint anymore but in a dedicated metastore `shards` table.

Ingest V2 flushes every WAL write from the process to the operating system before acknowledging it, so acknowledged documents survive a process crash as long as the OS and the WAL storage stay up. It does not fsync: a host failure or power loss can still lose acknowledged documents.

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

## Capping shard scale-up by indexing capacity

When ingestion throughput per shard is high, the control plane opens more shards. Under sudden load, it can open more shards than the indexers can process. Extra shards create extra indexing pipelines without increasing indexing throughput.

Setting `QW_ENABLE_INDEXING_CAPACITY_SHARD_CAP=true` on the control plane caps throughput-driven shard scale-up:

- The pipeline budget is the total CPU capacity advertised by indexers divided by `QW_CPU_MILLIS_PER_PIPELINE` (default `1000`, i.e. one pipeline per core).
- Each source with shards uses `ceil(num_shards / max_shards_per_pipeline)` pipelines. Open and closed shards are counted. `max_shards_per_pipeline` is computed with the same load model as the indexing scheduler.
- A scale-up for a source is clamped to the free slots in its last pipeline, plus `max_shards_per_pipeline` slots per pipeline left in the budget.

Scale-ups needed to reach `min_shards`, and opening the first shards of a source, are never clamped. Non-sharded sources (Kafka, etc.) are not counted in the budget.

The gauge `quickwit_control_plane_shard_scale_up_at_max_capacity` is `1` when the last scale-up was clamped, `0` otherwise.
