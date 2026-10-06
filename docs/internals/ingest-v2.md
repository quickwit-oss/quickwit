# Ingest V2

Ingest V2 is the latest ingestion API that is designed to be more efficient and scalable for thousands of indexes than the previous version. It is the default since 0.9.

## Architecture

Just like ingest V1, the new ingest uses [`mrecordlog`](https://github.com/quickwit-oss/mrecordlog) to persist ingested documents that are waiting to be indexed. But unlike V1, which always persists the documents locally on the node that receives them, ingest V2 can dynamically distribute them into WAL units called _shards_. The assigned shard can be local or on another indexer. The control plane is in charge of distributing the shards to balance the indexing work as well as possible across all indexer nodes. The progress within each shard is not tracked as an index metadata checkpoint anymore but in a dedicated metastore `shards` table.

In the future, the shard based ingest will also be capable of writing a replica for each shard, thus ensuring a high durability of the documents that are waiting to be indexed (durability of the indexed documents is guarantied by the object store).

### Shard throughput metering

Each ingester owns one shard rate meter through an `Arc<SharedRateMeter>` shared with its shards. The meter owns the per-shard map and its private mutex, exposing operations that manage locking internally. The shard builder inserts an entry with the source, identity, state, and advertisability, and dropping the shard removes that entry. Closure and advertisability changes update the entry. Recovered WAL queues create closed, advertisable entries with fresh rate histories.

Successful WAL appends add the validated document batch's byte count to its meter entry. Harvesting resets pending byte counts and updates the existing short-term and long-term rate windows. Readings include advertisable shards even when they are idle or closed, until the shards are dropped.

The meter's private synchronous mutex is independent of the ingester state and WAL locks. Its operations perform only in-memory work and never hold the mutex across an await. Operational code may call meter methods while holding state locks; the meter never acquires those locks in return. Harvesting groups readings by source after releasing the meter mutex.

The ingester exposes a shared meter handle through `shared_rate_meter_rx()`, a watch receiver populated after initialization and recovery complete. Counter updates do not send watch notifications. The shard readings publisher is the sole harvester: every second it clones the handle from the watch receiver, harvests the meter, publishes a snapshot through the existing reporting channel, and updates shard metrics. It acquires neither the ingester state lock nor the WAL lock, so sampling continues during WAL I/O stalls and idle periods. The publisher skips missed timer ticks, waits for initialization, and exits if initialization fails or the ingester state is dropped. The persist path only records successfully appended bytes.

### Shard reporting rollout

During the compatibility release, shard reporting and scaling switch when all ready indexers advertise `enable_shard_scaling_v2`. The gossip broadcaster checks this through `Cluster::all_indexers_migrated()`, which waits until the local node appears as ready before allowing the task to exit. This prevents the broadcaster from stopping on an empty startup view. The gRPC reporter and control plane check the ready-indexer membership in their ingester pools. Pool entries refresh when this capability or ingester status changes. The pool predicate retains its existing behavior for an empty membership view: all indexers are considered migrated.

Until all indexers have migrated, ingesters broadcast the latest completed shard snapshot through gossip every five seconds. The legacy `ingester.primary_shards:` keys and rounded-up integer MiB/s representation are preserved. Only sources whose serialized legacy representation changes are updated, and removed sources have their keys deleted. The control plane consumes these events through the existing listener and event broker, using the legacy scaling controller.

Once all indexers have migrated, the gossip broadcaster removes its previously published shard keys and exits its five-second broadcast loop. The indexer state reporter includes shard snapshots in its one-second gRPC reports, and the control plane accepts them with the existing generation checks and uses v2 periodic reconciliation. Shard reports received through the inactive transport do not update the model or trigger scaling. Running-indexing-task reporting keeps its existing behavior.

The gossip task does not restart after cutover. Returning to legacy reporting requires restarting the indexers. Processes make the cutover decision independently from their current membership views, and their views can differ briefly while changes propagate into the pools. Neither transport harvests rates: the shared publisher remains the sole one-second harvester and metrics reporter. Capacity-score broadcasting also retains its one-second interval. The legacy wiring remains available for removal in the following release.

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
