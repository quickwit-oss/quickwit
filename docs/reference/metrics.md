---
title: Metrics
sidebar_position: 70
---

Quickwit exposes key metrics in the [Prometheus](https://prometheus.io/) format on the `/metrics` endpoint. You can use any front-end that supports Prometheus to examine the behavior of Quickwit visually.

:::tip

Workloads with a large number of indexes generate high cardinality metrics for the label `index`. Set the environment variable `QW_DISABLE_PER_INDEX_METRICS=true` to disable that label if this is problematic for your metrics database.

:::

## Cache Metrics

Quickwit exposes several metrics every caches. The cache type is defined in the `component_name` label. Values are `fastfields`, `shortlived`, `splitfooter`, `fd`, `partial_request`, and `searcher_split`.

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_cache` | `in_cache_count` | Count of entries in cache by component | [`component_name`] | `gauge` |
| `quickwit_cache` | `in_cache_num_bytes` | Number of bytes in cache by component | [`component_name`] | `gauge` |
| `quickwit_cache` | `cache_hits_total` | Number of cache hits by component | [`component_name`] | `counter` |
| `quickwit_cache` | `cache_hits_bytes` | Number of cache hits in bytes by component | [`component_name`] | `counter` |
| `quickwit_cache` | `cache_misses_total` | Number of cache misses by component | [`component_name`] | `counter` |
| `quickwit_cache` | `cache_evict_total` | Number of cache entries evicted by component | [`component_name`] | `counter` |
| `quickwit_cache` | `cache_evict_bytes` | Number of cache entries evicted in bytes by component | [`component_name`] | `counter` |

## Cluster Metrics

Cluster metrics help track the behavior of the Chitchat protocol.

Note: the cluster protocol uses GRPC to catch up large deltas in its state. Those calls are monitored as [GRPC metrics](#grpc-metrics).

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_cluster` | `live_nodes` | The number of live nodes observed locally | `gauge` |
| `quickwit_cluster` | `ready_nodes` | The number of ready nodes observed locally | `gauge` |
| `quickwit_cluster` | `zombie_nodes` | The number of zombie nodes observed locally | `gauge` |
| `quickwit_cluster` | `dead_nodes` | The number of dead nodes observed locally | `gauge` |
| `quickwit_cluster` | `cluster_state_size_bytes` | The size of the cluster state in bytes | `gauge` |
| `quickwit_cluster` | `node_state_size_bytes` | The size of the node state in bytes | `gauge` |
| `quickwit_cluster` | `node_state_keys` | The number of keys in the node state | `gauge` |
| `quickwit_cluster` | `gossip_recv_messages_total` | Total number of gossip messages received | `counter` |
| `quickwit_cluster` | `gossip_recv_bytes_total` | Total amount of gossip data received in bytes | `counter` |
| `quickwit_cluster` | `gossip_sent_messages_total` | Total number of gossip messages sent | `counter` |
| `quickwit_cluster` | `gossip_sent_bytes_total` | Total amount of gossip data sent in bytes | `counter` |
| `quickwit_cluster` | `grpc_gossip_rounds_total` | Total number of gRPC gossip rounds performed with peer nodes | `counter` |

## Control Plane Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_control_plane` | `indexes_total` | Number of indexes | | `gauge` |
| `quickwit_control_plane` | `restart_total` | Number of control plane restarts | | `counter` |
| `quickwit_control_plane` | `schedule_total` | Number of control plane schedule operations | | `counter` |
| `quickwit_control_plane` | `apply_total` | Number of control plane apply plan operations | | `counter` |
| `quickwit_control_plane` | `metastore_error_aborted` | Number of aborted metastore transactions (do not trigger a control plane restart) | | `counter` |
| `quickwit_control_plane` | `metastore_error_maybe_executed` | Number of metastore transactions with an uncertain outcome (do trigger a control plane restart) | | `counter` |
| `quickwit_control_plane` | `open_shards_total` | Number of open shards per source | [`index_id`] | `gauge` |
| `quickwit_control_plane` | `shards` | Number of (remote/local) shards in the indexing plan | [`locality`] | `gauge` |

## GRPC Metrics

The following subsystems expose gRPC metrics: `cluster`, `control_plane`, `indexing`, `ingest`, `metastore`.

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_{subsystem}` | `grpc_requests_total` | Total number of gRPC requests processed | [`kind`, `rpc`, `status`] | `counter` |
| `quickwit_{subsystem}` | `grpc_requests_in_flight` | Number of gRPC requests in-flight | [`kind`, `rpc`] | `gauge` |
| `quickwit_{subsystem}` | `grpc_request_duration_seconds` | Duration of request in seconds | [`kind`, `rpc`, `status`] | `histogram` |
| `quickwit_grpc` | `circuit_break_total` | Circuit breaker counter | | `counter` |

## Indexing Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_indexing` | `processed_docs_total` | Number of processed docs by index and processed status | [`index`, `docs_processed_status`] | `counter` |
| `quickwit_indexing` | `processed_bytes` | Number of bytes of processed documents by index and processed status | [`index`, `docs_processed_status`] | `counter` |
| `quickwit_indexing` | `backpressure_micros` | Amount of time spent in backpressure (in micros) | [`actor_name`] | `counter` |
| `quickwit_indexing` | `concurrent_upload_available_permits_num` | Number of available concurrent upload permits by component | [`component`] | `gauge` |
| `quickwit_indexing` | `split_builders` | Number of existing index writer instances | | `gauge` |
| `quickwit_indexing` | `ongoing_merge_operations` | Number of ongoing merge operations | | `gauge` |
| `quickwit_indexing` | `pending_merge_operations` | Number of pending merge operations | | `gauge` |
| `quickwit_indexing` | `pending_merge_bytes` | Number of pending merge bytes | | `gauge` |
| `quickwit_indexing` | `kafka_rebalance_total` | Number of kafka rebalances | | `counter` |

## Ingest Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_ingest` | `docs_total` | Total number of docs ingested, measured in ingester's leader | [`validity`] | `counter` |
| `quickwit_ingest` | `docs_bytes_total` | Total size of the docs ingested in bytes, measured in ingester's leader | [`validity`] | `counter` |
| `quickwit_ingest` | `ingest_result_total` | Number of ingest requests by result | [`result`] | `counter` |
| `quickwit_ingest` | `reset_shards_operations_total` | Total number of reset shards operations performed | [`status`] | `counter` |
| `quickwit_ingest` | `shards` | Number of shards hosted by the ingester | [`state`] | `gauge` |
| `quickwit_ingest` | `shard_lt_throughput_mib` | Shard long term throughput as reported through chitchat | | `histogram` |
| `quickwit_ingest` | `shard_st_throughput_mib` | Shard short term throughput as reported through chitchat | | `histogram` |
| `quickwit_ingest` | `wal_acquire_lock_requests_in_flight` | Number of acquire lock requests in-flight | [`operation`, `type`] | `gauge` |
| `quickwit_ingest` | `wal_acquire_lock_request_duration_secs` | Duration of acquire lock requests in seconds | [`operation`, `type`] | `histogram` |
| `quickwit_ingest` | `wal_disk_used_bytes` | WAL disk space used in bytes | | `gauge` |
| `quickwit_ingest` | `wal_memory_used_bytes` | WAL memory used in bytes | | `gauge` |
<!-- uncomment when replication is released
| `quickwit_ingest` | `replicated_num_bytes_total` | Total size in bytes of the replicated docs | | `counter` |
| `quickwit_ingest` | `replicated_num_docs_total` | Total number of docs replicated | | `counter` |
-->

Note that the legacy ingest (V1) only records the `docs_total` and `docs_bytes_total` metrics. The `validity` label is always set to `valid` because it doesn't parse the documents at ingest time. Invalid documents are discarded asynchronously in the indexing pipeline's doc processor.

## Janitor Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_janitor` | `ongoing_num_delete_operations_total` | Number of ongoing delete operations per index | [`index`] | `gauge` |
| `quickwit_janitor` | `gc_deleted_splits_total` | Total number of splits deleted by the garbage collector | [`result`] | `counter` |
| `quickwit_janitor` | `gc_deleted_bytes_total` | Total number of bytes deleted by the garbage collector | | `counter` |
| `quickwit_janitor` | `gc_runs_total` | Total number of garbage collector executions | [`result`] | `counter` |
| `quickwit_janitor` | `gc_seconds_total` | Total time spent running the garbage collector | | `counter` |

## Jaeger Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_jaeger` | `requests_total` | Number of requests | [`operation`, `index`] | `counter` |
| `quickwit_jaeger` | `request_errors_total` | Number of failed requests | [`operation`, `index`] | `counter` |
| `quickwit_jaeger` | `request_duration_seconds` | Duration of requests | [`operation`, `index`, `error`] | `histogram` |
| `quickwit_jaeger` | `fetched_traces_total` | Number of traces retrieved from storage | [`operation`, `index`] | `counter` |
| `quickwit_jaeger` | `fetched_spans_total` | Number of spans retrieved from storage | [`operation`, `index`] | `counter` |
| `quickwit_jaeger` | `transferred_bytes_total` | Number of bytes transferred | [`operation`, `index`] | `counter` |

## Memory Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_memory` | `active_bytes` | Total number of bytes in active pages allocated by the application, as reported by jemalloc `stats.active` | | `gauge` |
| `quickwit_memory` | `allocated_bytes` | Total number of bytes allocated by the application, as reported by jemalloc `stats.allocated` | | `gauge` |
| `quickwit_memory` | `resident_bytes` | Total number of bytes in physically resident data pages mapped by the allocator, as reported by jemalloc `stats.resident` | | `gauge` |
| `quickwit_memory` | `in_flight_data_bytes` | Amount of data in-flight in various buffers in bytes | [`component`] | `gauge` |

## Metastore Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_metastore` | `acquire_connections` | Number of connections being acquired (PostgreSQL only) | | `gauge` |
| `quickwit_metastore` | `active_connections` | Number of active (used + idle) connections (PostgreSQL only) | | `gauge` |
| `quickwit_metastore` | `idle_connections` | Number of idle connections (PostgreSQL only) | | `gauge` |

## OTLP Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_otlp` | `requests_total` | Number of requests | [`service`, `index`, `transport`, `format`] | `counter` |
| `quickwit_otlp` | `request_errors_total` | Number of failed requests | [`service`, `index`, `transport`, `format`] | `counter` |
| `quickwit_otlp` | `request_duration_seconds` | Duration of requests | [`service`, `index`, `transport`, `format`, `error`] | `histogram` |
| `quickwit_otlp` | `ingested_log_records_total` | Number of log records ingested | [`service`, `index`, `transport`, `format`] | `counter` |
| `quickwit_otlp` | `ingested_spans_total` | Number of spans ingested | [`service`, `index`, `transport`, `format`] | `counter` |
| `quickwit_otlp` | `ingested_bytes_total` | Number of bytes ingested | [`service`, `index`, `transport`, `format`] | `counter` |

## REST API Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit` | `http_requests_total` | Total number of HTTP requests processed | [`method`, `status_code`] | `counter` |
| `quickwit` | `request_duration_secs` | Response time in seconds | [`method`, `status_code`] | `histogram` |
| `quickwit` | `ongoing_requests` | Number of ongoing requests on specific endpoint groups | [`endpoint_group`] | `gauge` |
| `quickwit` | `pending_requests` | Number of pending requests on specific endpoint groups | [`endpoint_group`] | `gauge` |

## Search Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_search` | `root_search_requests_total` | Total number of root search gRPC requests processed | [`status`] | `counter` |
| `quickwit_search` | `root_search_request_duration_seconds` | Duration of root search gRPC requests in seconds | [`status`] | `histogram` |
| `quickwit_search` | `root_search_targeted_splits` | Number of splits targeted per root search gRPC request | [`status`] | `histogram` |
| `quickwit_search` | `leaf_search_requests_total` | Total number of leaf search gRPC requests processed | [`status`] | `counter` |
| `quickwit_search` | `leaf_search_request_duration_seconds` | Duration of leaf search gRPC requests in seconds | [`status`] | `histogram` |
| `quickwit_search` | `leaf_search_targeted_splits` | Number of splits targeted per leaf search gRPC request | [`status`] | `histogram` |
| `quickwit_search` | `leaf_searches_splits_total` | Number of leaf searches (count of splits) started | | `counter` |
| `quickwit_search` | `leaf_search_split_duration_secs` | Number of seconds required to run a leaf search over a single split. The timer starts after the semaphore is obtained | | `histogram` |
| `quickwit_search` | `leaf_search_single_split_tasks` | Number of single split search tasks pending or ongoing | [`status`] | `gauge` |
| `quickwit_search` | `leaf_search_single_split_warmup_num_bytes` | Size of the short lived cache for a single split once the warmup is done | | `histogram` |
| `quickwit_search` | `job_assigned_total` | Number of jobs assigned from this searcher (root) to other searchers (leafs), per affinity rank | [`affinity`] | `counter` |
| `quickwit_search` | `searcher_local_kv_store_size_bytes` | Size of the searcher kv store in bytes. This store is used to cache scroll contexts | | `gauge` |

## Storage Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_storage` | `get_slice_timeout_outcome` | Outcome of get_slice operations. success_after_1_timeout means the operation succeeded after a retry caused by a timeout | [`outcome`] | `counter` |
| `quickwit_storage` | `object_storage_requests_total` | Number of requests to the object store, by action and status. Requests are recorded when the response headers are returned | [`action`, `status`] | `counter` |
| `quickwit_storage` | `object_storage_request_duration` | Durations until the response headers are returned from the object store, by action and status | [`action`, `status`] | `histogram` |
| `quickwit_storage` | `object_storage_download_num_bytes` | Amount of data downloaded from object storage | [`status`] | `counter` |
| `quickwit_storage` | `object_storage_download_errors` | Number of download requests that received successful response headers but failed during download | [`status`] | `counter` |
| `quickwit_storage` | `object_storage_upload_num_bytes` | Amount of data uploaded to object storage. The value recorded for failed and aborted uploads is the full payload size | [`status`] | `counter` |

## CLI Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_cli` | `thread_unpark_duration_microseconds` | Duration for which a thread of the main tokio runtime is unparked | `histogram` |
