---
title: Metrics
sidebar_position: 70
---

Quickwit exposes key metrics in the [Prometheus](https://prometheus.io/) format on the `/metrics` endpoint. You can use any front-end that supports Prometheus to examine the behavior of Quickwit visually.

## Cache Metrics

Quickwit exposes metrics for several cache components, including `fastfields`, `fd`, `partial_request`, `predicate`, and `searcher_split`. These metrics share the same structure. Feuer-backed memory caches retain Quickwit's hit/miss counters and expose occupancy and eviction through the native metrics below.

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_cache_{cache_name}` | `in_cache_count` | Count of {cache_name} in cache | `gauge` |
| `quickwit_cache_{cache_name}` | `in_cache_num_bytes` | Number of {cache_name} bytes in cache | `gauge` |
| `quickwit_cache_{cache_name}` | `cache_hits_total` | Number of {cache_name} cache hits | `counter` |
| `quickwit_cache_{cache_name}` | `cache_hits_bytes` | Number of {cache_name} cache hits in bytes | `counter` |
| `quickwit_cache_{cache_name}` | `cache_misses_total` | Number of {cache_name} cache hits | `counter` |
| `quickwit_cache_{cache_name}` | `cache_evict_total` | Number of {cache_name} cache entry evicted | `counter` |
| `quickwit_cache_{cache_name}` | `cache_evict_bytes` | Number of {cache_name} cache entry evicted in bytes | `counter` |

### Feuer cache metrics

Feuer metrics are exported on `/metrics` with their original names and native labels, plus a `cache` label identifying the instance: `split_range` (`disk_cache`), `partial_request` (`partial_request_cache`), `predicate` (`predicate_cache`), or `list_fields`. Sum over `cache` to aggregate instances. All caches expose the native lookup and memory metrics. Disk-backed caches additionally expose disk and I/O buffer-pool metrics. The labels listed below are additional to `cache`:

- Lookups: `feuer_lookup_total{outcome}`, `feuer_lookup_bytes_total{source}`, and `feuer_lookup_duration_seconds{outcome}`. Counters distinguish memory hits, disk hits, callbacks, callback errors, and lookup-only misses. Durations record successful disk hits and callbacks, including callback work. Result-cache lookups use the lookup-only API and report misses separately from callback errors.
- Memory: `feuer_memory_operations_total{operation}`, `feuer_memory_used_bytes`, `feuer_memory_capacity_bytes`, `feuer_memory_entries`, and `feuer_memory_compacted_payload_bytes_total`.
- Disk: lookup counters and hit-only `feuer_disk_lookup_duration_seconds` (only `cache`), `feuer_disk_chunks{state}`, `feuer_disk_payload_bytes`, `feuer_disk_entries`, eviction and batch-byte counters, and `feuer_disk_io_*` metrics. I/O durations record successful operations only.
- Disk read sizes: `feuer_disk_read_size_bytes` records requested bytes per successful data-file read, excluding alignment padding, with only `cache` and exponential buckets doubling from 1 KiB through 1 GiB. These reads include metadata and whole cached entries.
- Disk writes: `feuer_disk_write_queue_total{outcome}`, `feuer_disk_write_pending_bytes`, `feuer_disk_write_queued_entries`, and `feuer_disk_write_queue_duration_seconds`. Queue duration measures admission to dequeue (including stale entries), with only `cache` and 25 exponential buckets starting at 1 µs and doubling. `feuer_disk_write_entries_total{outcome}` records terminal write/publication outcomes. `feuer_disk_written_entries_total` counts entries in successfully written batches before publication checks.

Feuer's read-buffer pool shares the configured memory capacity with cached allocations. Idle buffers are capped at 7% of that capacity by default. `FEUER_IDLE_BUFFER_POOL_PERCENT` overrides this percentage (0–100). The buffer gauge has `bucket` labels (`32 KiB`, `256 KiB`, `512 KiB`, `1 MiB`, `2 MiB`, `4 MiB`, `8 MiB`, `16 MiB`, `32 MiB`, `64 MiB`, and `>64 MiB`) and `status` labels (`idle` or `used`). Allocations above 64 MiB share an exact-size reuse bucket.

| Metric | Type | Meaning |
|---|---|---|
| `feuer_memory_used_bytes` | Gauge | Cached allocation charges plus idle buffer capacity. Excludes active reads and caller-only results. |
| `feuer_memory_capacity_bytes` | Gauge | Configured memory capacity for the cache instance, shared with its buffer pool. |
| `feuer_io_buffer_pool_bytes` | Gauge | Allocation capacity per `bucket` and `status`. Used bytes are charged to `feuer_memory_used_bytes` only when held by cache entries. Do not add these gauges. |

See [Feuer's metric inventory](https://github.com/PSeitz/feuer/blob/965bfcb000bf4f9ce19f0bd715c8f8b0337e1095/metrics.md) for all names and label values. Concurrent callback downloads are coalesced, so `feuer_lookup_total{outcome="callback"}` is not a remote GET count. Object-storage GET counters measure actual remote requests.

## CLI Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit` | `allocated_num_bytes` | Number of bytes allocated memory, as reported by jemalloc. | `gauge` |

## Common Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit` | `write_bytes`| Number of bytes written by a given component in [`indexer`, `merger`, `deleter`, `split_downloader_{merge,delete}`] | [`index`, `component`] | `counter` |

## Indexing Metrics

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_indexing` | `processed_docs_total`| Number of processed docs by index, source and processed status in [`valid`, `schema_error`, `parse_error`, `transform_error`] | [`index`, `source`, `docs_processed_status`] | `counter` |
| `quickwit_indexing` | `processed_bytes`| Number of processed bytes by index, source and processed status in [`valid`, `schema_error`, `parse_error`, `transform_error`] | [`index`, `source`, `docs_processed_status`] | `counter` |
| `quickwit_indexing` | `available_concurrent_upload_permits`| Number of available concurrent upload permits by component in [`merger`, `indexer`] | [`component`] | `gauge` |
| `quickwit_indexing` | `ongoing_merge_operations`| Number of available concurrent upload permits by component in [`merger`, `indexer`]. | [`index`, `source`] | `gauge` |

## Ingest Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_ingest` | `docs_bytes_total` | Total size of the docs ingested, measured in ingester's leader, after validation and before persistence/replication | `counter` |
| `quickwit_ingest` | `docs_total` | Total number of the docs ingested, measured in ingester's leader, after validation and before persistence/replication | `counter` |
| `quickwit_ingest` | `queue_count` | Number of queues currently active | `counter` |

## Metastore Metrics

All metastore methods are monitored by the 3 metrics:

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_metastore` | `requests_total` | Number of requests | [`operation`, `index`] | `counter` |
| `quickwit_metastore` | `request_errors_total` | Number of failed requests | [`operation`, `index`] | `counter` |
| `quickwit_metastore` | `request_duration_seconds` | Duration of requests | [`operation`, `index`, `error`] | `histogram` |

Examples of operation names: `create_index`, `index_metadata`, `delete_index`, `stage_splits`, `publish_splits`, `list_splits`, `add_source`, ...

PostgreSQL-backed metastores also expose connection pool gauges:

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_metastore` | `active_connections` | Number of active PostgreSQL pool connections, including used and idle connections | `gauge` |
| `quickwit_metastore` | `idle_connections` | Number of idle PostgreSQL pool connections | `gauge` |
| `quickwit_metastore` | `acquire_connections` | Number of requests currently waiting to acquire a PostgreSQL pool connection | `gauge` |
| `quickwit_metastore` | `max_connections` | Maximum number of PostgreSQL pool connections configured per metastore node | `gauge` |

## Rest API Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit` | `http_requests_total` | Total number of HTTP requests received | `counter` |

## Search Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_search` | `split_search_outcome` | Number of local leaf split search outcomes by `category` (`success`, operational `error`, cache/pruning, or cancellation phase). Operational errors are classified by `error` (`create_reader`, `warmup`, `tantivy_search`, or `panic`). Retries are counted separately | `counter` |
| `quickwit_search` | `leaf_search_split_duration_secs` | Number of seconds required to run a leaf search over a single split. The timer starts after the semaphore is obtained | `histogram` |
| `quickwit_search` | `active_search_threads_count` | Number of threads in use in the CPU thread pool | `gauge` |

## Storage Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_storage` | `object_storage_gets_total` | Number of objects fetched | `counter` |
| `quickwit_storage` | `object_storage_puts_total` | Number of objects uploaded. May differ from object_storage_requests_parts due to multipart upload | `counter` |
| `quickwit_storage` | `object_storage_puts_parts` | Number of object parts uploaded | `counter` |
| `quickwit_storage` | `object_storage_download_num_bytes` | Amount of data downloaded from an object storage | `counter` |
