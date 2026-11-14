---
title: Metrics
sidebar_position: 70
---

Quickwit exposes key metrics in the [Prometheus](https://prometheus.io/) format on the `/metrics` endpoint. You can use any front-end that supports Prometheus to examine the behavior of Quickwit visually.

## Cache Metrics

Quickwit exposes metrics for several cache components, including `fastfields`, `fd`, `partial_request`, `predicate`, and `searcher_split`. These metrics share the same structure. Feuer-backed partial-request and predicate caches retain Quickwit's hit/miss counters with or without a disk tier. Both memory and disk hits count as cache hits. Native Feuer metrics below provide the tier breakdown, occupancy, and eviction metrics.

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

`quickwit_storage_disk_cache_startup_fallback` is a gauge with a `cache` label
(`storage`, `partial_request`, `predicate`, or `list_fields`). A value of `1` means
the cache is using memory only because disk initialization failed. A value of `0`
means no fallback occurred. Disabled caches do not emit this metric.

The `feuer_*` metrics use a `cache` label to identify the instance: `storage`, `partial_request`, `predicate`, or `list_fields`. The table lists all metric families. Labels shown are additional to `cache`; disk metrics depend on the platform and enabled tiers.

| Metric | Type | Additional labels and values | Description |
| --- | --- | --- | --- |
| `feuer_lookup_total` | counter | `outcome`: `memory_hit`, `disk_hit`, `callback`, `callback_error`, `miss` | Completed lookups. Lookup-only misses use `miss`. |
| `feuer_lookup_bytes_total` | counter | `source`: `memory`, `disk`, `callback` | Payload bytes returned by successful lookups. |
| `feuer_lookup_duration_seconds` | histogram | `outcome`: `disk_hit`, `callback`; `size_range`: `lt_100kib`, `lt_1mib`, `lt_10mib`, `lt_32mib`, `gte_32mib` | Successful lookup duration after a memory miss, including callback work. Payload-size ranges are mutually exclusive. |
| `feuer_admission_filter_total` | counter | `tier`: `memory`, `disk`; `outcome`: `accepted`, `rejected` | Completed caller-supplied admission filter decisions. |
| `feuer_memory_operations_total` | counter | `operation`: `insert`, `redundant`, `remove` | Internal memory-tier operations. |
| `feuer_memory_eviction_triggering_insertions_total` | counter | — | Insertion attempts that evicted at least one entry under capacity pressure. |
| `feuer_memory_used_bytes` | gauge | — | Cached allocation charges plus idle allocation capacity. |
| `feuer_memory_capacity_bytes` | gauge | — | Configured memory capacity. |
| `feuer_memory_entries` | gauge | — | Cached objects in memory. |
| `feuer_io_buffer_pool_bytes` | gauge | `bucket`: `32 KiB`, `256 KiB`, `512 KiB`, `1 MiB`, `2 MiB`, `4 MiB`, `8 MiB`, `16 MiB`, `32 MiB`, `64 MiB`, `>64 MiB`; `status`: `idle`, `used` | Allocation bytes by size and status. |
| `feuer_disk_lookup_total` | counter | `outcome`: `hit`, `absent`, `io_error`, `checksum_failed`, `fallthrough` | Disk-tier lookups, including buffered entries. Errors become misses. |
| `feuer_disk_lookup_duration_seconds` | histogram | — | Successful disk-tier hit duration, including buffered copies or verified reads. |
| `feuer_disk_chunks` | gauge | `state`: `free`, `allocated` | 1-MiB chunks. Allocated chunks include payloads, metadata, and unfinished writes. |
| `feuer_disk_payload_bytes` | gauge | — | Indexed payload bytes, excluding metadata and padding. |
| `feuer_disk_entries` | gauge | — | Indexed disk entries. |
| `feuer_disk_recovered_chunks` | gauge | — | Allocated chunks restored by recovery and still reserved. |
| `feuer_disk_eviction_triggering_insertions_total` | counter | — | Insertion attempts that evicted at least one entry under capacity pressure. |
| `feuer_disk_batch_bytes_total` | counter | `kind`: `payload`, `chunk` | Payload and payload-chunk bytes in successfully written regions before publication. |
| `feuer_disk_io_total` | counter | `operation`: `read`, `write`; `outcome`: `success`, `error` | Completed data-file operations. |
| `feuer_disk_io_bytes_total` | counter | `operation`: `read`, `write` | Bytes completed by data-file operations. |
| `feuer_disk_io_duration_seconds` | histogram | `operation`: `read`, `write`; `outcome`: `success` | Successful data-file operation duration. |
| `feuer_disk_read_size_bytes` | histogram | — | Requested bytes per successful data-file read, excluding alignment padding. |
| `feuer_disk_read_queue_waiters` | gauge | — | Read-piece requests waiting for I/O channel capacity. |
| `feuer_disk_read_queue_wait_duration_seconds` | histogram | — | Channel-capacity waits ending in admission, cancellation, or queue closure. |
| `feuer_disk_write_queue_total` | counter | `outcome`: `queued`, `queue_full`, `queue_closed`, `canceled`, `already_covered`, `redundant` | Write-queue admissions, rejections, and pre-write discards. |
| `feuer_disk_write_queued_entries` | gauge | — | Entries waiting to start disk writes. |
| `feuer_disk_write_pending_bytes` | gauge | — | Logical payload bytes queued, prepared, or being written. |
| `feuer_disk_write_queue_duration_seconds` | histogram | — | Time from queue admission to dequeue. |
| `feuer_disk_write_entries_total` | counter | `outcome`: `published`, `already_covered`, `no_capacity`, `failed`, `canceled` | Terminal outcomes of entries handled by the disk writer. |
| `feuer_disk_written_entries_total` | counter | — | Entries in successfully written payload regions, whether published or discarded. |

Concurrent lookups for the same exact key can share one fetch, so `feuer_lookup_total{outcome="callback"}` counts completed callers. Use object-storage GET counters to measure remote requests. The eviction counters count insertion attempts that trigger eviction, rather than the number of evicted entries.

See [Feuer's metric inventory](https://github.com/PSeitz/feuer/blob/main/metrics.md) for further interpretation and query examples.

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
