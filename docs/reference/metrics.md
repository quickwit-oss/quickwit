---
title: Metrics
sidebar_position: 70
---

Quickwit exposes key metrics in the [Prometheus](https://prometheus.io/) format on the `/metrics` endpoint. You can use any front-end that supports Prometheus to examine the behavior of Quickwit visually.

## Cache Metrics

Quickwit exposes metrics for several cache components, including `fastfields`, `fd`, `partial_request`, `predicate`, `searcher_split`, and `splitfooter`. These metrics share the same structure.

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_cache_{cache_name}` | `in_cache_count` | Count of {cache_name} in cache | `gauge` |
| `quickwit_cache_{cache_name}` | `in_cache_num_bytes` | Number of {cache_name} bytes in cache | `gauge` |
| `quickwit_cache_{cache_name}` | `cache_hits_total` | Number of {cache_name} cache hits | `counter` |
| `quickwit_cache_{cache_name}` | `cache_hits_bytes` | Number of {cache_name} cache hits in bytes | `counter` |
| `quickwit_cache_{cache_name}` | `cache_misses_total` | Number of {cache_name} cache hits | `counter` |
| `quickwit_cache_{cache_name}` | `cache_evict_total` | Number of {cache_name} cache entry evicted | `counter` |
| `quickwit_cache_{cache_name}` | `cache_evict_bytes` | Number of {cache_name} cache entry evicted in bytes | `counter` |

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

## Runtime Metrics

Runtime metrics are labeled by `runtime_type` (`main`, `blocking`, `non_blocking`). One OS thread samples runtime and task metrics every second, so Tokio saturation cannot delay the sampler itself. Tokio publishes worker busy time when workers park or perform scheduler maintenance; use rates over several minutes to smooth these updates. Sampling does not account for unfinished work: poll duration is recorded when a poll returns, and scheduling delay when a task is polled again.

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_runtime` | `tokio_worker_busy_duration_milliseconds_total` | Cumulative busy time summed over all runtime workers, in milliseconds | [`runtime_type`] | `counter` |
| `quickwit_runtime` | `tokio_worker_threads` | Number of worker threads of the runtime | [`runtime_type`] | `gauge` |
| `quickwit_runtime` | `tokio_scheduled_tasks` | Number of tasks waiting in the workers' local run queues when last sampled. Excludes the global queue and the LIFO slot | [`runtime_type`] | `gauge` |

The `tokio_worker_busy_ratio` gauge has been removed. To calculate utilization as a fraction, use:

```promql
rate(quickwit_runtime_tokio_worker_busy_duration_milliseconds_total[5m])
  / (1000 * quickwit_runtime_tokio_worker_threads)
```

Task metrics are labeled by `task`, a static actor type or role name, and by `runtime_type`. The default task name is the full Rust type name. Source actors use the source implementation's type name; packagers, publishers, and uploaders distinguish their pipeline roles. Names exclude index IDs, source IDs, and other instance details. Test or externally supplied runtimes that have not been registered use `unregistered` and produce a rate-limited warning.

Each task group shares a monitor across actor instances and restarts. Monitors remain registered for the process lifetime so exported counters retain their cumulative totals. Runtime registration must precede spawning its actors; repeating a registration is harmless, but changing a runtime's label or sharing it with another runtime is rejected.

Task metrics cover the actor loop, excluding separately spawned tasks. Scheduling delay measures wake-to-poll time, not time spent in the actor mailbox. Poll duration is wall-clock time, including synchronous blocking and OS descheduling; it is not CPU time.

| Namespace | Metric Name | Description | Labels | Type |
| --------- | ----------- | ----------- | ------ | ---- |
| `quickwit_runtime` | `tokio_task_scheduled_duration_microseconds_total` | Cumulative wake-to-poll delay, excluding the initial wait before the first poll | [`task`, `runtime_type`] | `counter` |
| `quickwit_runtime` | `tokio_task_schedules_total` | Number of observed wake-to-poll intervals; multiple wakes before a poll count once | [`task`, `runtime_type`] | `counter` |
| `quickwit_runtime` | `tokio_task_poll_duration_microseconds_total` | Wall-clock time spent polling actor futures | [`task`, `runtime_type`] | `counter` |
| `quickwit_runtime` | `tokio_task_polls_total` | Number of times tasks were polled | [`task`, `runtime_type`] | `counter` |
| `quickwit_runtime` | `tokio_task_slow_polls_total` | Number of polls lasting at least 50µs | [`task`, `runtime_type`] | `counter` |
| `quickwit_runtime` | `tokio_task_idle_duration_microseconds_total` | Time tasks spent idle, waiting to be woken up by an external event | [`task`, `runtime_type`] | `counter` |

For mean scheduling delay in microseconds over a recent window, divide rates over that same window:

```promql
rate(quickwit_runtime_tokio_task_scheduled_duration_microseconds_total[5m])
  / rate(quickwit_runtime_tokio_task_schedules_total[5m])
```

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
