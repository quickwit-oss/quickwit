---
title: Metrics
sidebar_position: 6
---

Quickwit exposes some key metrics through [Prometheus](https://prometheus.io/). You can use any front-end that supports Prometheus to visually examine the behaviour of Quickwit.

## CLI Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit` | `allocated_num_bytes` | Number of bytes allocated memory, as reported by jemallocated. | `gauge` |

## Search Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_search` | `leaf_searches_splits_total` | Number of leaf searches (count of splits) started. | `counter` |
| `quickwit_search` | `leaf_search_split_duration_secs` | Number of seconds required to run a leaf search over a single split. The timer starts after the semaphore is obtained. | `histogram` |
| `quickwit_search` | `active_search_threads_count` | Number of threads in use in the CPU thread pool | `gauge` |

## Rest API Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit` | `http_requests_total` | Total number of HTTP requests received | `counter` |

## Storage Metrics

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `quickwit_storage` | `object_storage_gets_total` | Number of objects fetched. | `counter` |
| `quickwit_storage` | `object_storage_puts_total` | Number of objects uploaded. May differ from object_storage_requests_parts due to multipart upload. | `counter` |
| `quickwit_storage` | `object_storage_puts_parts` | Number of object parts uploaded. | `counter` |
| `quickwit_storage` | `object_storage_download_num_bytes` | Amount of data downloaded from an object storage. | `counter` |

## Cache Metrics

Currently Quickwit exposes metrics for three caches: `fastfields`, `shortlived`, `splitfooter`. These metrics share the same structure.

| Namespace | Metric Name | Description | Type |
| --------- | ----------- | ----------- | ---- |
| `cache_{cache_name}` | `in_cache_count` | Count of {cache_name} in cache | `gauge` |
| `cache_{cache_name}` | `in_cache_num_bytes` | Number of {component_name} bytes in cache | `gauge` |
| `cache_{cache_name}` | `cache_hit_total` | Number of {component_name} cache hits | `counter` |
| `cache_{cache_name}` | `cache_hits_bytes` | Number of {component_name} cache hits in bytes | `counter` |
| `cache_{cache_name}` | `cache_miss_total` | Number of {component_name} cache hits | `counter` |
