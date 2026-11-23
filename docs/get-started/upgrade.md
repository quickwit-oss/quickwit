---
title: Upgrade
sidebar_position: 5
---

This page documents breaking changes and migration steps when upgrading Quickwit. If you are upgrading across multiple minor versions, apply the steps for each intermediate version in order.

## Unreleased

### Searcher cache configuration

The searcher now uses a shared storage cache for split footers, hotcache, fast fields, and other split body ranges. Partial-request and predicate caches remain separate. All three support an optional disk tier.

:::warning Cache policies and virtual caches are ignored

The `policy` and `virtual_caches` options in `searcher.fast_field_cache`, `split_footer_cache`, `partial_request_cache`, and `predicate_cache`, including their `_capacity` aliases, are accepted for compatibility and have no effect. Existing configurations using these options continue to load. You can remove them when updating your configuration.

Eviction is managed by the cache implementation. Virtual-cache simulations and their metrics are no longer available.

:::

The legacy `fast_field_cache` and `split_footer_cache` settings, including their `_capacity` aliases, still accept memory capacities. When `storage_cache` is omitted or `null`, these budgets are added together into one memory-only storage cache. Omitted budgets contribute their defaults: `1G` for fast fields and `500M` for footers. The capacities now form a shared budget rather than separate reservations for each kind of data.

Prefer replacing both legacy budgets with an explicit `storage_cache`. Set both `memory_capacity` and `disk_capacity`. Explicit settings override the legacy budgets, but legacy settings are still validated. To disable the storage cache, set both capacities to zero. Setting only one legacy budget to zero leaves the other budget active.

For example, migrate this configuration:

```yaml
searcher:
  fast_field_cache:
    capacity: 1G
    policy: lru
    virtual_caches:
      - capacity: 2G
  split_footer_cache_capacity: 500M
  partial_request_cache:
    capacity: 64M
    policy: lru
  predicate_cache:
    capacity: 256M
    policy: s3-fifo
```

To this memory-only configuration, preserving the total configured memory budget:

```yaml
searcher:
  storage_cache:
    memory_capacity: 1500M
    disk_capacity: 0
  partial_request_cache:
    memory_capacity: 64M
  predicate_cache:
    memory_capacity: 256M
```

Scalar capacities, the `capacity` alias for `memory_capacity` in legacy and result-cache mappings, and the `partial_request_cache_capacity` and `predicate_cache_capacity` aliases remain supported. `storage_cache` requires the `memory_capacity` field name.

To enable a disk tier, use `disk_capacity` under `storage_cache`, `partial_request_cache`, or `predicate_cache`, and set the shared `searcher.storage_cache_directory`. Legacy fast-field and footer settings reject `disk_capacity`, including `0`. Per-cache `path` and `directory` fields are unsupported. Disk caching requires Linux with usable `io_uring` and a filesystem supporting direct I/O. Initialization failures prevent startup. See [cache configuration](../configuration/node-config.md#searcher-cache-configuration) and [disk requirements](../deployment/cluster-sizing.md#searchers).

Update dashboards and alerts that use the former fast-field, split-footer, or virtual-cache metrics. See the [cache metrics reference](../reference/metrics.md) for the replacement metrics.

## Upgrading to 0.9

1. **Back up your metastore.** If you use the PostgreSQL metastore, take a snapshot. If you use the file-backed metastore, keep a copy of the `indexes/` directory. 0.9 runs SQL migrations that add new columns (`maturity`, compaction-related fields) on first start; rolling back requires the backup.
2. **Back up your index data (optional).** The on-disk split format and object-storage layout are unchanged, so no re-indexing is needed. A backup is still recommended if your storage is not already versioned.
3. **Stop all 0.8.x nodes before starting any 0.9 node.** Mixed-version clusters are not supported during the migration.

### Ingest V2

In 0.8.x, ingest V2 was opt-in via `QW_ENABLE_INGEST_V2=true` and was served from a dedicated route `POST /api/v1/{index}/ingest-v2`.

In 0.9:

- `QW_ENABLE_INGEST_V2` now defaults to `true`.
- `POST /api/v1/{index}/ingest` routes to V2 automatically. V1 still exists internally and can be reached per-request with `?use_legacy_ingest=true`, or cluster-wide by setting `QW_ENABLE_INGEST_V2=false`.
- `POST /api/v1/{index}/ingest-v2` has been removed.
- `QW_DISABLE_INGEST_V1=true` lets you force pure-V2 operation once all clients are migrated.

**What changes operationally.** Ingest V2 shards commits across multiple indexers running in parallel. On a freshly-stopped ingest stream you may observe more (and smaller) splits than V1 produced, because the tail splits of each shard do not always meet the merge policy's `merge_factor` threshold. If you rely on a low steady-state split count, tune `indexing_settings.merge_policy.merge_factor` down (for example to `2`) in your index config or run the merge-on-demand path.

### Configuration changes

- **`rest_listen_port` is deprecated.** Move it under the new `rest` block:
  ```yaml
  # before (0.8.x, still works in 0.9 with a deprecation warning)
  rest_listen_port: 7280

  # after (0.9)
  rest:
    listen_port: 7280
  ```
  The old field keeps working for the 0.9 cycle to ease the transition; plan to remove it in 0.10.
- **Stemming is now gated behind the `multilang` cargo feature.** If you build Quickwit from source and rely on stemming, add `--features multilang`. The official binaries and Docker images are built with `multilang` enabled, so users of the distributed images are unaffected.
- The unused `multilang` *tokenizer* feature (distinct from the `multilang` cargo feature above) was removed. If you used a custom doc mapper referencing it, switch to the standard tokenizers.

### Metric name changes

Quickwit metrics now use the `metrics-rs` instrumentation stack. Most metric names are unchanged, but two groups changed and may require dashboard or alert updates:

- gRPC metrics now use a `service` label instead of embedding the service name in the metric name:
  - `quickwit_<service>_grpc_requests_total` becomes `quickwit_grpc_requests_total{service="<service>"}`
  - `quickwit_<service>_grpc_requests_in_flight` becomes `quickwit_grpc_requests_in_flight{service="<service>"}`
  - `quickwit_<service>_grpc_request_duration_seconds` becomes `quickwit_grpc_request_duration_seconds{service="<service>"}`
- Janitor GC metrics no longer include the duplicated `quickwit_` prefix from the old `quickwit_janitor` namespace. For example, `quickwit_quickwit_janitor_gc_deleted_bytes_total` becomes `quickwit_janitor_gc_deleted_bytes_total`.

### Rust toolchain (for source builds only)

Building from source now requires Rust **1.92** (tracked in `rust-toolchain.toml`). Users of the published Docker images or prebuilt binaries are unaffected.

### Rolling back

Rollback is one-way for the metastore. If you must return to 0.8.x, restore the metastore from the backup taken in step 1 of "Before you upgrade" and shut down all 0.9 nodes before starting any 0.8.x node.

## Upgrading to earlier versions

See the [CHANGELOG](https://github.com/quickwit-oss/quickwit/blob/main/CHANGELOG.md) for breaking changes in prior releases.
