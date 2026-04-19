---
title: Upgrade
sidebar_position: 5
---

This page documents breaking changes and migration steps when upgrading Quickwit. If you are upgrading across multiple minor versions, apply the steps for each intermediate version in order.

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

### Rust toolchain (for source builds only)

Building from source now requires Rust **1.92** (tracked in `rust-toolchain.toml`). Users of the published Docker images or prebuilt binaries are unaffected.

### Rolling back

Rollback is one-way for the metastore. If you must return to 0.8.x, restore the metastore from the backup taken in step 1 of "Before you upgrade" and shut down all 0.9 nodes before starting any 0.8.x node.

## Upgrading to earlier versions

See the [CHANGELOG](https://github.com/quickwit-oss/quickwit/blob/main/CHANGELOG.md) for breaking changes in prior releases.
