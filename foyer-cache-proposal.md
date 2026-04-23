# Proposal: Hybrid Caching with Foyer for Search Performance

**Date:** 2026-04-19
**Author:** François Massot
**Status:** Draft

## Problem

As more customers create log monitors and dashboards, the first 24h of data is accessed repeatedly — monitors evaluate every 1-5 minutes, dashboards reload on every page view. Today, all search caches are **memory-only**. When memory is full, frequently accessed data gets evicted, and the next query must fetch split data from S3 again (~50-200ms per split).

At Dailymotion scale (2PB stored, 8 searchers × 56GB RAM), memory caches fill quickly and the hit rate drops. This causes:
- Dashboard timeouts (30s)
- Monitor "No Data" evaluations
- CPU hotspots on searchers processing the same data repeatedly

## Current Caching Architecture

There are 5 caches in the search path today. **All are memory-only** except the split file cache:

| Cache | What it stores | Default size | Location |
|---|---|---|---|
| **Split file cache** | Entire split files on local disk | Configurable | `quickwit-storage/src/split_cache/` |
| **Split footer cache** | Tantivy index metadata per split | 500 MB | `quickwit-search/src/leaf.rs` |
| **Fast field cache** | `.fast` columnar field data | 1 GB | `quickwit-storage/src/cache/quickwit_cache.rs` |
| **Partial request cache** | Encoded `LeafSearchResponse` per (split, query) | 64 MB | `quickwit-search/src/leaf_cache.rs` |
| **Predicate cache** | Query AST evaluation results (HitSet bitmaps) | 256 MB | `quickwit-search/src/leaf_cache.rs` |

### Why not use the existing split file cache?

The split file cache downloads and stores **entire split files** on local NVMe. It's designed for a different use case: avoiding repeated S3 downloads for the same split. It doesn't help with:
- Caching parsed/decoded data (footers, fast fields) — still needs CPU to parse from the split file
- Caching computed results (search responses, predicate evaluations) — not stored at all
- Fine-grained eviction — it caches whole splits (~500MB-1GB each), not the hot slices within them

## Proposal: Replace Memory Caches with Foyer Hybrid Caches

[Foyer](https://github.com/foyer-rs/foyer) is a Rust hybrid cache library that provides **memory + disk tiered caching** with modern eviction algorithms. It's actively maintained, used in RisingWave (streaming database), and designed for exactly this use case.

### What foyer gives us

1. **Hybrid memory + disk caching** — When an entry is evicted from memory, it spills to local NVMe instead of being lost. The next access reads from NVMe (~0.1ms) instead of S3 (~50-200ms).

2. **Modern eviction algorithms** — S3-FIFO, LRU, LFU — we already implement some of these ourselves. Foyer gives us battle-tested implementations.

3. **Async-native** — Built on tokio, no blocking I/O in the cache path.

4. **Admission control** — Can reject items that aren't worth caching (e.g., one-off queries), keeping the cache focused on monitor/dashboard patterns.

### Which caches to migrate

Not all caches benefit equally from disk spilling. The key insight is: **which caches have entries that are expensive to recompute and accessed repeatedly?**

| Cache | Migrate to foyer? | Why |
|---|---|---|
| **Partial request cache** | **Yes — highest impact** | Monitor and dashboard queries hit the same (split, query) pairs every 1-5 min. At 64MB memory, this fills instantly at scale. Spilling to NVMe means repeated monitor evaluations hit disk cache instead of re-executing the search. |
| **Fast field cache** | **Yes — high impact** | Fast fields are the columnar data used for aggregations (the core of dashboard queries). At 1GB memory, hot fields from the last 24h of splits get evicted too quickly at scale. |
| **Split footer cache** | **Yes — moderate impact** | Footers are small (~10-50KB per split) but accessed on every query. 500MB is usually enough, but hybrid caching adds a safety net. |
| **Predicate cache** | **Maybe** | Query AST evaluations are useful for repeated identical queries, but the bitmap size varies wildly. Worth benchmarking. |
| **Split file cache** | **No** | Already on disk. Keep as-is. |

### Expected impact

**Monitors (highest impact):** A log monitor evaluating every minute on a 15-minute window touches the same ~15-30 splits repeatedly. With memory-only caching, if the partial request cache is full, each evaluation re-executes the search (~200-500ms per split). With foyer, evicted entries spill to NVMe and the next evaluation reads from disk (~1-5ms per split). Expected speedup: **10-50x for cache-evicted monitor queries**.

**Dashboards:** A dashboard with 10 widgets, each querying the last 4h, touches ~40-80 splits. On reload, most of these should be in cache. With foyer, the working set for the last 24h stays on NVMe even when memory is full. Expected improvement: **dashboards that currently timeout (30s) should load in <5s**.

## Implementation Plan

### Phase 1: Partial request cache (highest ROI)

Replace `LeafSearchCache` internals with foyer's `HybridCache`. This is the most self-contained change:

- **Key**: `CacheKey { split_id, normalized_request, merged_time_range }` (already exists)
- **Value**: `Vec<u8>` (Prost-encoded `LeafSearchResponse`, already serialized)
- **Memory tier**: Same capacity as today (configurable)
- **Disk tier**: Local NVMe, configurable size (e.g., 50-100GB)
- **Eviction**: S3-FIFO or LFU (foyer supports both)

Files to change:
- `quickwit-search/src/leaf_cache.rs` — Replace `MemorySizedCache<CacheKey>` with foyer `HybridCache`
- `quickwit-search/src/service.rs` — Initialize foyer cache in `SearcherContext`
- `quickwit-config/src/node_config/mod.rs` — Add disk cache config (path, size)
- `quickwit-storage/Cargo.toml` or `quickwit-search/Cargo.toml` — Add foyer dependency

### Phase 2: Fast field cache

Replace `QuickwitCache` (which wraps `MemorySizedCache`) with foyer for the `.fast` file slices. This gives us hybrid caching for the columnar data used in all aggregation queries.

Files to change:
- `quickwit-storage/src/cache/quickwit_cache.rs` — Swap to foyer backend
- `quickwit-storage/src/cache/memory_sized_cache.rs` — May need adapter trait

### Phase 3: Benchmark & tune

- Run Dailymotion-like workload on dogfooding cluster with foyer enabled
- Compare dashboard latency p50/p95/p99 before/after
- Compare monitor evaluation reliability (missed evaluations)
- Tune memory/disk ratios based on working set size

## Configuration

```yaml
searcher:
  partial_request_cache:
    capacity: 64mb           # memory tier (same as today)
    disk_capacity: 50gb      # NEW: NVMe tier
    disk_path: /data/cache   # NEW: local NVMe path
    policy: s3-fifo
  fast_field_cache:
    capacity: 1gb
    disk_capacity: 100gb
    disk_path: /data/cache
    policy: s3-fifo
```

## Risks

1. **Disk I/O contention** — NVMe is shared with the split file cache and indexing pipeline. Need to benchmark that cache reads don't compete with indexing writes.
2. **Cache coherence** — Splits are immutable once published, so there's no invalidation problem. But merged/deleted splits need cache eviction (already handled by split ID in the key).
3. **Cold start** — After a searcher restart, the disk cache is warm but memory cache is cold. Foyer handles this (disk tier persists across restarts).
4. **Dependency** — Adding foyer is a new dependency. It's well-maintained (RisingWave uses it in production) but still a bet.

## Prioritizing Fresh Data in the Cache

### The problem

Today all caches are **age-unaware**. They evict based on access recency (LRU) or frequency (S3-FIFO/TinyLFU), but have no concept of split data freshness. A split covering yesterday's logs and a split covering logs from 3 months ago are treated identically.

In practice, monitors and dashboards overwhelmingly query the **last 1-24 hours**. The data we most need to keep hot is the freshest data. But a single ad-hoc query scanning 30 days of data can flush the entire cache, evicting the hot 24h working set.

We already have the information to do better: every split carries `timestamp_start` and `timestamp_end` (seconds since epoch). This flows through `SplitIdAndFooterOffsets` and into the `CacheKey` as `merged_time_range`.

### Strategy 1: Age-weighted eviction priority

When deciding what to evict, weight items by **data freshness** (how recent the split's time range is), not just access recency.

```
eviction_priority = base_priority(access_time, frequency) × age_penalty(split_timestamp_end)
```

Where `age_penalty` increases as the split's data gets older:
- Split covering last 1h → age_penalty = 1.0 (never penalized)
- Split covering 1-24h ago → age_penalty = 1.0-2.0
- Split covering 1-7d ago → age_penalty = 2.0-5.0
- Split covering >7d ago → age_penalty = 10.0

**Implementation with foyer:** Foyer supports custom `Weighter` traits. We can make the "cost" of caching old data higher, so the cache naturally evicts old-split entries first when under pressure.

**Implementation without foyer (current caches):** The `quick_cache` crate (used in S3Fifo) already supports a `Weighter` trait. We could adjust `QuickCacheWeighter` to factor in split age. The LRU implementation would need a custom comparator.

### Strategy 2: Admission control — reject old data from the cache

Don't even admit entries for old splits into the memory tier. This is the simplest approach and prevents cache pollution from ad-hoc historical queries.

```rust
fn should_admit(split_timestamp_end: i64, now: i64) -> bool {
    let age_secs = now - split_timestamp_end;
    age_secs < ADMISSION_THRESHOLD_SECS // e.g., 24h = 86400
}
```

For queries on older data:
- **Without foyer:** Skip the memory cache entirely, go straight to S3.
- **With foyer:** Skip the memory tier, but still check/populate the disk tier. This way, repeated historical queries benefit from NVMe without polluting RAM.

**Implementation:** In `LeafSearchCache::put()` and the fast field cache `put()`, check the split's time range before inserting. The `merged_time_range` is already computed in `CacheKey::from_split_meta_and_request`.

### Strategy 3: Tiered freshness — memory for fresh, disk for warm, S3 for cold

With foyer's hybrid cache, we can create a natural three-tier system:

| Data age | Tier | Latency | Capacity |
|---|---|---|---|
| Last 1-4h | Memory | ~0.001ms | Small (1-4 GB) |
| Last 4-24h | NVMe disk | ~0.1ms | Medium (50-100 GB) |
| Older than 24h | S3 | ~50-200ms | Unlimited |

**How it works:**
- Fresh data enters memory tier naturally (high access frequency from monitors keeps it hot)
- When evicted from memory, foyer spills to NVMe (still fast for dashboards)
- Admission control prevents data older than 24h from entering either tier (goes to S3 every time)

This is the combination of Strategy 2 (admission control for the memory tier) + foyer's natural spilling (memory → disk) + a hard cutoff for very old data.

### Strategy 4: Proactive warmup for the latest splits

Instead of waiting for queries to populate the cache, **proactively load the fast fields and footers of the most recent splits** when they are published by the indexer.

The control plane already knows when new splits are published. We could add a "cache warmup" step:

1. Indexer publishes a new split → control plane notifies searchers
2. Searcher fetches and caches the split footer + fast fields immediately
3. By the time the first monitor query arrives, the data is already in cache

**Benefit:** Eliminates the "cold first query" problem where the first monitor evaluation after a new split is published is slow because nothing is cached yet.

**Implementation:** Add a `warmup_recent_splits()` background task in the searcher that periodically checks for newly published splits and pre-populates the footer and fast field caches.

### Recommended approach

**Start with Strategy 2 (admission control) + Strategy 3 (tiered with foyer).** This gives the biggest bang for the effort:

1. Admission control is a ~10-line change that immediately stops cache pollution from historical queries.
2. Foyer gives us the NVMe tier so the 24h working set survives memory pressure.
3. Strategy 4 (proactive warmup) can be added later as an optimization.

Strategy 1 (age-weighted eviction) is more complex and may not be needed if admission control is aggressive enough.

---

## Alternatives Considered

- **Increase memory cache sizes**: Doesn't scale — memory is expensive and finite. Dailymotion already has 56GB per searcher.
- **Use the existing split file cache more aggressively**: It caches raw split files, not parsed/computed data. Still needs CPU to decode on every access.
- **Redis/memcached sidecar**: Network hop adds latency, operational complexity. NVMe is faster and simpler.
- **Custom mmap-based cache**: More work, foyer already solves this well.
