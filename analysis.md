# Analysis: A Download Manager in `quickwit-storage`

## Goal

Today, fetching split data from object storage is spread across two crates and a
deep stack of `async`-wrapping decorators ("async onions"). Caching is bolted on
by **wrapping the tantivy `Directory`**, and every other concern (debouncing,
retry/timeout, bundle-offset translation, the on-disk split cache, the
fast-field cache) is a separate `#[async_trait] impl Storage` decorator. A single
`get_slice` therefore traverses 5–9 boxed futures even when it is a pure cache
hit that performs no I/O.

This document analyzes replacing that with a **`DownloadManager` in
`quickwit-storage`**: a *synchronous core* (cache lookups, bundle-offset math,
range merging, request dedup keying, bookkeeping) plus **one thin, genuinely
async layer** that performs the network/disk download. It is the result of three
agreed design decisions:

| Decision | Choice |
|----------|--------|
| **Scope** | Search read path **+** the on-disk split-cache downloads (`get_slice`/`get_all`/`copy_to_file`). Indexing/upload path is out of scope for now. |
| **Approach** | **Collapse the onion** into a single manager: replace the decorator chain with a sync core + one async download fn. |
| **Caching** | **Unify** all read caching in the storage download manager, keyed by physical split path + absolute range. The directory wrappers become thin views. |

---

## 1. Current architecture

### 1.1 The decorator stack (storage crate)

`quickwit-storage` composes behavior with the `Storage` trait
(`quickwit-storage/src/storage.rs`), every method `async`. Each decorator wraps
`Arc<dyn Storage>` and is itself `#[async_trait]`, so each call site allocates a
`Pin<Box<dyn Future>>`:

| Layer | File | What it adds | Sync or async work? |
|-------|------|--------------|---------------------|
| `TimeoutAndRetryStorage` | `src/timeout_and_retry_storage.rs` | aggressive per-attempt timeout + retry on `get_slice` | sync (loop/policy) + async (the awaited attempt) |
| `StorageWithCache` (split cache) | `src/cache/storage_with_cache.rs` + `src/split_cache/mod.rs` | on-disk full-split LRU cache (`SplitCacheBackingStorage`) | sync lookup wrapped in `async fn get`; async only on disk read |
| `StorageWithCache` (fast fields) | `src/cache/storage_with_cache.rs` | in-memory `MemorySizedCache` for `.fast` data via `QuickwitCache` | **fully sync lookup**, needlessly `.await`ed |
| `DebouncedStorage` | `src/debouncer.rs` | coalesce concurrent identical `(path, range)` requests | sync hashing/lookup; async only awaiting the shared future |
| `S3CompatibleObjectStorage` | `src/object_storage/s3_compatible_storage.rs` | the real HTTP GET (`REQUEST_SEMAPHORE`, range header, retry, `download_all`) | **genuinely async** |

Assembly order (search node), bottom → top:
```
S3CompatibleObjectStorage                          (s3_compatible_storage_resolver.rs:64)
  └─ DebouncedStorage
       └─ SplitCache.wrap_storage → StorageWithCache(SplitCacheBackingStorage)   (split_cache/mod.rs:129)
            └─ (per split) StorageWithCache(fast_fields QuickwitCache)           (leaf.rs:233)
                 └─ BundleStorage                                                (leaf.rs:226)
                      └─ TimeoutAndRetryStorage (optional)                       (leaf.rs:200)
```

### 1.2 The directory stack (directories crate)

On top of `Storage`, search builds a tantivy `Directory` tower in
`open_index_with_caches` (`quickwit-search/src/leaf.rs:216`):
```
S3 …storage… → BundleStorage
  └─ StorageDirectory          (storage_directory.rs — sync read_bytes() PANICS, async only)
       └─ CachingDirectory      (caching_directory.rs — ByteRangeCache, "ephemeral unbounded")
            └─ HotDirectory      (hot_directory.rs — static slice cache from the split footer)
```
- `BundleStorage` (`src/bundle_storage.rs`) translates a logical file path inside
  the `.split` to an absolute byte range in the bundle — **pure arithmetic +
  metadata-map lookup**, but still an `async fn get_slice`.
- `CachingDirectory` holds the per-split `ByteRangeCache`
  (`caching_directory.rs:31`). Its `get_slice` lookup is **synchronous** —
  `ByteRangeCache::get_slice` returns `Option<OwnedBytes>` with no `.await`.
- `HotDirectory` serves a static, pre-computed slice cache (the hotcache built at
  index time). Lookups are **synchronous**.

### 1.3 The core problem: the async download path is an N-layer onion

The waste we are removing is on the **download path** (`get_slice`/`get_all`),
which `warmup` drives at high volume. A single fetch threads through every
decorator in §1.1/§1.2, and **each layer is `#[async_trait]`**, so each one
allocates a `Box::pin`'d future and adds an `.await` — *regardless of whether that
layer does any I/O*:

```
TimeoutAndRetry → Bundle → StorageWithCache(fast) → StorageWithCache(splitcache) → Debounced → S3
```

Of those six layers, exactly one (S3, plus the split-cache's disk read) actually
performs I/O. The other five do **synchronous work** — cache lookups, bundle
offset math, dedup keying — yet each still pays the full async cost, because an
`async fn` whose body is synchronous is **not** free:

1. **A future (state machine) is constructed** for the call.
2. With `#[async_trait]`, that future is **`Box::pin`'d → a heap allocation per
   call** (`async-trait` desugars to `Pin<Box<dyn Future + Send>>`).
3. The caller **`.await`s it → a poll**, and the `.await` is an optimization
   barrier (no inlining across it).

A pure-sync body only avoids the task suspend/wake (it returns `Poll::Ready` on
the first poll, no scheduler round-trip). The **boxed allocation + poll + await
barrier remain — per layer, per call**. Multiplied by layer count × warmup call
volume, that is the cost that "adds up." We are paying async machinery for
**synchronous decisions**; the network wait itself is unavoidable and not the
target.

On top of that, caching is split across **five** places (split-footer cache,
fast-field `QuickwitCache`, on-disk `SplitCache`, ephemeral `ByteRangeCache`,
static `HotDirectory` cache), each keyed differently (`split_id` string, logical
path, physical path, range), at two different abstraction levels — so there is no
single place that owns "fetch a split byte range."

### 1.4 Aside: the sync read path falls out for free

A secondary (not load-bearing) observation: tantivy's `FileHandle` already exposes
both `read_bytes` (sync) and `read_bytes_async`. Warmup populates the caches via
the async path; the subsequent collection/scoring phase reads via **sync**
`read_bytes`, which today must hit `CachingDirectory`'s `ByteRangeCache` because
`StorageDirectory::read_bytes` panics by design. Since the proposed `resolve()` is
already a plain synchronous `fn`, that sync handle can call it directly — a nice
side benefit, but the rationale for this refactor stands entirely on §1.3.

---

## 2. Proposed design

### 2.1 Principle

> **Decide synchronously, download asynchronously.**

Split the work into:

- **Sync core** — everything that is CPU/memory-only: cache lookups across all
  tiers, bundle logical→physical offset translation, range coalescing, dedup
  keying, metrics, eviction bookkeeping. No futures, no allocation of boxed
  async state. Returns a *resolution*.
- **Thin async layer** — exactly one future type that performs the actual
  download (object-store HTTP GET, or on-disk split-cache file read), bounded by
  the existing semaphore, with retry/timeout, and that writes results back into
  the caches synchronously on completion.

### 2.2 Shape

```rust
// quickwit-storage/src/download_manager/mod.rs
pub struct DownloadManager {
    caches: CacheStack,          // footer + fast-field + byte-range, unified, sync API
    split_cache: Option<Arc<SplitCache>>, // on-disk full-split cache (sync lookup, async fill)
    inflight: AsyncDebouncer<DownloadKey, StorageResult<OwnedBytes>>, // dedup
    retry: RetryParams,
    timeout_policy: Option<StorageTimeoutPolicy>,
    backend: Arc<dyn Storage>, // the RAW object-store backend, no decorators (see 2.4)
}

/// Outcome of the synchronous resolution step. No I/O has happened.
pub enum Resolution {
    /// Served entirely from a cache tier — zero futures, return immediately.
    Hit(OwnedBytes),
    /// Needs a download; carries the physical key + range to fetch.
    Miss(DownloadPlan),
}

impl DownloadManager {
    /// 100% synchronous. Safe to call from tantivy's sync `read_bytes`.
    pub fn resolve(&self, split: &SplitId, range: LogicalRange) -> Resolution { ... }

    /// The thin async layer. Only entered on a Miss. Debounced + retried +
    /// semaphore-bounded; writes the result into the caches before returning.
    pub async fn download(&self, plan: DownloadPlan) -> StorageResult<OwnedBytes> { ... }

    /// Convenience used by warmup: resolve, and await only if it's a miss.
    pub async fn get(&self, split: &SplitId, range: LogicalRange) -> StorageResult<OwnedBytes> {
        match self.resolve(split, range) {
            Resolution::Hit(bytes) => Ok(bytes),
            Resolution::Miss(plan) => self.download(plan).await,
        }
    }
}
```

`resolve()` walks the cache tiers in order (byte-range / fast-field → on-disk
split cache index → footer), applies the bundle-offset translation, and merges
the request against already-cached touching ranges — all synchronously. Only a
genuine miss produces a `DownloadPlan`, and only then is a single future created.

### 2.3 Unified cache stack

Replace the five scattered caches and the two caching directories with one
`CacheStack` owned by the manager, keyed uniformly by **physical split path +
absolute byte range**:

- byte-range cache (replaces `CachingDirectory`'s `ByteRangeCache` + the
  fast-field `MemorySizedCache`/`QuickwitCache`),
- static hotcache slices (replaces `HotDirectory`'s static cache — loaded once
  per split, still keyed by physical range),
- split-footer cache,
- on-disk `SplitCache` (kept; its lookup becomes a sync index check, its fill the
  async path).

The tantivy directories (`StorageDirectory`/`CachingDirectory`/`HotDirectory`)
collapse into **one thin `DownloadManagerDirectory`** whose `FileHandle`:
- `read_bytes` (sync) → `manager.resolve(...)`, returning `Hit` bytes or erroring
  on a miss (preserving today's "everything must be warmed" invariant);
- `read_bytes_async` → `manager.get(...)` (resolve + thin download).

### 2.4 We keep the `Storage` trait

The `Storage` trait stays as-is. It is the backend abstraction used across the
whole codebase (indexing, upload, metastore, CLI, and every backend: S3, Azure,
GCS, local, RAM), and it already exposes exactly the genuinely-async I/O the
manager needs: `get_slice`, `get_all`, `copy_to_file`. There is **no new download
trait** — that would be a redundant abstraction over what `Storage` already does.

What the manager holds is the **raw backend** `Arc<dyn Storage>` — i.e.
`S3CompatibleObjectStorage` (or the local/Azure/GCS impl) with **none of the
read-path decorators** wrapped around it. On a `Miss`, `download()` calls the
backend's existing `storage.get_slice(...)` / `get_all(...)` / `copy_to_file(...)`.

So the refactor removes the *decorator stack on top of `Storage`*
(`DebouncedStorage`, `StorageWithCache`, `TimeoutAndRetryStorage`) and folds their
behavior into the manager's sync core + thin `download()` — it does **not** touch
the `Storage` trait itself, nor the write path (`put`/`delete`/indexing), which is
unaffected by this read-only change.

### 2.5 Where debounce/retry/timeout go

These collapse from standalone decorators into the manager's `download()`:
- **debounce**: keyed in `download()` on the physical `DownloadKey`, so dedup
  happens once, at the only place a download is issued (today's `DebouncedStorage`
  logic in `debouncer.rs` is reused as-is — it is already a sync map + shared
  future).
- **retry + timeout**: a loop inside `download()` around the single
  `Storage::get_slice` await on the raw backend (reuses `TimeoutAndRetryStorage`'s
  policy iteration and `aws_retry`).

---

## 3. Before / after

The headline is the **download path** (`warmup`), where the onion multiplies
futures across layers that mostly do synchronous work.

**Before — one `get_slice` on a cache miss, a boxed future per layer:**
```
TimeoutAndRetry.get_slice().await        ← Box::pin
  Bundle.get_slice().await               ← Box::pin (offset math — sync work)
    StorageWithCache(fast).get_slice()   ← Box::pin (sync lookup, awaited)
      StorageWithCache(splitcache)       ← Box::pin (sync keying, awaited)
        Debounced.get_slice().await      ← Box::pin (sync keying, awaited)
          S3.get_slice().await           ← Box::pin (real HTTP)   ← only this does I/O
```
6 boxed futures, 5 of them for synchronous work — *even though the call hits the
network exactly once*.

**After — `resolve()` does all the sync work future-free; one future does the I/O:**
```
manager.get(split, range)
  → resolve(...)            ← sync: caches + offset math + dedup keying, 0 futures
  → download(plan).await    ← 1 future: debounce + retry + raw-backend get_slice
```
**Miss: 6 boxed futures → 1.** And a fetch whose range is already cached (in the
long-term fast-field cache, footer cache, or on-disk split cache) resolves as a
`Hit` with **0 futures — even though warmup calls it from an async context**,
because `resolve()` is synchronous and `get()` returns `Ready` without building the
download future.

*(Aside, per §1.4: the post-warmup sync collection path calls `resolve()` directly
from `read_bytes`, so it too is future-free — but the win above stands without
relying on that.)*

---

## 4. Migration plan (incremental, each step compiles & ships)

1. **Introduce `ObjectDownload`** and implement it for the existing backends as a
   thin pass-through next to today's `Storage` impl. No behavior change.
2. **Build `CacheStack`** — move `ByteRangeCache`, fast-field cache, footer cache
   behind one sync-lookup API keyed by physical path+range. Port the static
   hotcache slices into it. Cover with the existing cache unit tests.
3. **Build `DownloadManager`** with `resolve()` + `download()`, folding in
   `DebouncedStorage` and `TimeoutAndRetryStorage` logic and the on-disk
   `SplitCache` (sync index lookup, async fill).
4. **Add `DownloadManagerDirectory`** (sync `read_bytes` via `resolve`, async via
   `get`); switch `open_index_with_caches` to build it instead of the
   `Storage→StorageDirectory→Caching→Hot` tower.
5. **Repoint `warmup`** to call `manager.get(...)`; verify the
   warmup-then-sync-read invariant still holds (search reads are all hits).
6. **Delete** `StorageWithCache`, `CachingDirectory`, `HotDirectory`,
   `StorageDirectory`, the per-layer `Storage` decorators for reads, and the
   resolver wiring that stacked them.
7. **Validate** through the production path: ingest via OTLP, query via REST,
   confirm hit-rate metrics and end-to-end latency. Run the existing
   `quickwit-search` + `quickwit-storage` test suites and the integration tests.

Steps 1–4 are additive and can land behind the old path; step 6 is the deletion
once parity is confirmed.

---

## 5. Risks & things to watch

- **Tantivy fork coupling.** The sync-read-after-warmup contract depends on the
  custom tantivy fork's `FileHandle`. `resolve()` returning a miss inside sync
  `read_bytes` must error exactly as `StorageDirectory::read_bytes` panics today,
  so an un-warmed read is still a loud bug, not a silent sync stall.
- **Debounce + delete race.** The existing race noted in `debouncer.rs` (get/
  delete/get returning stale cached bytes) must be preserved-or-fixed
  deliberately, not lost in the move.
- **Cache key unification.** Today keys differ (footer by `split_id` string,
  fast-field by logical `.fast` suffix routing, byte-range by physical path).
  Collapsing to physical-path+range must keep the `.fast` long-term vs. ephemeral
  short-lived distinction (different sizes/lifetimes/metrics) — the `CacheStack`
  needs per-tier capacity & metrics, not one flat map.
- **`get_slice_stream`** currently bypasses the debouncer and cache. Decide
  whether the manager exposes a streaming download or it stays on the raw
  backend.
- **Metrics continuity.** Preserve the existing per-cache `CacheMetrics`
  (`fast_field_cache`, `split_footer_cache`, `searcher_split_cache`,
  `shortlived_cache`, …) so dashboards keep working.

---

## 6. Expected payoff

- Collapse the download onion: **6 boxed futures per `get_slice` → 1 on a miss**,
  and **0 on an already-cached range** — on the async warmup path that drives the
  bulk of the calls.
- One place that owns "fetch a split byte range," replacing two crates' worth of
  layering — easier to reason about, profile, and tune.
- Caching keyed consistently on physical bytes, no longer entangled with the
  tantivy `Directory` abstraction.

---

## 7. Open questions (non-blocking, for review)

1. **Streaming reads** — should `DownloadManager` own a streaming download
   (`get_slice_stream` equivalent), or is range+full-file enough for search +
   split-cache fill? (Today streaming bypasses cache/debounce.)
2. **Crate boundary** — the unified directory (`DownloadManagerDirectory`) needs
   tantivy types, so it likely lives in `quickwit-directories` while the manager
   lives in `quickwit-storage`. Confirm that split, or whether the thin directory
   should move into `quickwit-storage` to keep the manager self-contained.
3. **`copy_to_file` for split-cache** — the background full-split download task
   should route through `DownloadManager::download()` (so dedup/retry/semaphore are
   shared), calling the backend's existing `Storage::copy_to_file`. Confirm we want
   that rather than letting the task call the raw backend directly.
