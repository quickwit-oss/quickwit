// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytesize::ByteSize;
use lru::LruCache;
use mini_moka::sync::Cache as MokaCache;
use quick_cache::unsync::Cache as QuickCache;
use quickwit_config::CachePolicy;
use tokio::time::Instant;
use tracing::{error, warn};

use crate::OwnedBytes;
use crate::cache::stored_item::{StoredItem, ValueLen};
use crate::metrics::SingleCacheMetrics;

/// We do not evict anything that has been accessed in the last 60s.
///
/// The goal is to behave better on scan access patterns, without being as aggressive as
/// using a MRU strategy.
///
/// TLDR is:
///
/// If two items have been access in the last 60s it is not really worth considering the
/// latter too be more recent than the previous and do an eviction.
/// The difference is not significant enough to raise the probability of its future access.
///
/// On the other hand, for very large queries involving enough data to saturate the cache,
/// we are facing a scanning pattern. If variations of this  query is repeated over and over
/// a regular LRU eviction policy would yield a hit rate of 0.
pub(crate) const LRU_MIN_TIME_SINCE_LAST_ACCESS: Duration = Duration::from_secs(60);

// A fake entry inside a cache, which the cache believe to be of the given size
#[derive(Clone)]
pub(crate) struct FakeCacheEntry(pub usize);

impl ValueLen for FakeCacheEntry {
    fn len(&self) -> usize {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Capacity {
    Unlimited,
    InBytes(usize),
}

impl Capacity {
    fn exceeds_capacity(&self, num_bytes: usize) -> bool {
        match *self {
            Capacity::Unlimited => false,
            Capacity::InBytes(capacity_in_bytes) => num_bytes > capacity_in_bytes,
        }
    }
}

pub(crate) enum AnyCache<K: Hash + Eq, V: ValueLen = OwnedBytes> {
    Lru(Lru<K, V>),
    S3Fifo(S3Fifo<K, V>),
    TinyLfu(TinyLfu<K, V>),
}

impl<K: Hash + Eq + Send + Sync + 'static, V: ValueLen + Clone + Send + Sync + 'static>
    AnyCache<K, V>
{
    pub fn from_policy_and_capacity(
        policy: CachePolicy,
        capacity: ByteSize,
        cache_metrics: SingleCacheMetrics,
    ) -> Self {
        match policy {
            CachePolicy::Lru => AnyCache::Lru(Lru::with_capacity(
                Capacity::InBytes(capacity.as_u64().try_into().unwrap_or(usize::MAX)),
                cache_metrics,
            )),
            CachePolicy::S3Fifo => {
                AnyCache::S3Fifo(S3Fifo::with_capacity(capacity.as_u64(), cache_metrics))
            }
            CachePolicy::TinyLfu => {
                AnyCache::TinyLfu(TinyLfu::with_capacity(capacity.as_u64(), cache_metrics))
            }
        }
    }
    pub fn unbounded(cache_metrics: SingleCacheMetrics) -> Self {
        AnyCache::Lru(Lru::with_capacity(Capacity::Unlimited, cache_metrics))
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Arc<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            AnyCache::Lru(lru) => lru.get(cache_key),
            AnyCache::S3Fifo(s3fifo) => s3fifo.get(cache_key),
            AnyCache::TinyLfu(tiny_lfu) => tiny_lfu.get(cache_key),
        }
    }
    pub fn put(&mut self, key: K, value: V) {
        match self {
            AnyCache::Lru(lru) => lru.put(key, value),
            AnyCache::S3Fifo(s3fifo) => s3fifo.put(key, value),
            AnyCache::TinyLfu(tiny_lfu) => tiny_lfu.put(key, value),
        }
    }
}

pub struct Lru<K: Hash + Eq, V> {
    lru_cache: LruCache<K, StoredItem<V>>,
    num_items: usize,
    num_bytes: u64,
    capacity: Capacity,
    cache_metrics: SingleCacheMetrics,
}

impl<K: Hash + Eq, V> Drop for Lru<K, V> {
    fn drop(&mut self) {
        // we don't count this toward evicted entries, as we are clearing the whole cache
        self.cache_metrics.in_cache_count.sub(self.num_items as i64);
        self.cache_metrics
            .in_cache_num_bytes
            .sub(self.num_bytes as i64);
    }
}

impl<K: Hash + Eq, V: ValueLen + Clone> Lru<K, V> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    fn with_capacity(capacity: Capacity, cache_metrics: SingleCacheMetrics) -> Self {
        Lru {
            // The limit will be decided by the amount of memory in the cache,
            // not the number of items in the cache.
            // Enforcing this limit is done in the `NeedMutCache` impl.
            lru_cache: LruCache::unbounded(),
            num_items: 0,
            num_bytes: 0,
            capacity,
            cache_metrics,
        }
    }

    fn record_item(&mut self, num_bytes: u64) {
        self.num_items += 1;
        self.num_bytes += num_bytes;
        self.cache_metrics.in_cache_count.inc();
        self.cache_metrics.in_cache_num_bytes.add(num_bytes as i64);
    }

    fn drop_item(&mut self, num_bytes: u64) {
        self.num_items -= 1;
        self.num_bytes -= num_bytes;
        self.cache_metrics.in_cache_count.dec();
        self.cache_metrics.in_cache_num_bytes.sub(num_bytes as i64);
        self.cache_metrics.evict_num_items.inc();
        self.cache_metrics.evict_num_bytes.inc_by(num_bytes);
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let item_opt = self.lru_cache.get_mut(cache_key);
        if let Some(item) = item_opt {
            self.cache_metrics.hits_num_items.inc();
            self.cache_metrics.hits_num_bytes.inc_by(item.len() as u64);
            Some(item.payload())
        } else {
            self.cache_metrics.misses_num_items.inc();
            None
        }
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    fn put(&mut self, key: K, bytes: V) {
        if self.capacity.exceeds_capacity(bytes.len()) {
            // The value does not fit in the cache. We simply don't store it.
            if self.capacity != Capacity::InBytes(0) {
                warn!(
                    capacity_in_bytes = ?self.capacity,
                    len = bytes.len(),
                    "Downloaded a byte slice larger than the cache capacity."
                );
            }
            return;
        }
        if let Some(previous_data) = self.lru_cache.pop(&key) {
            self.drop_item(previous_data.len() as u64);
        }

        let now = Instant::now();
        while self
            .capacity
            .exceeds_capacity(self.num_bytes as usize + bytes.len())
        {
            if let Some((_, candidate_for_eviction)) = self.lru_cache.peek_lru() {
                let time_since_last_access =
                    now.duration_since(candidate_for_eviction.last_access_time());
                if time_since_last_access < LRU_MIN_TIME_SINCE_LAST_ACCESS {
                    // It is not worth doing an eviction.
                    // TODO: It is sub-optimal that we might have needlessly evicted items in this
                    // loop before just returning.
                    return;
                }
            }
            if let Some((_, bytes)) = self.lru_cache.pop_lru() {
                self.drop_item(bytes.len() as u64);
            } else {
                error!(
                    "Logical error. Even after removing all of the items in the cache the \
                     capacity is insufficient. This case is guarded against and should never \
                     happen."
                );
                return;
            }
        }
        self.record_item(bytes.len() as u64);
        self.lru_cache.put(key, StoredItem::new(bytes, now));
    }
}

// actually, quick_cache is a Clock-PRO, not a S3-fifo contrary to what quick-cache and Moka's
// readme says. While both are clearly distinct (one being clock-based, the other being fifo
// based), they are not too disimilar in term of strenght/weaknesses.
pub struct S3Fifo<K: Hash + Eq, V: ValueLen> {
    cache:
        QuickCache<K, V, QuickCacheWeighter, quick_cache::DefaultHashBuilder, QuickCacheLifecycle>,
    capacity: u64,
    cache_metrics: SingleCacheMetrics,
}

impl<K: Hash + Eq, V: ValueLen> Drop for S3Fifo<K, V> {
    fn drop(&mut self) {
        // we don't count this toward evicted entries, as we are clearing the whole cache
        self.cache_metrics
            .in_cache_count
            .sub(self.cache.len() as i64);
        self.cache_metrics
            .in_cache_num_bytes
            .sub(self.cache.weight() as i64);
    }
}

struct QuickCacheWeighter;
impl<K, V: ValueLen> quick_cache::Weighter<K, V> for QuickCacheWeighter {
    fn weight(&self, _key: &K, value: &V) -> u64 {
        value.len() as u64
    }
}

struct QuickCacheLifecycle;
#[derive(Default)]
struct QuickCacheQueryEffect {
    count: u64,
    bytes: u64,
}
impl<K, V: ValueLen> quick_cache::Lifecycle<K, V> for QuickCacheLifecycle {
    type RequestState = QuickCacheQueryEffect;

    fn begin_request(&self) -> Self::RequestState {
        QuickCacheQueryEffect::default()
    }
    fn on_evict(&self, state: &mut Self::RequestState, _key: K, val: V) {
        state.count += 1;
        state.bytes += val.len() as u64;
    }
}

impl<K: Hash + Eq, V: ValueLen + Clone> S3Fifo<K, V> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    fn with_capacity(capacity: u64, cache_metrics: SingleCacheMetrics) -> Self {
        S3Fifo {
            cache: QuickCache::with(
                (capacity / (128 * 1024)) as usize,
                capacity,
                QuickCacheWeighter,
                quick_cache::DefaultHashBuilder::new(),
                QuickCacheLifecycle,
            ),
            capacity,
            cache_metrics,
        }
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let item_opt = self.cache.get(cache_key);
        if let Some(item) = item_opt {
            self.cache_metrics.hits_num_items.inc();
            self.cache_metrics.hits_num_bytes.inc_by(item.len() as u64);
            Some(item.clone())
        } else {
            self.cache_metrics.misses_num_items.inc();
            None
        }
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    fn put(&mut self, key: K, value: V) {
        if self.capacity < value.len() as u64 {
            // The value does not fit in the cache. We simply don't store it.
            if self.capacity != 0 {
                warn!(
                    capacity_in_bytes = ?self.capacity,
                    len = value.len(),
                    "Downloaded a byte slice larger than the cache capacity."
                );
            }
            return;
        }

        self.cache_metrics.in_cache_count.inc();
        self.cache_metrics
            .in_cache_num_bytes
            .add(value.len() as i64);
        let evicted = self.cache.insert_with_lifecycle(key, value);
        self.cache_metrics.in_cache_count.sub(evicted.count as i64);
        self.cache_metrics
            .in_cache_num_bytes
            .sub(evicted.bytes as i64);
        self.cache_metrics.evict_num_items.inc_by(evicted.count);
        self.cache_metrics.evict_num_bytes.inc_by(evicted.bytes);
    }
}

// We don't make this value Clone to ensure each item is dropped only once
struct CapacityTracker<V: ValueLen> {
    item: V,
    cache_metrics: Weak<SingleCacheMetrics>,
}

impl<V: ValueLen> Drop for CapacityTracker<V> {
    fn drop(&mut self) {
        if let Some(cache_metrics) = self.cache_metrics.upgrade() {
            cache_metrics.in_cache_count.dec();
            cache_metrics.in_cache_num_bytes.sub(self.item.len() as i64);
            cache_metrics.evict_num_items.inc();
            cache_metrics.evict_num_bytes.inc_by(self.item.len() as u64);
        }
    }
}

pub struct TinyLfu<K: Hash + Eq, V: ValueLen> {
    // this field is put first so it's dropped before the cache
    // we use that to not count removed entries as "evicted", by
    // calling CapacityTracker's Drop only after its Weak has expired.
    cache_metrics: Arc<SingleCacheMetrics>,

    // we store an Arc because moka does a lot of internal cloning, and it's hard to not double
    // evict/forget to count eviction otherwise
    cache: MokaCache<K, Arc<CapacityTracker<V>>>,
    capacity: u64,
}

impl<K: Hash + Eq, V: ValueLen> Drop for TinyLfu<K, V> {
    fn drop(&mut self) {
        // we don't count this toward evicted entries, as we are clearing the whole cache
        self.cache_metrics
            .in_cache_count
            .sub(self.cache.entry_count() as i64);
        self.cache_metrics
            .in_cache_num_bytes
            .sub(self.cache.weighted_size() as i64);
    }
}

impl<K: Hash + Eq + Send + Sync + 'static, V: ValueLen + Clone + Send + Sync + 'static>
    TinyLfu<K, V>
{
    /// Creates a new NeedMutSliceCache with the given capacity.
    fn with_capacity(capacity: u64, cache_metrics: SingleCacheMetrics) -> Self {
        TinyLfu {
            cache: MokaCache::builder()
                .max_capacity(capacity)
                .weigher(|_k, v: &Arc<CapacityTracker<V>>| {
                    v.item.len().try_into().unwrap_or(u32::MAX)
                })
                .build(),
            capacity,
            cache_metrics: cache_metrics.into(),
        }
    }

    pub fn get<Q>(&mut self, cache_key: &Q) -> Option<V>
    where
        Arc<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let item_opt = self.cache.get(cache_key);
        if let Some(item) = item_opt {
            self.cache_metrics.hits_num_items.inc();
            self.cache_metrics
                .hits_num_bytes
                .inc_by(item.item.len() as u64);
            Some(item.item.clone())
        } else {
            self.cache_metrics.misses_num_items.inc();
            None
        }
    }

    /// Attempt to put the given amount of data in the cache.
    /// This may fail silently if the owned_bytes slice is larger than the cache
    /// capacity.
    fn put(&mut self, key: K, value: V) {
        if self.capacity < value.len() as u64 {
            // The value does not fit in the cache. We simply don't store it.
            if self.capacity != 0 {
                warn!(
                    capacity_in_bytes = ?self.capacity,
                    len = value.len(),
                    "Downloaded a byte slice larger than the cache capacity."
                );
            }
            return;
        }

        self.cache_metrics.in_cache_count.inc();
        self.cache_metrics
            .in_cache_num_bytes
            .add(value.len() as i64);
        self.cache.insert(
            key,
            CapacityTracker {
                item: value,
                cache_metrics: Arc::downgrade(&self.cache_metrics),
            }
            .into(),
        );
    }
}
