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

use std::sync::Arc;

use bytes::Bytes;
use feuer_memory::{MemoryCache, MemoryMetrics};

use crate::OwnedBytes;
use crate::metrics::CacheMetrics;

/// Feuer's memory tier for immutable objects, without disk or a Tokio runtime.
/// Keys are caller-provided 128-bit digests. Collisions are not checked against original keys.
pub struct FeuerMemoryCache {
    cache: MemoryCache,
    metrics: &'static CacheMetrics,
}

impl FeuerMemoryCache {
    /// A zero capacity disables caching. Positive capacities use Feuer's environment settings
    /// and soft memory budget, including its oversized-entry admission policy.
    pub fn new(
        capacity: u64,
        cache_name: &'static str,
        metrics: &'static CacheMetrics,
    ) -> anyhow::Result<Self> {
        let registry: mixtrics::metrics::BoxedRegistry =
            Box::new(crate::split_range_cache::metrics::QuickwitMetricsRegistry { cache_name });
        let memory_metrics = MemoryMetrics::new(&registry);
        if capacity == 0 {
            return Ok(Self {
                cache: MemoryCache::with_metrics(0, memory_metrics, 0),
                metrics,
            });
        }
        // The tiered constructor is Linux-only. Build the same memory tier directly so
        // memory-only search remains available on every supported platform.
        let config = feuer::CacheConfig::new("", 0, capacity)?;
        let cache =
            MemoryCache::with_metrics(capacity, memory_metrics, config.idle_buffer_pool_percent())
                .with_access_histories(Arc::new(feuer::ObjectAccessHistories::with_half_life(
                    config.access_count_half_life(),
                )))
                .with_reclaim_sample_size(config.eviction().sample_size());
        Ok(Self { cache, metrics })
    }

    /// Record demand before lookup, including misses, as required by Feuer's retention scorer.
    pub fn get(&self, key: u128) -> Option<OwnedBytes> {
        if self.cache.capacity() == 0 {
            return None;
        }
        let key = feuer::ObjectKeyHash(key);
        self.cache.access_histories().record_access(&key);
        match self.cache.get(&key) {
            Some(bytes) => {
                self.metrics.cache_metrics.hits_num_items.inc();
                self.metrics
                    .cache_metrics
                    .hits_num_bytes
                    .inc_by(bytes.len() as u64);
                Some(crate::stable_deref_bytes::into_owned_bytes(bytes))
            }
            None => {
                self.metrics.cache_metrics.misses_num_items.inc();
                None
            }
        }
    }

    /// Admit an immutable object without recording another request. Existing values are retained.
    pub fn put(&self, key: u128, bytes: OwnedBytes) -> bool {
        if self.cache.capacity() == 0 {
            return false;
        }
        self.cache.insert(
            feuer::ObjectKeyHash(key),
            feuer::Download::new(Bytes::from_owner(bytes)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(capacity: u64) -> FeuerMemoryCache {
        FeuerMemoryCache::new(capacity, "predicate", &crate::metrics::PREDICATE_CACHE).unwrap()
    }

    #[test]
    fn test_disabled_cache_does_not_retain_entries_or_history() {
        let cache = cache(0);
        assert!(!cache.put(1, OwnedBytes::new(&b"result"[..])));
        assert!(cache.get(1).is_none());
        assert_eq!(cache.cache.access_histories().request_count(), 0);
        assert_eq!(cache.cache.used_bytes(), 0);
    }

    #[test]
    fn test_memory_cache_retains_immutable_results_and_records_demand() {
        let cache = cache(1_000_000);
        assert!(cache.get(1).is_none());
        assert!(cache.put(1, OwnedBytes::new(&b"result"[..])));
        assert!(!cache.put(1, OwnedBytes::new(&b"replacement"[..])));
        assert_eq!(cache.get(1).unwrap().as_slice(), b"result");
        assert!(cache.get(2).is_none());
        assert_eq!(cache.cache.access_histories().request_count(), 3);
        assert_eq!(cache.cache.used_bytes(), 6);
    }

    #[test]
    fn test_memory_cache_uses_feuer_oversized_entry_admission() {
        let cache = cache(1);
        assert!(cache.put(1, OwnedBytes::new(&b"result"[..])));
        assert_eq!(cache.get(1).unwrap().as_slice(), b"result");
        assert_eq!(cache.cache.used_bytes(), 6);
    }
}
