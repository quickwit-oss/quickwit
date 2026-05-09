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

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

tokio::task_local! {
    static STORAGE_CACHE_METRICS: Arc<StorageCacheMetrics>;
}

/// Accumulates storage cache activity for the current observed operation.
#[derive(Debug, Default)]
pub struct StorageCacheMetrics {
    hit_bytes: AtomicUsize,
    miss_bytes: AtomicUsize,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
}

/// Point-in-time storage cache activity counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageCacheMetricsSnapshot {
    /// Number of bytes returned by the cache.
    pub hit_bytes: usize,
    /// Number of bytes fetched from the backing storage after cache misses.
    pub miss_bytes: usize,
    /// Number of cache reads that returned bytes.
    pub hit_count: usize,
    /// Number of cache reads that fell through to backing storage.
    pub miss_count: usize,
}

impl StorageCacheMetrics {
    /// Returns the current cache activity counters.
    pub fn snapshot(&self) -> StorageCacheMetricsSnapshot {
        StorageCacheMetricsSnapshot {
            hit_bytes: self.hit_bytes.load(Ordering::Relaxed),
            miss_bytes: self.miss_bytes.load(Ordering::Relaxed),
            hit_count: self.hit_count.load(Ordering::Relaxed),
            miss_count: self.miss_count.load(Ordering::Relaxed),
        }
    }

    fn record_hit(&self, num_bytes: usize) {
        self.hit_count.fetch_add(1, Ordering::Relaxed);
        self.hit_bytes.fetch_add(num_bytes, Ordering::Relaxed);
    }

    fn record_miss(&self, num_bytes: usize) {
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        self.miss_bytes.fetch_add(num_bytes, Ordering::Relaxed);
    }
}

impl StorageCacheMetricsSnapshot {
    /// Returns the non-negative delta between this snapshot and an earlier one.
    pub fn saturating_delta_since(self, before: Self) -> Self {
        Self {
            hit_bytes: self.hit_bytes.saturating_sub(before.hit_bytes),
            miss_bytes: self.miss_bytes.saturating_sub(before.miss_bytes),
            hit_count: self.hit_count.saturating_sub(before.hit_count),
            miss_count: self.miss_count.saturating_sub(before.miss_count),
        }
    }
}

/// Runs `future` with storage cache activity recorded into `metrics`.
pub async fn with_storage_cache_metrics<F, T>(metrics: Arc<StorageCacheMetrics>, future: F) -> T
where F: Future<Output = T> {
    STORAGE_CACHE_METRICS.scope(metrics, future).await
}

pub(crate) fn record_storage_cache_hit(num_bytes: usize) {
    let _ = STORAGE_CACHE_METRICS.try_with(|metrics| metrics.record_hit(num_bytes));
}

pub(crate) fn record_storage_cache_miss(num_bytes: usize) {
    let _ = STORAGE_CACHE_METRICS.try_with(|metrics| metrics.record_miss(num_bytes));
}
