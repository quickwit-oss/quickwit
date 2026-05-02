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

// See https://prometheus.io/docs/practices/naming/

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use quickwit_config::CacheConfig;
use quickwit_metrics::{Counter, Gauge, GaugeGuard, Histogram, counter, gauge, histogram};

/// Counters associated to storage operations.
pub struct StorageMetrics {
    pub shortlived_cache: CacheMetrics,
    pub partial_request_cache: CacheMetrics,
    pub predicate_cache: CacheMetrics,
    pub fd_cache_metrics: CacheMetrics,
    pub fast_field_cache: CacheMetrics,
    pub split_footer_cache: CacheMetrics,
    pub searcher_split_cache: CacheMetrics,
    pub get_slice_timeout_successes: [Counter; 3],
    pub get_slice_timeout_all_timeouts: Counter,
    pub object_storage_get_total: Counter,
    pub object_storage_get_errors_total: Counter,
    pub object_storage_get_slice_in_flight_count: Gauge,
    pub object_storage_get_slice_in_flight_num_bytes: Gauge,
    pub object_storage_put_total: Counter,
    pub object_storage_put_parts: Counter,
    pub object_storage_download_num_bytes: Counter,
    pub object_storage_upload_num_bytes: Counter,

    pub object_storage_delete_requests_total: Counter,
    pub object_storage_bulk_delete_requests_total: Counter,
    pub object_storage_delete_request_duration: Histogram,
    pub object_storage_bulk_delete_request_duration: Histogram,
}

static GET_SLICE_TIMEOUT_OUTCOME_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "get_slice_timeout_outcome",
        description: "Outcome of get_slice operations. success_after_1_timeout means the operation succeeded after a retry caused by a timeout.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_requests_total",
        description: "Total number of object storage requests performed.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_REQUEST_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    histogram!(
        name: "object_storage_request_duration_seconds",
        description: "Duration of object storage requests in seconds.",
        subsystem: "storage",
        buckets: vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
    )
});

static OBJECT_STORAGE_GET_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_gets_total",
        description: "Number of objects fetched. Might be lower than get_slice_timeout_outcome if queries are debounced.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_GET_ERRORS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_get_errors_total",
        description: "Number of GetObject errors.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "object_storage_get_slice_in_flight_count",
        description: "Number of GetObject for which the memory was allocated but the download is still in progress.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_NUM_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "object_storage_get_slice_in_flight_num_bytes",
        description: "Memory allocated for GetObject requests that are still in progress.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_PUT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_puts_total",
        description: "Number of objects uploaded. May differ from object_storage_requests_parts due to multipart upload.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_PUT_PARTS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_puts_parts",
        description: "Number of object parts uploaded.",
        subsystem: "",
    )
});

static OBJECT_STORAGE_DOWNLOAD_NUM_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_download_num_bytes",
        description: "Amount of data downloaded from an object storage.",
        subsystem: "storage",
    )
});

static OBJECT_STORAGE_UPLOAD_NUM_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_upload_num_bytes",
        description: "Amount of data uploaded to an object storage.",
        subsystem: "storage",
    )
});

impl Default for StorageMetrics {
    fn default() -> Self {
        let get_slice_timeout_successes = [
            counter!(
                parent: &*GET_SLICE_TIMEOUT_OUTCOME_TOTAL,
                "outcome" => "success_after_0_timeout",
            ),
            counter!(
                parent: &*GET_SLICE_TIMEOUT_OUTCOME_TOTAL,
                "outcome" => "success_after_1_timeout",
            ),
            counter!(
                parent: &*GET_SLICE_TIMEOUT_OUTCOME_TOTAL,
                "outcome" => "success_after_2+_timeout",
            ),
        ];
        let get_slice_timeout_all_timeouts = counter!(
            parent: &*GET_SLICE_TIMEOUT_OUTCOME_TOTAL,
            "outcome" => "all_timeouts",
        );

        let object_storage_delete_requests_total = counter!(
            parent: &*OBJECT_STORAGE_REQUESTS_TOTAL,
            "action" => "delete_object",
        );
        let object_storage_bulk_delete_requests_total = counter!(
            parent: &*OBJECT_STORAGE_REQUESTS_TOTAL,
            "action" => "delete_objects",
        );

        let object_storage_delete_request_duration = histogram!(
            parent: &*OBJECT_STORAGE_REQUEST_DURATION,
            "action" => "delete_object",
        );
        let object_storage_bulk_delete_request_duration = histogram!(
            parent: &*OBJECT_STORAGE_REQUEST_DURATION,
            "action" => "delete_objects",
        );

        StorageMetrics {
            fast_field_cache: CacheMetrics::for_component("fastfields"),
            fd_cache_metrics: CacheMetrics::for_component("fd"),
            partial_request_cache: CacheMetrics::for_component("partial_request"),
            predicate_cache: CacheMetrics::for_component("predicate"),
            searcher_split_cache: CacheMetrics::for_component("searcher_split"),
            shortlived_cache: CacheMetrics::for_component("shortlived"),
            split_footer_cache: CacheMetrics::for_component("splitfooter"),
            get_slice_timeout_successes,
            get_slice_timeout_all_timeouts,
            object_storage_get_total: OBJECT_STORAGE_GET_TOTAL.clone(),
            object_storage_get_errors_total: OBJECT_STORAGE_GET_ERRORS_TOTAL.clone(),
            object_storage_get_slice_in_flight_count: OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_COUNT
                .clone(),
            object_storage_get_slice_in_flight_num_bytes:
                OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_NUM_BYTES.clone(),
            object_storage_put_total: OBJECT_STORAGE_PUT_TOTAL.clone(),
            object_storage_put_parts: OBJECT_STORAGE_PUT_PARTS.clone(),
            object_storage_download_num_bytes: OBJECT_STORAGE_DOWNLOAD_NUM_BYTES.clone(),
            object_storage_upload_num_bytes: OBJECT_STORAGE_UPLOAD_NUM_BYTES.clone(),
            object_storage_delete_requests_total,
            object_storage_bulk_delete_requests_total,
            object_storage_delete_request_duration,
            object_storage_bulk_delete_request_duration,
        }
    }
}

/// Counters associated to a cache.
pub struct CacheMetrics {
    pub component_name: String,
    pub cache_metrics: SingleCacheMetrics,
    virtual_caches_metrics: RwLock<HashMap<CacheConfig, SingleCacheMetrics>>,
}

#[derive(Clone)]
pub struct SingleCacheMetrics {
    pub in_cache_count: Gauge,
    pub in_cache_num_bytes: Gauge,
    pub hits_num_items: Counter,
    pub hits_num_bytes: Counter,
    pub misses_num_items: Counter,
    pub evict_num_items: Counter,
    pub evict_num_bytes: Counter,
}

impl CacheMetrics {
    pub fn for_component(component_name: &str) -> Self {
        CacheMetrics {
            component_name: component_name.to_string(),
            cache_metrics: SingleCacheMetrics {
                in_cache_count: gauge!(
                    parent: &*CACHE_IN_CACHE_COUNT,
                    "component_name" => component_name.to_string(),
                ),
                in_cache_num_bytes: gauge!(
                    parent: &*CACHE_IN_CACHE_NUM_BYTES,
                    "component_name" => component_name.to_string(),
                ),
                hits_num_items: counter!(
                    parent: &*CACHE_HITS_TOTAL,
                    "component_name" => component_name.to_string(),
                ),
                hits_num_bytes: counter!(
                    parent: &*CACHE_HITS_BYTES,
                    "component_name" => component_name.to_string(),
                ),
                misses_num_items: counter!(
                    parent: &*CACHE_MISSES_TOTAL,
                    "component_name" => component_name.to_string(),
                ),
                evict_num_items: counter!(
                    parent: &*CACHE_EVICT_TOTAL,
                    "component_name" => component_name.to_string(),
                ),
                evict_num_bytes: counter!(
                    parent: &*CACHE_EVICT_BYTES,
                    "component_name" => component_name.to_string(),
                ),
            },
            virtual_caches_metrics: RwLock::default(),
        }
    }

    pub fn virtual_cache(&self, config: &CacheConfig) -> SingleCacheMetrics {
        if let Some(virtual_cache_metrics) = self.virtual_caches_metrics.read().unwrap().get(config)
        {
            return virtual_cache_metrics.clone();
        }

        let capacity = config.capacity().as_u64().to_string();
        let policy = config.policy().to_string();
        let new_virtual_cache_metrics = SingleCacheMetrics {
            in_cache_count: gauge!(
                parent: &*VIRTUAL_CACHE_IN_CACHE_COUNT,
                "component_name" => self.component_name.clone(),
                "capacity" => capacity.clone(),
                "policy" => policy.clone(),
            ),
            in_cache_num_bytes: gauge!(
                parent: &*VIRTUAL_CACHE_IN_CACHE_NUM_BYTES,
                "component_name" => self.component_name.clone(),
                "capacity" => capacity.clone(),
                "policy" => policy.clone(),
            ),
            hits_num_items: counter!(
                parent: &*VIRTUAL_CACHE_HITS_TOTAL,
                "component_name" => self.component_name.clone(),
                "capacity" => capacity.clone(),
                "policy" => policy.clone(),
            ),
            hits_num_bytes: counter!(
                parent: &*VIRTUAL_CACHE_HITS_BYTES,
                "component_name" => self.component_name.clone(),
                "capacity" => capacity.clone(),
                "policy" => policy.clone(),
            ),
            misses_num_items: counter!(
                parent: &*VIRTUAL_CACHE_MISSES_TOTAL,
                "component_name" => self.component_name.clone(),
                "capacity" => capacity.clone(),
                "policy" => policy.clone(),
            ),
            evict_num_items: counter!(
                parent: &*VIRTUAL_CACHE_EVICT_TOTAL,
                "component_name" => self.component_name.clone(),
                "capacity" => capacity.clone(),
                "policy" => policy.clone(),
            ),
            evict_num_bytes: counter!(
                parent: &*VIRTUAL_CACHE_EVICT_BYTES,
                "component_name" => self.component_name.clone(),
                "capacity" => capacity,
                "policy" => policy,
            ),
        };

        self.virtual_caches_metrics
            .write()
            .unwrap()
            .entry(config.clone())
            .or_insert(new_virtual_cache_metrics)
            .clone()
    }
}

static CACHE_IN_CACHE_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "in_cache_count",
        description: "Count of in cache by component",
        subsystem: "cache",
        observable: true,
    )
});

static CACHE_IN_CACHE_NUM_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "in_cache_num_bytes",
        description: "Number of bytes in cache by component",
        subsystem: "cache",
        observable: true,
    )
});

static CACHE_HITS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_hits_total",
        description: "Number of cache hits by component",
        subsystem: "cache",
        observable: true,
    )
});

static CACHE_HITS_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_hits_bytes",
        description: "Number of cache hits in bytes by component",
        subsystem: "cache",
        observable: true,
    )
});

static CACHE_MISSES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_misses_total",
        description: "Number of cache misses by component",
        subsystem: "cache",
        observable: true,
    )
});

static CACHE_EVICT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_evict_total",
        description: "Number of cache entry evicted by component",
        subsystem: "cache",
        observable: true,
    )
});

static CACHE_EVICT_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_evict_bytes",
        description: "Number of cache entry evicted in bytes by component",
        subsystem: "cache",
        observable: true,
    )
});

static VIRTUAL_CACHE_IN_CACHE_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "virtual_in_cache_count",
        description: "Count of in cache by component",
        subsystem: "cache",
        observable: true,
    )
});

static VIRTUAL_CACHE_IN_CACHE_NUM_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "virtual_in_cache_num_bytes",
        description: "Number of bytes in cache by component",
        subsystem: "cache",
        observable: true,
    )
});

static VIRTUAL_CACHE_HITS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_hits_total",
        description: "Number of cache hits by component",
        subsystem: "cache",
        observable: true,
    )
});

static VIRTUAL_CACHE_HITS_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_hits_bytes",
        description: "Number of cache hits in bytes by component",
        subsystem: "cache",
        observable: true,
    )
});

static VIRTUAL_CACHE_MISSES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_misses_total",
        description: "Number of cache misses by component",
        subsystem: "cache",
        observable: true,
    )
});

static VIRTUAL_CACHE_EVICT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_evict_total",
        description: "Number of cache entry evicted by component",
        subsystem: "cache",
        observable: true,
    )
});

static VIRTUAL_CACHE_EVICT_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_evict_bytes",
        description: "Number of cache entry evicted in bytes by component",
        subsystem: "cache",
        observable: true,
    )
});

/// Storage counters exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static STORAGE_METRICS: LazyLock<StorageMetrics> = LazyLock::new(StorageMetrics::default);

#[cfg(test)]
pub static CACHE_METRICS_FOR_TESTS: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("fortest"));

pub fn object_storage_get_slice_in_flight_guards(
    get_request_size: usize,
) -> (GaugeGuard, GaugeGuard) {
    let mut bytes_guard = GaugeGuard::from_gauge(
        &crate::STORAGE_METRICS.object_storage_get_slice_in_flight_num_bytes,
    );
    bytes_guard.increment(get_request_size as f64);
    let mut count_guard =
        GaugeGuard::from_gauge(&crate::STORAGE_METRICS.object_storage_get_slice_in_flight_count);
    count_guard.increment(1.0);
    (bytes_guard, count_guard)
}
