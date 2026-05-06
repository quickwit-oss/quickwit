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

//! Storage and cache metrics following
//! [Prometheus naming conventions](https://prometheus.io/docs/practices/naming/).

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use quickwit_config::CacheConfig;
use quickwit_metrics::{
    Counter, Gauge, GaugeGuard, Histogram, LabelNames, Labels, counter, gauge, histogram,
    label_names, label_values, labels,
};

const ACTION_DELETE_OBJECT: Labels<1> = labels!("action" => "delete_object");
const OUTCOME: LabelNames<1> = label_names!("outcome");
const COMPONENT_NAME: LabelNames<1> = label_names!("component_name");
const COMPONENT_CAPACITY_POLICY: LabelNames<3> =
    label_names!("component_name", "capacity", "policy");

static GET_SLICE_TIMEOUT_OUTCOME_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "get_slice_timeout_outcome",
        description: "Outcome of get_slice operations. success_after_1_timeout means the operation succeeded after a retry caused by a timeout.",
        subsystem: "storage",
    )
});

pub(crate) static GET_SLICE_TIMEOUT_SUCCESS_AFTER_0_TIMEOUT: LazyLock<Counter> = LazyLock::new(
    || counter!(parent: GET_SLICE_TIMEOUT_OUTCOME_TOTAL, labels: [label_values!(OUTCOME => "success_after_0_timeout")]),
);

pub(crate) static GET_SLICE_TIMEOUT_SUCCESS_AFTER_1_TIMEOUT: LazyLock<Counter> = LazyLock::new(
    || counter!(parent: GET_SLICE_TIMEOUT_OUTCOME_TOTAL, labels: [label_values!(OUTCOME => "success_after_1_timeout")]),
);

pub(crate) static GET_SLICE_TIMEOUT_SUCCESS_AFTER_2_PLUS_TIMEOUT: LazyLock<Counter> = LazyLock::new(
    || counter!(parent: GET_SLICE_TIMEOUT_OUTCOME_TOTAL, labels: [label_values!(OUTCOME => "success_after_2+_timeout")]),
);

pub(crate) static GET_SLICE_TIMEOUT_ALL_TIMEOUTS: LazyLock<Counter> = LazyLock::new(
    || counter!(parent: GET_SLICE_TIMEOUT_OUTCOME_TOTAL, labels: [label_values!(OUTCOME => "all_timeouts")]),
);

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

pub(crate) static OBJECT_STORAGE_DELETE_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(
    || counter!(parent: OBJECT_STORAGE_REQUESTS_TOTAL, labels: [ACTION_DELETE_OBJECT]),
);

pub(crate) static OBJECT_STORAGE_BULK_DELETE_REQUESTS_TOTAL: LazyLock<Counter> = LazyLock::new(
    || counter!(parent: OBJECT_STORAGE_REQUESTS_TOTAL, labels: [ACTION_DELETE_OBJECT]),
);

pub(crate) static OBJECT_STORAGE_DELETE_REQUEST_DURATION: LazyLock<Histogram> = LazyLock::new(
    || histogram!(parent: OBJECT_STORAGE_REQUEST_DURATION, labels: [ACTION_DELETE_OBJECT]),
);

pub(crate) static OBJECT_STORAGE_BULK_DELETE_REQUEST_DURATION: LazyLock<Histogram> = LazyLock::new(
    || histogram!(parent: OBJECT_STORAGE_REQUEST_DURATION, labels: [ACTION_DELETE_OBJECT]),
);

pub(crate) static OBJECT_STORAGE_GET_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_gets_total",
        description: "Number of objects fetched. Might be lower than get_slice_timeout_outcome if queries are debounced.",
        subsystem: "storage",
    )
});

pub(crate) static OBJECT_STORAGE_GET_ERRORS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_get_errors_total",
        description: "Number of GetObject errors.",
        subsystem: "storage",
    )
});

pub(crate) static OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "object_storage_get_slice_in_flight_count",
        description: "Number of GetObject for which the memory was allocated but the download is still in progress.",
        subsystem: "storage",
    )
});

pub(crate) static OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_NUM_BYTES: LazyLock<Gauge> =
    LazyLock::new(|| {
        gauge!(
            name: "object_storage_get_slice_in_flight_num_bytes",
            description: "Memory allocated for GetObject requests that are still in progress.",
            subsystem: "storage",
        )
    });

pub(crate) static OBJECT_STORAGE_PUT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_puts_total",
        description: "Number of objects uploaded. May differ from object_storage_requests_parts due to multipart upload.",
        subsystem: "storage",
    )
});

pub(crate) static OBJECT_STORAGE_PUT_PARTS: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_puts_parts",
        description: "Number of object parts uploaded.",
        subsystem: "",
    )
});

pub(crate) static OBJECT_STORAGE_DOWNLOAD_NUM_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_download_num_bytes",
        description: "Amount of data downloaded from an object storage.",
        subsystem: "storage",
    )
});

pub(crate) static OBJECT_STORAGE_UPLOAD_NUM_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "object_storage_upload_num_bytes",
        description: "Amount of data uploaded to an object storage.",
        subsystem: "storage",
    )
});

/// Metrics for a named cache component (e.g. "shortlived", "splitfooter").
///
/// Each `CacheMetrics` instance holds a set of counters and gauges scoped to a
/// `component_name` label. It also supports virtual sub-caches keyed by
/// [`CacheConfig`], which add `capacity` and `policy` labels.
pub struct CacheMetrics {
    component_name: String,
    pub(crate) cache_metrics: SingleCacheMetrics,
    virtual_caches_metrics: RwLock<HashMap<CacheConfig, SingleCacheMetrics>>,
}

/// Per-cache counters and gauges tracking items in cache, hits, misses, and
/// evictions.
#[derive(Clone)]
pub struct SingleCacheMetrics {
    /// Current number of items stored in the cache.
    pub(crate) in_cache_count: Gauge,
    /// Current number of bytes stored in the cache.
    pub(crate) in_cache_num_bytes: Gauge,
    /// Total number of cache hits (items).
    pub(crate) hits_num_items: Counter,
    /// Total number of cache hit bytes.
    pub(crate) hits_num_bytes: Counter,
    /// Total number of cache misses (items).
    pub(crate) misses_num_items: Counter,
    /// Total number of evicted items.
    pub(crate) evict_num_items: Counter,
    /// Total number of evicted bytes.
    pub(crate) evict_num_bytes: Counter,
}

impl CacheMetrics {
    /// Creates a new `CacheMetrics` for the given component name.
    pub fn for_component(component_name: &str) -> Self {
        let component_name = component_name.to_string();
        let labels = label_values!(COMPONENT_NAME => component_name.clone());
        CacheMetrics {
            component_name,
            cache_metrics: SingleCacheMetrics {
                in_cache_count: gauge!(parent: CACHE_IN_CACHE_COUNT, labels: [labels]),
                in_cache_num_bytes: gauge!(parent: CACHE_IN_CACHE_NUM_BYTES, labels: [labels]),
                hits_num_items: counter!(parent: CACHE_HITS_TOTAL, labels: [labels]),
                hits_num_bytes: counter!(parent: CACHE_HITS_BYTES, labels: [labels]),
                misses_num_items: counter!(parent: CACHE_MISSES_TOTAL, labels: [labels]),
                evict_num_items: counter!(parent: CACHE_EVICT_TOTAL, labels: [labels]),
                evict_num_bytes: counter!(parent: CACHE_EVICT_BYTES, labels: [labels]),
            },
            virtual_caches_metrics: RwLock::default(),
        }
    }

    /// Returns metrics for a virtual sub-cache identified by `config`.
    ///
    /// Virtual caches share the same parent component but add `capacity` and
    /// `policy` labels. Instances are cached and reused across calls.
    pub(crate) fn virtual_cache(&self, config: &CacheConfig) -> SingleCacheMetrics {
        if let Some(virtual_cache_metrics) = self.virtual_caches_metrics.read().unwrap().get(config)
        {
            return virtual_cache_metrics.clone();
        }

        let labels = label_values!(
            COMPONENT_CAPACITY_POLICY =>
            self.component_name.clone(),
            config.capacity().as_u64().to_string(),
            config.policy().to_string(),
        );
        let new_virtual_cache_metrics = SingleCacheMetrics {
            in_cache_count: gauge!(parent: VIRTUAL_CACHE_IN_CACHE_COUNT, labels: [labels]),
            in_cache_num_bytes: gauge!(parent: VIRTUAL_CACHE_IN_CACHE_NUM_BYTES, labels: [labels]),
            hits_num_items: counter!(parent: VIRTUAL_CACHE_HITS_TOTAL, labels: [labels]),
            hits_num_bytes: counter!(parent: VIRTUAL_CACHE_HITS_BYTES, labels: [labels]),
            misses_num_items: counter!(parent: VIRTUAL_CACHE_MISSES_TOTAL, labels: [labels]),
            evict_num_items: counter!(parent: VIRTUAL_CACHE_EVICT_TOTAL, labels: [labels]),
            evict_num_bytes: counter!(parent: VIRTUAL_CACHE_EVICT_BYTES, labels: [labels]),
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
    )
});

static CACHE_IN_CACHE_NUM_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "in_cache_num_bytes",
        description: "Number of bytes in cache by component",
        subsystem: "cache",
    )
});

static CACHE_HITS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_hits_total",
        description: "Number of cache hits by component",
        subsystem: "cache",
    )
});

static CACHE_HITS_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_hits_bytes",
        description: "Number of cache hits in bytes by component",
        subsystem: "cache",
    )
});

static CACHE_MISSES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_misses_total",
        description: "Number of cache misses by component",
        subsystem: "cache",
    )
});

static CACHE_EVICT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_evict_total",
        description: "Number of cache entry evicted by component",
        subsystem: "cache",
    )
});

static CACHE_EVICT_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "cache_evict_bytes",
        description: "Number of cache entry evicted in bytes by component",
        subsystem: "cache",
    )
});

static VIRTUAL_CACHE_IN_CACHE_COUNT: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "virtual_in_cache_count",
        description: "Count of in cache by component",
        subsystem: "cache",
    )
});

static VIRTUAL_CACHE_IN_CACHE_NUM_BYTES: LazyLock<Gauge> = LazyLock::new(|| {
    gauge!(
        name: "virtual_in_cache_num_bytes",
        description: "Number of bytes in cache by component",
        subsystem: "cache",
    )
});

static VIRTUAL_CACHE_HITS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_hits_total",
        description: "Number of cache hits by component",
        subsystem: "cache",
    )
});

static VIRTUAL_CACHE_HITS_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_hits_bytes",
        description: "Number of cache hits in bytes by component",
        subsystem: "cache",
    )
});

static VIRTUAL_CACHE_MISSES_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_misses_total",
        description: "Number of cache misses by component",
        subsystem: "cache",
    )
});

static VIRTUAL_CACHE_EVICT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_evict_total",
        description: "Number of cache entry evicted by component",
        subsystem: "cache",
    )
});

static VIRTUAL_CACHE_EVICT_BYTES: LazyLock<Counter> = LazyLock::new(|| {
    counter!(
        name: "virtual_cache_evict_bytes",
        description: "Number of cache entry evicted in bytes by component",
        subsystem: "cache",
    )
});

pub(crate) static FAST_FIELD_CACHE: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("fastfields"));

pub(crate) static FD_CACHE_METRICS: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("fd"));

/// Cache metrics for partial-request byte ranges (used by leaf search caches).
pub static PARTIAL_REQUEST_CACHE: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("partial_request"));

/// Cache metrics for predicate-evaluated content (used by leaf search caches).
pub static PREDICATE_CACHE: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("predicate"));

pub(crate) static SEARCHER_SPLIT_CACHE: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("searcher_split"));

/// Cache metrics for short-lived byte range caches (used during leaf search
/// and caching directory warmup).
pub static SHORTLIVED_CACHE: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("shortlived"));

/// Cache metrics for split footer caches (used to cache split metadata).
pub static SPLIT_FOOTER_CACHE: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("splitfooter"));

#[cfg(test)]
/// Cache metrics for tests.
pub static CACHE_METRICS_FOR_TESTS: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("fortest"));

pub(crate) fn object_storage_get_slice_in_flight_guards(
    get_request_size: usize,
) -> (GaugeGuard, GaugeGuard) {
    let bytes_guard = GaugeGuard::new(
        &OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_NUM_BYTES,
        get_request_size as f64,
    );
    let count_guard = GaugeGuard::new(&OBJECT_STORAGE_GET_SLICE_IN_FLIGHT_COUNT, 1.0);
    (bytes_guard, count_guard)
}
