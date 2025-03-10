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

use std::sync::LazyLock;

use quickwit_common::metrics::{
    new_counter, new_counter_vec, new_gauge, new_histogram_vec, Histogram, IntCounter,
    IntCounterVec, IntGauge,
};

/// Counters associated to storage operations.
pub struct StorageMetrics {
    pub shortlived_cache: CacheMetrics,
    pub partial_request_cache: CacheMetrics,
    pub fd_cache_metrics: CacheMetrics,
    pub fast_field_cache: CacheMetrics,
    pub split_footer_cache: CacheMetrics,
    pub searcher_split_cache: CacheMetrics,
    pub get_slice_timeout_successes: [IntCounter; 3],
    pub get_slice_timeout_all_timeouts: IntCounter,
    pub object_storage_get_total: IntCounter,
    pub object_storage_get_errors_total: IntCounterVec<1>,
    pub object_storage_put_total: IntCounter,
    pub object_storage_put_parts: IntCounter,
    pub object_storage_download_num_bytes: IntCounter,
    pub object_storage_upload_num_bytes: IntCounter,

    pub object_storage_delete_requests_total: IntCounter,
    pub object_storage_bulk_delete_requests_total: IntCounter,
    pub object_storage_delete_request_duration:
        LazyLock<Histogram, Box<dyn Fn() -> Histogram + Send>>,
    pub object_storage_bulk_delete_request_duration:
        LazyLock<Histogram, Box<dyn Fn() -> Histogram + Send>>,
    pub object_storage_get_request_duration: LazyLock<Histogram, Box<dyn Fn() -> Histogram + Send>>,
    pub object_storage_put_request_duration: LazyLock<Histogram, Box<dyn Fn() -> Histogram + Send>>,
    pub object_storage_put_part_request_duration:
        LazyLock<Histogram, Box<dyn Fn() -> Histogram + Send>>,
}

impl Default for StorageMetrics {
    fn default() -> Self {
        let get_slice_timeout_outcome_total_vec = new_counter_vec(
            "get_slice_timeout_outcome",
            "Outcome of get_slice operations. success_after_1_timeout means the operation \
             succeeded after a retry caused by a timeout.",
            "storage",
            &[],
            ["outcome"],
        );
        let get_slice_timeout_successes = [
            get_slice_timeout_outcome_total_vec.with_label_values(["success_after_0_timeout"]),
            get_slice_timeout_outcome_total_vec.with_label_values(["success_after_1_timeout"]),
            get_slice_timeout_outcome_total_vec.with_label_values(["success_after_2+_timeout"]),
        ];
        let get_slice_timeout_all_timeouts =
            get_slice_timeout_outcome_total_vec.with_label_values(["all_timeouts"]);

        let object_storage_requests_total = new_counter_vec(
            "object_storage_requests_total",
            "Total number of object storage requests performed.",
            "storage",
            &[],
            ["action"],
        );
        let object_storage_delete_requests_total =
            object_storage_requests_total.with_label_values(["delete_object"]);
        let object_storage_bulk_delete_requests_total =
            object_storage_requests_total.with_label_values(["delete_objects"]);

        let object_storage_request_duration = new_histogram_vec(
            "object_storage_request_duration_seconds",
            "Duration of object storage requests in seconds.",
            "storage",
            &[],
            ["action"],
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
        );
        let object_storage_request_duration_clone = object_storage_request_duration.clone();
        let object_storage_delete_request_duration = LazyLock::new(Box::new(move || {
            object_storage_request_duration_clone.with_label_values(["delete_object"])
        }) as _);
        let object_storage_request_duration_clone = object_storage_request_duration.clone();
        let object_storage_bulk_delete_request_duration = LazyLock::new(Box::new(move || {
            object_storage_request_duration_clone.with_label_values(["delete_objects"])
        }) as _);
        let object_storage_request_duration_clone = object_storage_request_duration.clone();
        let object_storage_get_request_duration = LazyLock::new(Box::new(move || {
            object_storage_request_duration_clone.with_label_values(["get"])
        }) as _);
        let object_storage_request_duration_clone = object_storage_request_duration.clone();
        let object_storage_put_request_duration = LazyLock::new(Box::new(move || {
            object_storage_request_duration_clone.with_label_values(["put"])
        }) as _);
        let object_storage_request_duration_clone = object_storage_request_duration.clone();
        let object_storage_put_part_request_duration = LazyLock::new(Box::new(move || {
            object_storage_request_duration_clone.with_label_values(["put_part"])
        }) as _);

        StorageMetrics {
            fast_field_cache: CacheMetrics::for_component("fastfields"),
            fd_cache_metrics: CacheMetrics::for_component("fd"),
            partial_request_cache: CacheMetrics::for_component("partial_request"),
            searcher_split_cache: CacheMetrics::for_component("searcher_split"),
            shortlived_cache: CacheMetrics::for_component("shortlived"),
            split_footer_cache: CacheMetrics::for_component("splitfooter"),
            get_slice_timeout_successes,
            get_slice_timeout_all_timeouts,
            object_storage_get_total: new_counter(
                "object_storage_gets_total",
                "Number of objects fetched.",
                "storage",
                &[],
            ),
            object_storage_get_errors_total: new_counter_vec::<1>(
                "object_storage_get_errors_total",
                "Number of GetObject errors.",
                "storage",
                &[],
                ["code"],
            ),
            object_storage_put_total: new_counter(
                "object_storage_puts_total",
                "Number of objects uploaded. May differ from object_storage_requests_parts due to \
                 multipart upload.",
                "storage",
                &[],
            ),
            object_storage_put_parts: new_counter(
                "object_storage_puts_parts",
                "Number of object parts uploaded.",
                "",
                &[],
            ),
            object_storage_download_num_bytes: new_counter(
                "object_storage_download_num_bytes",
                "Amount of data downloaded from an object storage.",
                "storage",
                &[],
            ),
            object_storage_upload_num_bytes: new_counter(
                "object_storage_upload_num_bytes",
                "Amount of data uploaded to an object storage.",
                "storage",
                &[],
            ),
            object_storage_delete_requests_total,
            object_storage_bulk_delete_requests_total,
            object_storage_delete_request_duration,
            object_storage_bulk_delete_request_duration,
            object_storage_get_request_duration,
            object_storage_put_request_duration,
            object_storage_put_part_request_duration,
        }
    }
}

/// Counters associated to a cache.
#[derive(Clone)]
pub struct CacheMetrics {
    pub component_name: String,
    pub in_cache_count: IntGauge,
    pub in_cache_num_bytes: IntGauge,
    pub hits_num_items: IntCounter,
    pub hits_num_bytes: IntCounter,
    pub misses_num_items: IntCounter,
    pub evict_num_items: IntCounter,
    pub evict_num_bytes: IntCounter,
}

impl CacheMetrics {
    pub fn for_component(component_name: &str) -> Self {
        const CACHE_METRICS_NAMESPACE: &str = "cache";
        CacheMetrics {
            component_name: component_name.to_string(),
            in_cache_count: new_gauge(
                "in_cache_count",
                "Count of in cache by component",
                CACHE_METRICS_NAMESPACE,
                &[("component_name", component_name)],
            ),
            in_cache_num_bytes: new_gauge(
                "in_cache_num_bytes",
                "Number of bytes in cache by component",
                CACHE_METRICS_NAMESPACE,
                &[("component_name", component_name)],
            ),
            hits_num_items: new_counter(
                "cache_hits_total",
                "Number of cache hits by component",
                CACHE_METRICS_NAMESPACE,
                &[("component_name", component_name)],
            ),
            hits_num_bytes: new_counter(
                "cache_hits_bytes",
                "Number of cache hits in bytes by component",
                CACHE_METRICS_NAMESPACE,
                &[("component_name", component_name)],
            ),
            misses_num_items: new_counter(
                "cache_misses_total",
                "Number of cache misses by component",
                CACHE_METRICS_NAMESPACE,
                &[("component_name", component_name)],
            ),
            evict_num_items: new_counter(
                "cache_evict_total",
                "Number of cache entry evicted by component",
                CACHE_METRICS_NAMESPACE,
                &[("component_name", component_name)],
            ),
            evict_num_bytes: new_counter(
                "cache_evict_bytes",
                "Number of cache entry evicted in bytes by component",
                CACHE_METRICS_NAMESPACE,
                &[("component_name", component_name)],
            ),
        }
    }
}

/// Storage counters exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static STORAGE_METRICS: LazyLock<StorageMetrics> = LazyLock::new(StorageMetrics::default);

#[cfg(test)]
pub static CACHE_METRICS_FOR_TESTS: LazyLock<CacheMetrics> =
    LazyLock::new(|| CacheMetrics::for_component("fortest"));
