// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

// See https://prometheus.io/docs/practices/naming/

use once_cell::sync::Lazy;
use quickwit_common::metrics::{new_counter, new_gauge, IntCounter, IntGauge};

/// Counters associated to storage operations.
pub struct StorageMetrics {
    pub shortlived_cache: CacheMetrics,
    pub fast_field_cache: CacheMetrics,
    pub split_footer_cache: CacheMetrics,
}

/// Counters associated to a cache.
#[derive(Clone)]
pub struct CacheMetrics {
    pub component_name: String,
    pub in_cache_count: IntGauge,
    pub in_cache_num_bytes: IntGauge,
    pub hits_num_items: IntCounter,
    pub hits_num_bytes: IntCounter,
    pub miss_num_items: IntCounter,
}

impl CacheMetrics {
    fn for_component(component_name: &str) -> Self {
        let namespace = format!("cache_{component_name}");
        CacheMetrics {
            component_name: component_name.to_string(),
            in_cache_count: new_gauge(
                "in_cache_count",
                "Count of {component_name} in cache",
                &namespace,
            ),
            in_cache_num_bytes: new_gauge(
                "in_cache_num_bytes",
                "Number of {component_name} bytes in cache",
                &namespace,
            ),
            hits_num_items: new_counter(
                &format!("cache_hit_total"),
                "Number of {component_name} cache hits",
                &namespace,
            ),
            hits_num_bytes: new_counter(
                "cache_hits_bytes",
                "Number of {component_name} cache hits in bytes",
                &namespace,
            ),
            miss_num_items: new_counter(
                &format!("cache_miss_total"),
                "Number of {component_name} cache hits",
                &namespace,
            ),
        }
    }
}

impl Default for StorageMetrics {
    fn default() -> Self {
        StorageMetrics {
            fast_field_cache: CacheMetrics::for_component("fastfields"),
            shortlived_cache: CacheMetrics::for_component("shortlived"),
            split_footer_cache: CacheMetrics::for_component("splitfooter"),
        }
    }
}

/// Storage counters exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static STORAGE_METRICS: Lazy<StorageMetrics> = Lazy::new(StorageMetrics::default);

#[cfg(test)]
pub static CACHE_METRICS_FOR_TESTS: Lazy<CacheMetrics> =
    Lazy::new(|| CacheMetrics::for_component("fortest"));
