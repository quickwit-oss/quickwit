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
use quickwit_cache::CacheMetrics;
use quickwit_common::metrics::{new_counter, IntCounter};

/// Counters associated to storage operations.
pub struct StorageMetrics {
    pub shortlived_cache: CacheMetrics,
    pub fast_field_cache: CacheMetrics,
    pub split_footer_cache: CacheMetrics,
    pub object_storage_get_total: IntCounter,
    pub object_storage_put_total: IntCounter,
    pub object_storage_put_parts: IntCounter,
    pub object_storage_download_num_bytes: IntCounter,
}

impl Default for StorageMetrics {
    fn default() -> Self {
        StorageMetrics {
            fast_field_cache: CacheMetrics::for_component("fastfields"),
            shortlived_cache: CacheMetrics::for_component("shortlived"),
            split_footer_cache: CacheMetrics::for_component("splitfooter"),
            object_storage_get_total: new_counter(
                "object_storage_gets_total",
                "Number of objects fetched.",
                "quickwit_storage",
            ),
            object_storage_put_total: new_counter(
                "object_storage_puts_total",
                "Number of objects uploaded. May differ from object_storage_requests_parts due to \
                 multipart upload.",
                "quickwit_storage",
            ),
            object_storage_put_parts: new_counter(
                "object_storage_puts_parts",
                "Number of object parts uploaded.",
                "",
            ),
            object_storage_download_num_bytes: new_counter(
                "object_storage_download_num_bytes",
                "Amount of data downloaded from an object storage.",
                "quickwit_storage",
            ),
        }
    }
}

/// Storage counters exposes a bunch a set of storage/cache related metrics through a prometheus
/// endpoint.
pub static STORAGE_METRICS: Lazy<StorageMetrics> = Lazy::new(StorageMetrics::default);

#[cfg(test)]
pub static CACHE_METRICS_FOR_TESTS: Lazy<CacheMetrics> =
    Lazy::new(|| CacheMetrics::for_component("fortest"));
