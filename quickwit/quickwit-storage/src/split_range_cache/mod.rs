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

mod access_trace;
mod coalescer;
#[cfg(all(test, target_os = "linux"))]
mod disk_tests;
pub(crate) mod metrics;
mod storage;
#[cfg(all(test, target_os = "linux"))]
mod tests;

use quickwit_config::DiskCacheConfig;
pub use storage::{FeuerSplitRangeStorage, wrap_storage_with_split_range_cache};

use self::coalescer::DownloadCoalescer;
use crate::{FeuerIoQueues, StorageResolver};

/// Process-wide Feuer cache for exact slices of immutable split objects.
/// Each slice uses the whole-object API; cached slices cannot cover other requests.
pub struct FeuerSplitRangeCache {
    cache: feuer::TieredMemoryDiskCache,
    downloads: DownloadCoalescer,
}

impl FeuerSplitRangeCache {
    /// Open the cache and optionally export native traces to `QW_FEUER_ACCESS_TRACE_URI`.
    pub async fn open_with_access_trace(
        config: &DiskCacheConfig,
        resolver: &StorageResolver,
        queues: &FeuerIoQueues,
    ) -> anyhow::Result<Self> {
        let mut cache = Self::open_with_io_queues(config, queues).await?;
        let destination = match std::env::var("QW_FEUER_ACCESS_TRACE_URI") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => return Ok(cache),
            Err(error) => return Err(error.into()),
        };
        let (storage, run) = access_trace::open_run(&destination, resolver, config).await?;
        cache.cache = access_trace::attach(cache.cache, storage, run);
        Ok(cache)
    }

    /// Open `split_cache` in the resolved directory with explicit disk and memory capacities.
    pub async fn open(config: &DiskCacheConfig) -> anyhow::Result<Self> {
        let queues = FeuerIoQueues::default();
        Self::open_with_io_queues(config, &queues).await
    }

    /// Open an independent split-range instance using shared disk I/O concurrency.
    /// Model each source request's fixed retrieval cost as 256,000 equivalent bytes.
    pub async fn open_with_io_queues(
        config: &DiskCacheConfig,
        queues: &FeuerIoQueues,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            cache: queues
                .open(config, "split_cache", "split_range", Some(256_000))
                .await?,
            downloads: DownloadCoalescer::default(),
        })
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn config_for_test(path: impl AsRef<std::path::Path>) -> DiskCacheConfig {
    DiskCacheConfig {
        path: Some(path.as_ref().to_path_buf()),
        disk_capacity: bytesize::ByteSize::mib(512),
        memory_capacity: bytesize::ByteSize::mib(8),
    }
}
