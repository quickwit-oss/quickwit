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

//! Foyer-backed hybrid cache (memory + disk) implementing [`StorageCache`].
//!
//! When entries are evicted from the memory tier, they spill to local disk
//! instead of being lost. The next access reads from disk (~0.1ms) instead
//! of fetching from S3 (~50-200ms).

use std::ops::Range;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use foyer::DeviceBuilder as _;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::OwnedBytes;
use crate::cache::StorageCache;
use crate::metrics::SingleCacheMetrics;

const FULL_SLICE: Range<usize> = 0..usize::MAX;

/// Cache key for the foyer hybrid cache: (file path, byte range).
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
struct FoyerSliceKey {
    path: String,
    range_start: usize,
    range_end: usize,
}

impl FoyerSliceKey {
    fn new(path: &Path, byte_range: Range<usize>) -> Self {
        FoyerSliceKey {
            path: path.to_string_lossy().into_owned(),
            range_start: byte_range.start,
            range_end: byte_range.end,
        }
    }
}

/// Cache value wrapper for foyer — just bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct FoyerSliceValue(Vec<u8>);

/// A hybrid (memory + disk) cache implementing `StorageCache`.
///
/// Backed by foyer's `HybridCache`. Memory tier holds hot entries,
/// disk tier catches evicted entries.
pub struct FoyerStorageCache {
    cache: foyer::HybridCache<FoyerSliceKey, FoyerSliceValue>,
    metrics: SingleCacheMetrics,
}

impl FoyerStorageCache {
    /// Build a new hybrid storage cache.
    ///
    /// - `memory_capacity`: bytes for the in-memory tier
    /// - `disk_path`: directory for the disk tier
    /// - `disk_capacity`: bytes for the disk tier
    /// - `metrics`: Prometheus counters for this cache component
    pub async fn build(
        memory_capacity: usize,
        disk_path: &Path,
        disk_capacity: usize,
        metrics: SingleCacheMetrics,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(disk_path).await?;

        info!(
            memory_capacity_mb = memory_capacity / 1024 / 1024,
            disk_capacity_mb = disk_capacity / 1024 / 1024,
            disk_path = %disk_path.display(),
            "building foyer hybrid storage cache"
        );

        let device = foyer::FsDeviceBuilder::new(disk_path)
            .with_capacity(disk_capacity)
            .build()?;
        let engine_config = foyer::BlockEngineConfig::new(device);

        let cache = foyer::HybridCacheBuilder::new()
            .memory(memory_capacity)
            .storage()
            .with_engine_config(engine_config)
            .build()
            .await?;

        Ok(FoyerStorageCache { cache, metrics })
    }
}

#[async_trait]
impl StorageCache for FoyerStorageCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let key = FoyerSliceKey::new(path, byte_range);
        match self.cache.get(&key).await {
            Ok(Some(entry)) => {
                self.metrics.hits_num_items.inc();
                let bytes = &entry.value().0;
                self.metrics.hits_num_bytes.inc_by(bytes.len() as u64);
                Some(OwnedBytes::new(bytes.clone()))
            }
            Ok(None) => {
                self.metrics.misses_num_items.inc();
                None
            }
            Err(err) => {
                self.metrics.misses_num_items.inc();
                warn!(error = %err, "foyer storage cache get failed");
                None
            }
        }
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        self.get(path, FULL_SLICE).await
    }

    async fn put(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        let key = FoyerSliceKey::new(&path, byte_range);
        self.metrics.in_cache_count.inc();
        self.metrics.in_cache_num_bytes.add(bytes.len() as i64);
        self.cache.insert(key, FoyerSliceValue(bytes.to_vec()));
    }

    async fn put_all(&self, path: PathBuf, bytes: OwnedBytes) {
        self.put(path, FULL_SLICE, bytes).await;
    }
}
