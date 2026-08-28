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

mod key;
mod metrics;
mod storage;
#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use bytesize::ByteSize;
use foyer::{Code, DeviceBuilder};
pub(crate) use key::SplitRangeCacheKey;
use quickwit_config::{
    CachePolicy, DiskCompression, RecoverMode, SplitRangeCacheWritePolicy,
    SplitRangeDiskCacheConfig,
};
pub use storage::{FoyerSplitRangeStorage, wrap_storage_with_split_range_cache};

/// Process-wide Foyer hybrid cache for exact split byte-range payloads.
pub struct FoyerSplitRangeCache {
    pub(crate) cache: foyer::HybridCache<SplitRangeCacheKey, Bytes>,
    pub(crate) max_entry_size: usize,
    pub(crate) block_size: usize,
}

impl FoyerSplitRangeCache {
    /// Create `config.path` if needed, open Foyer's cache files there, recover
    /// any existing image, and start flushers/reclaimers.
    ///
    /// This does not mount a volume; the path must already sit on a usable
    /// filesystem (for example an EBS mount).
    pub async fn open(config: &SplitRangeDiskCacheConfig) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&config.path)
            .await
            .with_context(|| {
                format!(
                    "failed to create split range cache directory `{}`",
                    config.path.display()
                )
            })?;
        let device = build_foyer_fs_device(
            &config.path,
            bytesize_to_usize(config.disk_capacity, "disk_capacity")?,
        )?;
        let engine = build_block_engine(config, device)?;
        let memory_capacity = bytesize_to_usize(config.memory_capacity, "memory_capacity")?;
        let cache = foyer::HybridCacheBuilder::new()
            .with_name("split-range-v1")
            .with_metrics_registry(Box::new(metrics::QuickwitMetricsRegistry))
            .with_policy(foyer_write_policy(config.write_policy))
            .with_flush_on_close(foyer_flush_on_close(config.write_policy))
            .memory(memory_capacity)
            .with_eviction_config(foyer_memory_eviction_config(config.memory_eviction_policy)?)
            .with_weighter(|key: &SplitRangeCacheKey, value: &Bytes| {
                key.estimated_size() + value.len()
            })
            .storage()
            .with_engine_config(engine)
            .with_recover_mode(foyer_recover_mode(config.recover_mode))
            .with_compression(foyer_compression(config.compression))
            .build()
            .await?;
        Ok(Self {
            cache,
            max_entry_size: bytesize_to_usize(config.max_entry_size, "max_entry_size")?,
            block_size: bytesize_to_usize(config.block_size, "block_size")?,
        })
    }

    /// Stop new disk writes and wait for in-flight flush/reclaim work.
    ///
    /// Cache files stay on disk for the next [`Self::open`]. This drops file
    /// descriptors only; it does not unmount the volume.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.cache.close().await.map_err(Into::into)
    }
}

pub(crate) fn foyer_write_policy(policy: SplitRangeCacheWritePolicy) -> foyer::HybridCachePolicy {
    match policy {
        SplitRangeCacheWritePolicy::WriteOnEviction => foyer::HybridCachePolicy::WriteOnEviction,
        SplitRangeCacheWritePolicy::WriteOnInsertion => foyer::HybridCachePolicy::WriteOnInsertion,
    }
}

/// Flush the memory tier on close under write-on-eviction so a graceful
/// restart can recover hot entries. Write-on-insertion already submitted
/// those entries to disk.
pub(crate) fn foyer_flush_on_close(policy: SplitRangeCacheWritePolicy) -> bool {
    matches!(policy, SplitRangeCacheWritePolicy::WriteOnEviction)
}

fn foyer_recover_mode(recover_mode: RecoverMode) -> foyer::RecoverMode {
    match recover_mode {
        RecoverMode::Quiet => foyer::RecoverMode::Quiet,
    }
}

fn foyer_compression(compression: DiskCompression) -> foyer::Compression {
    match compression {
        DiskCompression::Lz4 => foyer::Compression::Lz4,
    }
}

fn foyer_memory_eviction_config(policy: CachePolicy) -> anyhow::Result<foyer::S3FifoConfig> {
    match policy {
        CachePolicy::S3Fifo => Ok(foyer::S3FifoConfig::default()),
        CachePolicy::Lru | CachePolicy::TinyLfu => {
            anyhow::bail!(
                "split_range_disk_cache.memory_eviction_policy must be s3-fifo in phase 1"
            )
        }
    }
}

fn build_foyer_fs_device(
    path: &Path,
    disk_capacity: usize,
) -> anyhow::Result<Arc<dyn foyer::Device>> {
    let device = foyer::FsDeviceBuilder::new(path)
        .with_capacity(disk_capacity)
        .with_throttle(foyer::Throttle::default())
        .build()?;
    Ok(device)
}

fn build_block_engine(
    config: &SplitRangeDiskCacheConfig,
    device: Arc<dyn foyer::Device>,
) -> anyhow::Result<foyer::BlockEngineConfig<SplitRangeCacheKey, Bytes, foyer::HybridCacheProperties>>
{
    Ok(foyer::BlockEngineConfig::new(device)
        .with_block_size(bytesize_to_usize(config.block_size, "block_size")?)
        .with_flushers(config.flushers)
        .with_reclaimers(config.reclaimers)
        .with_clean_block_threshold(config.clean_block_threshold)
        .with_buffer_pool_size(bytesize_to_usize(
            config.buffer_pool_size,
            "buffer_pool_size",
        )?)
        .with_submit_queue_size_threshold(bytesize_to_usize(
            config.submit_queue_size_threshold,
            "submit_queue_size_threshold",
        )?)
        .with_eviction_pickers(vec![
            Box::new(foyer::InvalidRatioPicker::new(0.8)),
            Box::<foyer::FifoPicker>::default(),
        ]))
}

fn bytesize_to_usize(size: ByteSize, field: &'static str) -> anyhow::Result<usize> {
    usize::try_from(size.as_u64())
        .with_context(|| format!("split_range_disk_cache.{field} does not fit usize"))
}

#[cfg(test)]
pub(crate) fn config_for_test(path: impl AsRef<Path>) -> SplitRangeDiskCacheConfig {
    SplitRangeDiskCacheConfig {
        path: path.as_ref().to_path_buf(),
        disk_capacity: ByteSize::mb(64),
        memory_capacity: ByteSize::mb(8),
        buffer_pool_size: ByteSize::mb(4),
        submit_queue_size_threshold: ByteSize::mb(8),
        memory_eviction_policy: CachePolicy::S3Fifo,
        write_policy: SplitRangeCacheWritePolicy::WriteOnEviction,
        compression: DiskCompression::Lz4,
        recover_mode: RecoverMode::Quiet,
        block_size: ByteSize::mb(4),
        max_entry_size: ByteSize::mb(2),
        flushers: 1,
        reclaimers: 1,
        clean_block_threshold: 16,
    }
}
