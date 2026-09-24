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

#[cfg(test)]
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use bytesize::ByteSize;
use foyer::{Code, DeviceBuilder};
pub(crate) use key::SplitRangeCacheKey;
use quickwit_config::{SplitRangeDiskCacheConfig, SplitRangeMemoryEvictionPolicy};
pub use storage::{FoyerSplitRangeStorage, wrap_storage_with_split_range_cache};

const BLOCK_SIZE: usize = ByteSize::mb(64).as_u64() as usize;
const MAX_ENTRY_SIZE: usize = ByteSize::mb(60).as_u64() as usize;
const FLUSHERS: usize = 8;
const RECLAIMERS: usize = 8;
const CLEAN_BLOCK_THRESHOLD: usize = 16;
const S3FIFO_GHOST_QUEUE_CAPACITY_RATIO: f64 = 1.0;
const S3FIFO_SMALL_QUEUE_CAPACITY_RATIO: f64 = 0.2;
const S3FIFO_SMALL_TO_MAIN_FREQ_THRESHOLD: u8 = 1;
const COST_AWARE_FIXED_RETRIEVAL_COST: u64 = 10_000_000;
const COST_AWARE_SAMPLE_SIZE: usize = 2048;

/// Foyer hybrid cache for exact split byte-range payloads.
pub struct FoyerSplitRangeCache {
    pub(crate) cache: foyer::HybridCache<SplitRangeCacheKey, Bytes>,
    pub(crate) max_entry_size: usize,
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
        let device = build_foyer_fs_device(config)?;
        let engine = build_block_engine(config, device)?;
        let memory_capacity = bytesize_to_usize(config.memory_capacity, "memory_capacity")?;
        let cache = foyer::HybridCacheBuilder::new()
            .with_name("split-range-v1")
            .with_metrics_registry(Box::new(metrics::QuickwitMetricsRegistry))
            .with_policy(foyer::HybridCachePolicy::WriteOnEviction)
            .with_flush_on_close(true)
            .memory(memory_capacity)
            .with_eviction_config(foyer_memory_eviction_config(config.memory_eviction_policy))
            .with_weighter(|key: &SplitRangeCacheKey, value: &Bytes| {
                key.estimated_size() + value.len()
            })
            .storage()
            .with_engine_config(engine)
            .with_recover_mode(foyer::RecoverMode::Quiet)
            .with_compression(foyer::Compression::None)
            .build()
            .await?;
        Ok(Self {
            cache,
            max_entry_size: MAX_ENTRY_SIZE,
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

fn foyer_memory_eviction_config(policy: SplitRangeMemoryEvictionPolicy) -> foyer::EvictionConfig {
    match policy {
        SplitRangeMemoryEvictionPolicy::S3Fifo => foyer::S3FifoConfig {
            ghost_queue_capacity_ratio: S3FIFO_GHOST_QUEUE_CAPACITY_RATIO,
            small_queue_capacity_ratio: S3FIFO_SMALL_QUEUE_CAPACITY_RATIO,
            small_to_main_freq_threshold: S3FIFO_SMALL_TO_MAIN_FREQ_THRESHOLD,
        }
        .into(),
        SplitRangeMemoryEvictionPolicy::CostAware => foyer::CostAwareConfig {
            fixed_retrieval_cost: COST_AWARE_FIXED_RETRIEVAL_COST,
            sample_size: COST_AWARE_SAMPLE_SIZE,
        }
        .into(),
    }
}

fn foyer_throttle(config: &SplitRangeDiskCacheConfig) -> anyhow::Result<foyer::Throttle> {
    Ok(
        foyer::Throttle::default().with_write_throughput(bytesize_to_usize(
            config.write_throughput,
            "write_throughput",
        )?),
    )
}

fn build_foyer_fs_device(
    config: &SplitRangeDiskCacheConfig,
) -> anyhow::Result<Arc<dyn foyer::Device>> {
    let device = foyer::FsDeviceBuilder::new(&config.path)
        .with_capacity(bytesize_to_usize(config.disk_capacity, "disk_capacity")?)
        .with_throttle(foyer_throttle(config)?)
        .build()?;
    Ok(device)
}

fn build_block_engine(
    config: &SplitRangeDiskCacheConfig,
    device: Arc<dyn foyer::Device>,
) -> anyhow::Result<foyer::BlockEngineConfig<SplitRangeCacheKey, Bytes, foyer::HybridCacheProperties>>
{
    Ok(foyer::BlockEngineConfig::new(device)
        .with_block_size(BLOCK_SIZE)
        .with_flushers(FLUSHERS)
        .with_reclaimers(RECLAIMERS)
        .with_clean_block_threshold(CLEAN_BLOCK_THRESHOLD)
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
        disk_capacity: ByteSize::mb(512),
        memory_capacity: ByteSize::mb(8),
        buffer_pool_size: ByteSize::mb(4),
        submit_queue_size_threshold: ByteSize::mb(8),
        memory_eviction_policy: SplitRangeMemoryEvictionPolicy::S3Fifo,
        write_throughput: ByteSize::mib(500),
    }
}
