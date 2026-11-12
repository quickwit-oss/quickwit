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
#[cfg(all(test, target_os = "linux"))]
mod disk_tests;
pub(crate) mod metrics;
mod storage;
#[cfg(test)]
mod tests;

use quickwit_config::StorageCacheConfig;
pub use storage::wrap_storage_with_storage_cache;

use crate::{FeuerIoQueues, StorageResolver};

/// Optionally export native traces from an opened cache to `QW_FEUER_ACCESS_TRACE_URI`.
pub async fn attach_storage_cache_access_trace(
    cache: feuer::Cache,
    config: &StorageCacheConfig,
    resolver: &StorageResolver,
) -> anyhow::Result<feuer::Cache> {
    let destination = match std::env::var("QW_FEUER_ACCESS_TRACE_URI") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(cache),
        Err(error) => return Err(error.into()),
    };
    let mut trace_config = config.clone();
    trace_config.disk_capacity = bytesize::ByteSize::b(cache.config().disk_capacity());
    let (storage, run) = access_trace::open_run(&destination, resolver, &trace_config).await?;
    Ok(access_trace::attach(cache, storage, run))
}

/// Open `storage_cache` synchronously with shared disk I/O queues.
/// Memory-only caches need no directory or runtime.
/// Disk initialization blocks and requires an entered Tokio runtime.
/// Model each source request's fixed retrieval cost as 256,000 equivalent bytes.
pub fn open_storage_cache(
    config: &StorageCacheConfig,
    queues: &FeuerIoQueues,
) -> anyhow::Result<feuer::Cache> {
    let scorer = std::sync::Arc::new(feuer::FetchCostScorer {
        cost_per_request: 256_000.0,
        ..Default::default()
    });
    queues.open(config, "storage_cache", "storage", scorer, 1)
}

#[cfg(test)]
#[test]
fn test_memory_only_storage_cache_needs_no_directory_or_runtime() {
    let config = StorageCacheConfig {
        directory: None,
        disk_capacity: bytesize::ByteSize::b(0),
        memory_capacity: bytesize::ByteSize::mb(1),
    };
    open_storage_cache(&config, &FeuerIoQueues::default()).unwrap();
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn config_for_test(path: impl AsRef<std::path::Path>) -> StorageCacheConfig {
    StorageCacheConfig {
        directory: Some(path.as_ref().to_path_buf()),
        disk_capacity: bytesize::ByteSize::mib(512),
        memory_capacity: bytesize::ByteSize::mib(8),
    }
}
