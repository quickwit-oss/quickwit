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
#[cfg(all(test, target_os = "linux"))]
mod tests;

use quickwit_config::StorageCacheConfig;
pub use storage::{FeuerSplitRangeStorage, wrap_storage_with_split_range_cache};

use crate::{FeuerIoQueues, StorageResolver};

/// Open the split-range cache and optionally export native traces to `QW_FEUER_ACCESS_TRACE_URI`.
pub async fn open_split_range_cache_with_access_trace(
    config: &StorageCacheConfig,
    resolver: &StorageResolver,
    queues: &FeuerIoQueues,
) -> anyhow::Result<feuer::Cache> {
    let cache = open_split_range_cache(config, queues).await?;
    let destination = match std::env::var("QW_FEUER_ACCESS_TRACE_URI") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(cache),
        Err(error) => return Err(error.into()),
    };
    let (storage, run) = access_trace::open_run(&destination, resolver, config).await?;
    Ok(access_trace::attach(cache, storage, run))
}

/// Open `split_cache` with shared disk I/O queues.
/// Memory-only caches need no directory.
/// Model each source request's fixed retrieval cost as 256,000 equivalent bytes.
pub async fn open_split_range_cache(
    config: &StorageCacheConfig,
    queues: &FeuerIoQueues,
) -> anyhow::Result<feuer::Cache> {
    queues
        .open(config, "split_cache", "split_range", 256_000)
        .await
}

#[cfg(test)]
#[test]
fn test_memory_only_range_cache_needs_no_directory_or_runtime() {
    let config = StorageCacheConfig {
        path: None,
        disk_capacity: bytesize::ByteSize::b(0),
        memory_capacity: bytesize::ByteSize::mb(1),
    };
    futures::executor::block_on(open_split_range_cache(&config, &FeuerIoQueues::default()))
        .unwrap();
}

#[cfg(test)]
pub(crate) fn buffer_for_test(bytes: impl AsRef<[u8]>) -> feuer::AlignedBuffer {
    let bytes = bytes.as_ref();
    let mut buffer = feuer::AlignedBuffer::allocate_zeroed(bytes.len()).unwrap();
    buffer.as_mut_slice().copy_from_slice(bytes);
    buffer
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn config_for_test(path: impl AsRef<std::path::Path>) -> StorageCacheConfig {
    StorageCacheConfig {
        path: Some(path.as_ref().to_path_buf()),
        disk_capacity: bytesize::ByteSize::mib(512),
        memory_capacity: bytesize::ByteSize::mib(8),
    }
}
