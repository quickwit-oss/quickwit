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

use bytes::Bytes;
use quickwit_config::DiskCacheConfig;

use crate::OwnedBytes;

/// Shared I/O concurrency for independent Feuer caches on the same disk.
/// Queues are created lazily: memory-only instances do not require io_uring.
#[derive(Clone, Default)]
pub struct FeuerIoQueues {
    #[cfg(target_os = "linux")]
    queues: std::sync::Arc<tokio::sync::OnceCell<feuer::IoQueues>>,
}

impl FeuerIoQueues {
    pub(crate) async fn open(
        &self,
        config: &DiskCacheConfig,
        file_name: &'static str,
        cache_name: &'static str,
        fixed_retrieval_equivalent_bytes: Option<u64>,
    ) -> anyhow::Result<feuer::TieredMemoryDiskCache> {
        #[cfg(target_os = "linux")]
        {
            let cache_config = feuer::CacheConfig::new(
                config.directory()?,
                config.disk_capacity.as_u64(),
                config.memory_capacity.as_u64(),
            )?
            .with_file_name(file_name)?;
            let scorer = std::sync::Arc::new(feuer::AvoidedRetrievalCostScorer {
                cost_per_request: fixed_retrieval_equivalent_bytes.unwrap_or_default() as f64,
                ..Default::default()
            });
            let registry: mixtrics::metrics::BoxedRegistry =
                Box::new(crate::split_range_cache::metrics::QuickwitMetricsRegistry { cache_name });
            let queues = if config.disk_capacity.as_u64() == 0 {
                None
            } else {
                Some(
                    self.queues
                        .get_or_try_init(|| async {
                            // Creating rings waits for their driver threads; keep this off Tokio
                            // workers.
                            Ok::<_, anyhow::Error>(
                                tokio::task::spawn_blocking(feuer::IoQueues::new).await??,
                            )
                        })
                        .await?
                        .clone(),
                )
            };
            // Share the retrieval-cost model between memory and disk, as before.
            Ok(feuer::TieredMemoryDiskCache::open_with_retention_scorer(
                cache_config,
                Some(&registry),
                queues,
                scorer,
            )
            .await?)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (
                config,
                file_name,
                cache_name,
                fixed_retrieval_equivalent_bytes,
            );
            anyhow::bail!("Feuer disk caches require Linux")
        }
    }
}

/// A Feuer instance storing complete, immutable encoded results.
/// Keys must include a format version; values for a given key must never change.
pub struct FeuerCache {
    cache: feuer::TieredMemoryDiskCache,
}

#[cfg(all(test, target_os = "linux"))]
#[path = "feuer_cache_tests.rs"]
mod tests;

impl FeuerCache {
    /// Open an independent cache with the supplied shared queues and explicit capacities.
    /// `file_name` must be distinct for caches in the same directory.
    /// `cache_name` identifies the cache in metrics; use a fixed name, not a path or request key.
    pub async fn open(
        config: &DiskCacheConfig,
        queues: &FeuerIoQueues,
        file_name: &'static str,
        cache_name: &'static str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            cache: queues.open(config, file_name, cache_name, None).await?,
        })
    }

    /// Retrieve an entire object from memory or disk, returning `None` on a miss.
    pub async fn get(&self, key: String) -> Option<OwnedBytes> {
        // Feuer exposes lookup-or-fetch rather than lookup alone. A callback error here means
        // a cache miss, not an I/O failure. No value is inserted on a miss.
        match self
            .cache
            .get_or_fetch_object(key, || async { Err::<Bytes, _>(()) })
            .await
        {
            Ok(bytes) => Some(crate::stable_deref_bytes::into_owned_bytes(bytes)),
            Err(feuer::GetOrFetchError::Callback(())) => None,
        }
    }

    /// Admit an immutable object; an existing value for this key is retained.
    /// Disk writes use Feuer's bounded, best-effort background queue.
    pub async fn put(&self, key: String, bytes: OwnedBytes) {
        self.cache
            .get_or_fetch_object(key, || async {
                Ok::<_, std::convert::Infallible>(Bytes::from_owner(bytes))
            })
            .await
            .expect("whole-object insertion must cover the complete object");
    }
}
