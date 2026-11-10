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

use std::hash::Hash;
use std::io;
use std::path::Path;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::Mutex;

use bytes::Bytes;
use mixtrics::metrics::BoxedRegistry;
use quickwit_config::StorageCacheConfig;
use quickwit_metrics::{LazyGauge, gauge, lazy_gauge};

use crate::OwnedBytes;
use crate::stable_deref_bytes::into_owned_bytes;
use crate::storage_cache::metrics::QuickwitMetricsRegistry;

static DISK_CACHE_STARTUP_FALLBACK: LazyGauge = lazy_gauge!(
    name: "disk_cache_startup_fallback",
    description: "Whether a cache is using memory only after disk initialization failed (1) or not (0).",
    subsystem: "storage",
);

/// Shared I/O concurrency for independent Feuer caches on the same disk.
/// Queues are created lazily: memory-only instances do not require io_uring.
#[derive(Clone, Default)]
pub struct FeuerIoQueues {
    #[cfg(target_os = "linux")]
    queues: Arc<Mutex<Option<feuer::IoQueues>>>,
}

impl FeuerIoQueues {
    /// Disk initialization failures fall back to memory only with the same capacity.
    /// Configuration errors still propagate to the caller.
    pub(crate) fn open(
        &self,
        config: &StorageCacheConfig,
        file_name: &'static str,
        cache_name: &'static str,
        fixed_retrieval_equivalent_bytes: u64,
    ) -> anyhow::Result<feuer::Cache> {
        // Feuer checks disk capacity only when opening disk files. Validate it before
        // attempting I/O so an unavailable disk cannot hide invalid settings.
        let disk_capacity = config.disk_capacity.as_u64();
        anyhow::ensure!(
            disk_capacity == 0 || (2 * 1024 * 1024..=i64::MAX as u64).contains(&disk_capacity),
            "disk cache capacity must be zero or between 2 MiB and i64::MAX bytes"
        );
        let result = self.open_cache(
            config,
            file_name,
            cache_name,
            fixed_retrieval_equivalent_bytes,
        );
        let (cache, fallback) = match result {
            Ok(cache) => (cache, false),
            Err(error)
                if disk_capacity > 0
                    && (matches!(
                        error.downcast_ref::<feuer::CacheOpenError>(),
                        Some(feuer::CacheOpenError::Disk(_))
                    ) || error.is::<io::Error>()) =>
            {
                let mut memory_config = config.clone();
                memory_config.disk_capacity = bytesize::ByteSize::b(0);
                let cache = self.open_cache(
                    &memory_config,
                    file_name,
                    cache_name,
                    fixed_retrieval_equivalent_bytes,
                )?;
                tracing::warn!(
                    cache = cache_name,
                    directory = ?config.directory,
                    error = ?error,
                    "disk cache initialization failed; using memory-only caching until restart"
                );
                (cache, true)
            }
            Err(error) => return Err(error),
        };
        gauge!(parent: DISK_CACHE_STARTUP_FALLBACK, "cache" => cache_name).set(if fallback {
            1.0
        } else {
            0.0
        });
        Ok(cache)
    }

    fn open_cache(
        &self,
        config: &StorageCacheConfig,
        file_name: &'static str,
        cache_name: &'static str,
        fixed_retrieval_equivalent_bytes: u64,
    ) -> anyhow::Result<feuer::Cache> {
        let directory = if config.disk_capacity.as_u64() == 0 {
            Path::new("")
        } else {
            config.directory()?
        };
        let scorer = Arc::new(feuer::FetchCostScorer {
            cost_per_request: fixed_retrieval_equivalent_bytes as f64,
            ..Default::default()
        });
        let registry: BoxedRegistry = Box::new(QuickwitMetricsRegistry { cache_name });
        let builder = feuer::Cache::builder()
            .directory(directory)
            .disk_capacity(config.disk_capacity.as_u64())
            .memory_capacity(config.memory_capacity.as_u64())
            .file_name(file_name)
            .payload_version(1)
            .with_metrics(&registry)
            .with_memory_retention_scorer(scorer.clone())
            .with_disk_retention_scorer(scorer);
        #[cfg(target_os = "linux")]
        let builder = if config.disk_capacity.as_u64() == 0 {
            builder
        } else {
            let mut queues = self.queues.lock().unwrap();
            if queues.is_none() {
                *queues = Some(feuer::IoQueues::new()?);
            }
            builder.with_io_queues(queues.as_ref().unwrap().clone())
        };
        Ok(builder.open()?)
    }
}

/// A Feuer cache for immutable encoded results.
/// Values for a given key must never change. Each instance is a separate key namespace.
/// Keys must hash all identity fields. Only their 128-bit fingerprints are retained.
pub struct FeuerCache {
    cache: Option<feuer::Cache>,
}

impl FeuerCache {
    /// Open a memory-only instance without a Tokio runtime. Zero capacity disables caching.
    pub fn memory(capacity: u64, cache_name: &'static str) -> anyhow::Result<Self> {
        if capacity == 0 {
            return Ok(Self { cache: None });
        }
        let config = StorageCacheConfig {
            directory: None,
            disk_capacity: bytesize::ByteSize::b(0),
            memory_capacity: bytesize::ByteSize::b(capacity),
        };
        Self::open(&config, &FeuerIoQueues::default(), cache_name, cache_name)
    }

    /// Open a cache synchronously with the supplied disk I/O queues.
    /// Disk initialization blocks and requires an entered Tokio runtime.
    /// `file_name` must be distinct for caches in the same directory.
    /// `cache_name` identifies the cache in metrics; use a fixed name, not a path or request key.
    pub fn open(
        config: &StorageCacheConfig,
        queues: &FeuerIoQueues,
        file_name: &'static str,
        cache_name: &'static str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            cache: Some(queues.open(config, file_name, cache_name, 0)?),
        })
    }

    /// Retrieve an entire object from memory or disk, returning `None` on a miss.
    pub async fn get(&self, key: &(impl Hash + ?Sized)) -> Option<OwnedBytes> {
        self.cache.as_ref()?.get(key).await.map(into_owned_bytes)
    }

    /// Cache an immutable object without disk reads or waiting for disk admission.
    /// Values for a key must be equivalent. An existing memory entry is retained.
    /// Disk writes use Feuer's best-effort background queue.
    pub fn put(&self, key: &(impl Hash + ?Sized), bytes: OwnedBytes) {
        if let Some(cache) = &self.cache {
            cache.insert(key, Bytes::from_owner(bytes));
        }
    }
}

#[cfg(test)]
mod startup_tests {
    use bytesize::ByteSize;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    #[tokio::test]
    async fn test_disk_failure_falls_back_to_memory() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = ::metrics::set_default_local_recorder(&recorder);
        let directory = tempfile::tempdir().unwrap();
        let blocked = directory.path().join("file");
        std::fs::write(&blocked, b"keep this file").unwrap();
        let config = StorageCacheConfig {
            directory: Some(blocked.clone()),
            disk_capacity: ByteSize::mib(8),
            memory_capacity: ByteSize::mib(1),
        };
        let queues = FeuerIoQueues::default();
        for cache_name in ["storage", "partial_request", "predicate"] {
            let cache = queues.open(&config, cache_name, cache_name, 0).unwrap();
            assert_eq!(cache.config().disk_capacity(), 0);
            assert_eq!(
                cache.config().memory_capacity(),
                config.memory_capacity.as_u64()
            );
            cache.insert("key", Bytes::from_static(b"value"));
            assert_eq!(cache.get("key").await.unwrap().as_ref(), b"value");
            let fallback = snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .find(|(key, _, _, _)| {
                    key.key().name() == "quickwit_storage_disk_cache_startup_fallback"
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "cache" && label.value() == cache_name)
                })
                .expect("fallback metric must identify the affected cache");
            assert_eq!(fallback.3, DebugValue::Gauge(1.0.into()));
        }
        assert_eq!(std::fs::read(blocked).unwrap(), b"keep this file");
        assert_eq!(config.disk_capacity, ByteSize::mib(8));
    }

    #[tokio::test]
    async fn test_invalid_configuration_still_fails() {
        let queues = FeuerIoQueues::default();
        let mut config = StorageCacheConfig {
            directory: None,
            disk_capacity: ByteSize::mib(8),
            memory_capacity: ByteSize::mib(1),
        };
        let error = queues.open(&config, "cache", "storage", 0).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("storage_cache_directory must be set")
        );

        let directory = tempfile::tempdir().unwrap();
        config.directory = Some(directory.path().to_path_buf());
        for disk_capacity in [1, 2 * 1024 * 1024 - 1, u64::MAX] {
            config.disk_capacity = ByteSize::b(disk_capacity);
            let error = queues.open(&config, "cache", "storage", 0).unwrap_err();
            assert!(error.to_string().contains("disk cache capacity must be"));
        }
        config.disk_capacity = ByteSize::mib(8);
        config.memory_capacity = ByteSize::b(0);
        let error = queues.open(&config, "cache", "storage", 0).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<feuer::CacheOpenError>(),
            Some(feuer::CacheOpenError::Config(_))
        ));
    }

    #[test]
    fn test_memory_only_cache_reports_no_fallback_without_runtime() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = ::metrics::set_default_local_recorder(&recorder);
        FeuerCache::memory(1024, "storage").unwrap();
        let fallback = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "quickwit_storage_disk_cache_startup_fallback"
                    && key.key().labels().any(|label| label.key() == "cache")
            })
            .unwrap();
        assert_eq!(fallback.3, DebugValue::Gauge(0.0.into()));
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::path::PathBuf;

    use bytesize::ByteSize;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;
    use crate::open_storage_cache;

    #[tokio::test]
    async fn test_memory_only_instance_does_not_initialize_io_queues() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config(directory.path().join("unused"), 8);
        config.disk_capacity = ByteSize::b(0);
        let queues = FeuerIoQueues::default();
        let cache = FeuerCache::open(
            &config,
            &queues,
            "partial_response_cache",
            "partial_request",
        )
        .unwrap();
        cache.put("key", OwnedBytes::new(&b"value"[..]));
        assert_eq!(cache.get("key").await.unwrap().as_slice(), b"value");
        assert!(queues.queues.lock().unwrap().is_none());
        assert!(!config.directory().unwrap().exists());
    }

    #[tokio::test]
    async fn test_instances_share_queues_but_keep_contents_and_capacities_separate() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = ::metrics::set_default_local_recorder(&recorder);
        let directory = tempfile::tempdir().unwrap();
        let queues = FeuerIoQueues::default();
        let storage_config = config(directory.path().to_path_buf(), 8);
        let partial_config = config(directory.path().to_path_buf(), 16);
        let predicate_config = config(directory.path().to_path_buf(), 32);
        let storage = open_storage_cache(&storage_config, &queues.clone()).unwrap();
        assert!(queues.queues.lock().unwrap().is_some());
        let partial = FeuerCache::open(
            &partial_config,
            &queues,
            "partial_response_cache",
            "partial_request",
        )
        .unwrap();
        let predicate =
            FeuerCache::open(&predicate_config, &queues, "predicate_cache", "predicate").unwrap();
        let capacities: HashMap<_, _> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == "feuer_memory_capacity_bytes")
            .filter_map(|(key, _, _, value)| {
                let Some(cache_label) = key.key().labels().find(|label| label.key() == "cache")
                else {
                    // Typed metric parents are registered without observations.
                    assert_eq!(value, DebugValue::Gauge(0.0.into()));
                    return None;
                };
                Some((cache_label.value().to_owned(), value))
            })
            .collect();
        assert_eq!(capacities.len(), 3);
        for (cache_name, config) in [
            ("storage", &storage_config),
            ("partial_request", &partial_config),
            ("predicate", &predicate_config),
        ] {
            assert_eq!(
                capacities[cache_name],
                DebugValue::Gauge((config.memory_capacity.as_u64() as f64).into())
            );
        }
        for (cache, config, file_name) in [
            (&partial, &partial_config, "partial_response_cache"),
            (&predicate, &predicate_config, "predicate_cache"),
        ] {
            let cache_config = cache.cache.as_ref().unwrap().config();
            assert_eq!(cache_config.disk_capacity(), config.disk_capacity.as_u64());
            assert_eq!(
                cache_config.memory_capacity(),
                config.memory_capacity.as_u64()
            );
            assert_eq!(cache_config.file_name(), file_name);
            assert!(cache.get("missing").await.is_none());
        }
        storage
            .get_or_fetch("same-key", || async {
                Ok::<_, Infallible>(Bytes::from_static(b"storage"))
            })
            .await
            .unwrap();
        partial.put("same-key", OwnedBytes::new(&b"partial"[..]));
        predicate.put("same-key", OwnedBytes::new(&b"predicate"[..]));
        assert_eq!(storage.get("same-key").await.unwrap().as_ref(), b"storage");
        assert_eq!(
            partial.get("same-key").await.unwrap().as_slice(),
            b"partial"
        );
        assert_eq!(
            predicate.get("same-key").await.unwrap().as_slice(),
            b"predicate"
        );
    }

    fn config(path: PathBuf, disk_mib: u64) -> StorageCacheConfig {
        StorageCacheConfig {
            directory: Some(path),
            disk_capacity: ByteSize::mib(disk_mib),
            memory_capacity: ByteSize::mib(disk_mib / 8),
        }
    }
}
