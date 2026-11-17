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

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tokio::io::AsyncRead;

use crate::cache::StorageCache;
use crate::storage::SendableAsync;
use crate::{BulkDeleteError, ListObjectsStream, OwnedBytes, Storage, StorageResult};

/// Use with care, StorageWithCache is read-only.
/// Batch reads with more than 100 ranges bypass cache lookups and insertion.
pub struct StorageWithCache {
    pub storage: Arc<dyn Storage>,
    pub cache: Arc<dyn StorageCache>,
}

impl fmt::Debug for StorageWithCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageWithCache").finish()
    }
}

#[async_trait]
impl Storage for StorageWithCache {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check_connectivity().await
    }

    async fn put(
        &self,
        path: &Path,
        _payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        unimplemented!("StorageWithCache is readonly. Failed to put {:?}", path)
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        self.storage.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> StorageResult<OwnedBytes> {
        if let Some(bytes) = self.cache.get(path, byte_range.clone()).await {
            Ok(bytes)
        } else {
            let bytes = self.storage.get_slice(path, byte_range.clone()).await?;
            self.cache
                .put(path.to_owned(), byte_range, bytes.clone())
                .await;
            Ok(bytes)
        }
    }

    async fn get_slice_into(
        &self,
        path: &Path,
        range: Range<usize>,
        buffer: &mut [u8],
    ) -> StorageResult<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        // Read through the cache. Streaming would bypass it.
        let bytes = self.get_slice(path, range).await?;
        buffer.copy_from_slice(&bytes);
        Ok(())
    }

    async fn get_slices(
        &self,
        path: &Path,
        requested_ranges: Vec<Range<usize>>,
    ) -> StorageResult<Vec<OwnedBytes>> {
        let skip_cache =
            requested_ranges.len() > super::batch_read::MAX_NUM_RANGES_FOR_CACHE_LOOKUP;
        super::read_batch_with_async_cache(
            requested_ranges,
            |requested_range| self.cache.get(path, requested_range),
            |ranges_to_fetch| async move {
                // Large batches bypass insertion as well as lookups to avoid creating an entry
                // for every requested range. The per-query warmup cache still retains the data.
                if skip_cache {
                    return self.storage.get_slices(path, ranges_to_fetch).await;
                }
                let downloaded = self
                    .storage
                    .get_slices(path, ranges_to_fetch.clone())
                    .await?;
                assert_eq!(downloaded.len(), ranges_to_fetch.len());
                for (missing_range, bytes) in ranges_to_fetch.into_iter().zip(&downloaded) {
                    self.cache
                        .put(path.to_owned(), missing_range, bytes.clone())
                        .await;
                }
                Ok(downloaded)
            },
        )
        .await
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        _range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        unimplemented!(
            "StorageWithCache does not support streamed read yet. Failed to get {:?}",
            path
        )
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        if let Some(bytes) = self.cache.get_all(path).await {
            Ok(bytes)
        } else {
            let bytes = self.storage.get_all(path).await?;
            self.cache.put_all(path.to_owned(), bytes.clone()).await;
            Ok(bytes)
        }
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        unimplemented!("Failed to delete file `{path:?}`. `StorageWithCache` is read-only.")
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        unimplemented!("Failed to delete files `{paths:?}`. `StorageWithCache` is read-only.")
    }

    fn list(&self, prefix: &Path) -> ListObjectsStream {
        self.storage.list(prefix)
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.storage.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;
    use crate::{MockStorage, MockStorageCache, OwnedBytes};

    #[tokio::test(start_paused = true)]
    async fn test_cache_lookups_overlap_and_forward_all_misses_together() {
        use std::time::Duration;

        struct DelayedCache {
            bytes_by_range: Mutex<HashMap<Range<usize>, OwnedBytes>>,
        }

        #[async_trait]
        impl StorageCache for DelayedCache {
            async fn get(&self, _: &Path, range: Range<usize>) -> Option<OwnedBytes> {
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.bytes_by_range.lock().unwrap().get(&range).cloned()
            }

            async fn put(&self, _: PathBuf, range: Range<usize>, bytes: OwnedBytes) {
                assert_eq!(range.start % 2, 1);
                assert_eq!(bytes.as_slice(), &[range.start as u8]);
                self.bytes_by_range.lock().unwrap().insert(range, bytes);
            }

            async fn get_all(&self, _: &Path) -> Option<OwnedBytes> {
                unreachable!()
            }

            async fn put_all(&self, _: PathBuf, _: OwnedBytes) {
                unreachable!()
            }
        }

        let mut backend = MockStorage::default();
        backend.expect_get_slices().times(1).returning(|_, ranges| {
            assert_eq!(
                ranges,
                (1..64)
                    .step_by(2)
                    .map(|start| start..start + 1)
                    .collect::<Vec<_>>()
            );
            Ok(ranges
                .into_iter()
                .map(|range| OwnedBytes::new(vec![range.start as u8]))
                .collect())
        });
        let storage = StorageWithCache {
            storage: Arc::new(backend),
            cache: Arc::new(DelayedCache {
                bytes_by_range: Mutex::new(
                    (0..64)
                        .step_by(2)
                        .map(|start| (start..start + 1, OwnedBytes::new(vec![start as u8])))
                        .collect(),
                ),
            }),
        };
        let ranges: Vec<_> = (0..64).map(|index| index..index + 1).collect();
        let start = tokio::time::Instant::now();
        let buffers = storage
            .get_slices(Path::new("file"), ranges.clone())
            .await
            .unwrap();
        // 64 delayed lookups run in two waves of 32, then all misses are forwarded together.
        assert_eq!(start.elapsed(), Duration::from_millis(20));
        assert_eq!(buffers.len(), 64);
        for (index, buffer) in buffers.iter().enumerate() {
            assert_eq!(buffer.as_slice(), &[index as u8]);
        }
        // Repeating the batch uses the inserted bytes without another storage read.
        let repeated = storage.get_slices(Path::new("file"), ranges).await.unwrap();
        assert_eq!(repeated, buffers);
    }

    #[tokio::test]
    async fn test_batch_cache_limit() {
        for num_ranges in [100, 101] {
            let cache_operations = if num_ranges == 100 { 100 } else { 0 };
            let mut cache = MockStorageCache::default();
            cache
                .expect_get()
                .times(cache_operations)
                .returning(|_, _| None);
            cache
                .expect_put()
                .times(cache_operations)
                .returning(|_, range, bytes| {
                    assert_eq!(bytes.as_slice(), &[range.start as u8]);
                });
            let mut backend = MockStorage::default();
            backend
                .expect_get_slices()
                .times(1)
                .returning(move |_, ranges| {
                    assert_eq!(
                        ranges,
                        (0..num_ranges)
                            .map(|index| index..index + 1)
                            .collect::<Vec<_>>()
                    );
                    Ok(ranges
                        .into_iter()
                        .map(|range| OwnedBytes::new(vec![range.start as u8]))
                        .collect())
                });
            let storage = StorageWithCache {
                storage: Arc::new(backend),
                cache: Arc::new(cache),
            };
            let ranges: Vec<_> = (0..num_ranges).map(|index| index..index + 1).collect();
            let buffers = storage.get_slices(Path::new("file"), ranges).await.unwrap();
            assert_eq!(buffers.len(), num_ranges);
            for (index, buffer) in buffers.iter().enumerate() {
                assert_eq!(buffer.as_slice(), &[index as u8]);
            }
        }
    }

    #[tokio::test]
    async fn put_in_cache_test() {
        let mut mock_storage = MockStorage::default();
        let mut mock_cache = MockStorageCache::default();
        let actual_cache: Arc<Mutex<HashMap<PathBuf, OwnedBytes>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let cache1 = actual_cache.clone();
        mock_cache
            .expect_get_all()
            .times(2)
            .returning(move |path| cache1.lock().unwrap().get(path).cloned());
        mock_cache
            .expect_put_all()
            .times(1)
            .returning(move |path, data| {
                let actual_cache = actual_cache.clone();
                actual_cache.lock().unwrap().insert(path, data);
            });

        mock_storage
            .expect_get_all()
            .times(1)
            .returning(|_path| Ok(OwnedBytes::new(vec![1, 2, 3])));

        let storage_with_cache = StorageWithCache {
            storage: Arc::new(mock_storage),
            cache: Arc::new(mock_cache),
        };

        let data1 = storage_with_cache
            .get_all(Path::new("cool_file"))
            .await
            .unwrap();
        // hitting the cache
        let data2 = storage_with_cache
            .get_all(Path::new("cool_file"))
            .await
            .unwrap();
        assert_eq!(data1, data2);
    }
}
