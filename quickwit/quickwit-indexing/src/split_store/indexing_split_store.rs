// Copyright (C) 2024 Quickwit, Inc.
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

#[cfg(any(test, feature = "testsuite"))]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
#[cfg(any(test, feature = "testsuite"))]
use bytesize::ByteSize;
use quickwit_common::io::{IoControls, IoControlsAccess};
use quickwit_common::uri::Uri;
use quickwit_metastore::SplitMetadata;
use quickwit_storage::{PutPayload, Storage, StorageResult};
use tantivy::directory::{Advice, MmapDirectory};
use tantivy::Directory;
use time::OffsetDateTime;
use tracing::{debug, info_span, instrument, Instrument};

use super::IndexingSplitCache;
use crate::get_tantivy_directory_from_split_bundle;

/// IndexingSplitStore is a wrapper around a regular `Storage` to upload and
/// download splits while allowing for efficient caching.
///
/// We typically index with a limited amount of RAM or some constraints on the
/// expected time-to-search.
/// Because of these constraints, the indexer produces splits that are smaller
/// than optimal and need to be merged.
///
/// A split therefore typically undergoes a few merges relatively shortly after
/// its creation.
///
/// In order to alleviate the disk IO as well as the network bandwidth,
/// we save new splits into a split store.
///
/// The role of the `IndexingSplitStore` is to combine a cache and a storage
/// to avoid unnecessary download of fresh splits. Its behavior are however very different
/// from a usual cache as we have a strong knowledge of the split lifecycle.
///
/// The splits are stored on the local filesystem in the `IndexingSplitCache`.
#[derive(Clone)]
pub struct IndexingSplitStore {
    inner: Arc<InnerIndexingSplitStore>,
}

struct InnerIndexingSplitStore {
    /// The remote storage.
    remote_storage: Arc<dyn Storage>,
    split_cache: Arc<IndexingSplitCache>,
}

impl IndexingSplitStore {
    /// Creates an instance of [`IndexingSplitStore`]
    ///
    /// It needs the remote storage to work with.
    pub fn new(remote_storage: Arc<dyn Storage>, split_cache: Arc<IndexingSplitCache>) -> Self {
        let inner = InnerIndexingSplitStore {
            remote_storage,
            split_cache,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Helper function to create a indexing split store for tests.
    /// The resulting store does not have any local cache.
    pub fn create_without_local_store_for_test(remote_storage: Arc<dyn Storage>) -> Self {
        let inner = InnerIndexingSplitStore {
            remote_storage,
            split_cache: Arc::new(IndexingSplitCache::no_caching()),
        };
        IndexingSplitStore {
            inner: Arc::new(inner),
        }
    }

    pub fn remote_uri(&self) -> &Uri {
        self.inner.remote_storage.uri()
    }

    fn split_path(&self, split_id: &str) -> PathBuf {
        PathBuf::from(quickwit_common::split_file(split_id))
    }

    /// Stores a split.
    ///
    /// If a split is identified as mature by the merge policy,
    /// it will not be cached into the local storage.
    ///
    /// In order to limit the write IO, the file might be moved (and not copied into
    /// the store).
    /// In other words, after calling this function the file will not be available
    /// at `split_folder` anymore.
    #[instrument("store_split", skip_all)]
    pub async fn store_split(
        &self,
        split: &SplitMetadata,
        split_folder_path: &Path,
        put_payload: Box<dyn PutPayload>,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        let split_num_bytes = put_payload.len();

        let key = self.split_path(split.split_id());
        let is_mature = split.is_mature(OffsetDateTime::now_utc());
        self.inner
            .remote_storage
            .put(&key, put_payload)
            .instrument(info_span!("store_split_in_remote_storage", split=?split.split_id(), is_mature=is_mature, num_bytes=split_num_bytes))
            .await
            .with_context(|| {
                format!(
                    "failed uploading key {} in bucket {}",
                    key.display(),
                    self.inner.remote_storage.uri()
                )
            })?;

        let elapsed_secs = start.elapsed().as_secs_f32();
        let split_size_in_megabytes = split_num_bytes as f32 / 1_000_000f32;
        let throughput_mb_s = split_size_in_megabytes / elapsed_secs;

        debug!(
            split_size_in_megabytes = %split_size_in_megabytes,
            num_docs = %split.num_docs,
            elapsed_secs = %elapsed_secs,
            throughput_mb_s = %throughput_mb_s,
            is_mature = is_mature,
            "store-split-remote-success"
        );

        if !is_mature {
            debug!("store-in-cache");
            if self
                .inner
                .split_cache
                .move_into_cache(split.split_id(), split_folder_path)
                .await?
            {
                return Ok(());
            }
        }
        tokio::fs::remove_dir_all(split_folder_path).await?;
        Ok(())
    }

    /// Gets a split from the split store, and makes it available to the given `output_path`.
    /// If the split is available in the local disk cache, then it will be moved
    /// from the cache to the `output_dir_path`.
    ///
    /// The output_path is expected to be a directory path.
    ///
    /// If not, it will be fetched from the remote `Storage`.
    ///
    /// # Implementation detail:
    ///
    /// Depending on whether the split was obtained from the `Storage`
    /// or the cache, it could consist in a directly or a proper split file.
    /// This method takes care of the dealing with opening the split correctly.
    ///
    /// As we fetch the split, we optimistically assume that this is for a merge
    /// operation that will be successful and we remove the split from the cache.
    #[instrument(skip(self, output_dir_path, io_controls), fields(cache_hit))]
    pub async fn fetch_and_open_split(
        &self,
        split_id: &str,
        output_dir_path: &Path,
        io_controls: &IoControls,
    ) -> StorageResult<Box<dyn Directory>> {
        let path = PathBuf::from(quickwit_common::split_file(split_id));
        if let Some(split_path) = self
            .inner
            .split_cache
            .get_cached_split(split_id, output_dir_path)
            .await?
        {
            tracing::Span::current().record("cache_hit", true);
            let mmap_directory: Box<dyn Directory> = Box::new(MmapDirectory::open_with_madvice(
                split_path,
                Advice::Sequential,
            )?);
            return Ok(mmap_directory);
        } else {
            tracing::Span::current().record("cache_hit", false);
        }
        let dest_filepath = output_dir_path.join(&path);
        let dest_file = tokio::fs::File::create(&dest_filepath).await?;
        let mut dest_file_with_write_limit = io_controls.clone().wrap_write(dest_file);
        self.inner
            .remote_storage
            .copy_to(&path, &mut dest_file_with_write_limit)
            .instrument(info_span!("fetch_split_from_remote_storage", path=?path))
            .await?;
        get_tantivy_directory_from_split_bundle(&dest_filepath)
    }

    /// Takes a snapshot of the cache view (only used for testing).
    #[cfg(any(test, feature = "testsuite"))]
    pub async fn inspect_split_cache(&self) -> HashMap<String, ByteSize> {
        self.inner.split_cache.inspect().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytesize::ByteSize;
    use quickwit_common::io::IoControls;
    use quickwit_metastore::{SplitMaturity, SplitMetadata};
    use quickwit_storage::{RamStorage, SplitPayloadBuilder};
    use tempfile::tempdir;
    use time::OffsetDateTime;
    use tokio::fs;
    use ulid::Ulid;

    use super::IndexingSplitStore;
    use crate::split_store::{IndexingSplitCache, SplitStoreQuota};

    fn create_test_split_metadata(split_id: &str) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            maturity: SplitMaturity::Immature {
                maturation_period: Duration::from_secs(3600),
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_local_store_cache_in_and_out() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let split_cache_dir = tempdir()?;

        let split_cache = IndexingSplitCache::open(
            split_cache_dir.path().to_path_buf(),
            SplitStoreQuota::default(),
        )
        .await?;
        let remote_storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::new(remote_storage, Arc::new(split_cache));

        let split_id1 = Ulid::new().to_string();
        let split_id2 = Ulid::new().to_string();

        {
            let split1_dir = temp_dir.path().join(&split_id1);
            fs::create_dir_all(&split1_dir).await?;
            let split_metadata1 = create_test_split_metadata(&split_id1);
            fs::write(split1_dir.join("splitfile"), b"1234").await?;
            split_store
                .store_split(&split_metadata1, &split1_dir, Box::new(b"1234".to_vec()))
                .await?;
            assert!(!split1_dir.try_exists()?);
            assert!(split_cache_dir
                .path()
                .join(format!("{split_id1}.split"))
                .try_exists()?);
            let local_store_stats = split_store.inspect_split_cache().await;
            assert_eq!(local_store_stats.len(), 1);
            assert_eq!(
                local_store_stats.get(&split_id1).cloned(),
                Some(ByteSize(4))
            );
        }
        {
            let split2_dir = temp_dir.path().join(&split_id2);
            fs::create_dir_all(&split2_dir).await?;
            fs::write(split2_dir.join("splitfile"), b"567").await?;
            let split_metadata2 = create_test_split_metadata(&split_id2);
            split_store
                .store_split(&split_metadata2, &split2_dir, Box::new(b"567".to_vec()))
                .await?;
            assert!(!split2_dir.try_exists()?);
            assert!(split_cache_dir
                .path()
                .join(format!("{split_id2}.split"))
                .try_exists()?);
        }

        let local_store_stats = split_store.inspect_split_cache().await;
        assert_eq!(local_store_stats.len(), 2);
        assert_eq!(
            local_store_stats.get(&split_id1).cloned(),
            Some(ByteSize(4))
        );
        assert_eq!(
            local_store_stats.get(&split_id2).cloned(),
            Some(ByteSize(3))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_put_should_not_store_in_cache_when_max_num_files_reached() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;

        let split_cache_dir = tempdir()?;
        let split_cache = IndexingSplitCache::open(
            split_cache_dir.path().to_path_buf(),
            SplitStoreQuota::new(1, ByteSize::mb(1)),
        )
        .await?;

        let remote_storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::new(remote_storage, Arc::new(split_cache));

        let split_id1 = Ulid::new().to_string();
        let split_id2 = Ulid::new().to_string();

        {
            let split_path = temp_dir.path().join(&split_id1);
            fs::create_dir_all(&split_path).await?;
            fs::write(split_path.join("splitdatafile"), b"hello-world").await?;
            let split_metadata1 = create_test_split_metadata(&split_id1);
            split_store
                .store_split(
                    &split_metadata1,
                    &split_path,
                    Box::new(SplitPayloadBuilder::get_split_payload(
                        &[],
                        &[],
                        &[5, 5, 5],
                    )?),
                )
                .await?;
            assert!(!split_path.try_exists()?);
            assert!(split_cache_dir
                .path()
                .join(format!("{split_id1}.split"))
                .try_exists()?);
            let split_cache_stats = split_store.inspect_split_cache().await;
            assert_eq!(split_cache_stats.len(), 1);
            assert_eq!(
                split_cache_stats.get(&split_id1).cloned(),
                Some(ByteSize(11))
            );
        }
        {
            let split_path = temp_dir.path().join(&split_id2);
            fs::create_dir_all(&split_path).await?;
            fs::write(split_path.join("splitdatafile2"), b"hello-world2").await?;
            let split_metadata2 = create_test_split_metadata(&split_id2);

            split_store
                .store_split(
                    &split_metadata2,
                    &split_path,
                    Box::new(SplitPayloadBuilder::get_split_payload(
                        &[],
                        &[],
                        &[5, 5, 5],
                    )?),
                )
                .await?;
            assert!(!split_path.try_exists()?);
            assert!(split_cache_dir
                .path()
                .join(format!("{split_id2}.split"))
                .try_exists()?);
            let split_cache_stats = split_store.inspect_split_cache().await;
            assert_eq!(split_cache_stats.len(), 1);
            assert_eq!(
                split_cache_stats.get(&split_id2).cloned(),
                Some(ByteSize(12))
            );
        }
        {
            let output = tempfile::tempdir()?;
            let io_controls = IoControls::default();
            // get from cache
            let _split1 = split_store
                .fetch_and_open_split(&split_id1, output.path(), &io_controls)
                .await?;
            // get from remote storage
            let _split2 = split_store
                .fetch_and_open_split(&split_id2, output.path(), &io_controls)
                .await?;
        }
        Ok(())
    }
}
