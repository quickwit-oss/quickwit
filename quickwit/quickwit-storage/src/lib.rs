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

#![warn(missing_docs)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::len_without_is_empty)]
#![deny(clippy::disallowed_methods)]

//! `quickwit-storage` is the abstraction used in quickwit to interface itself
//! to different storage:
//! - object storages (S3)
//! - local filesystem
//! - distributed filesystems.
//! - etc.
//!
//! The `BundleStorage` bundles together multiple files into a single file.
mod cache;
mod debouncer;
mod file_descriptor_cache;
mod metrics;
mod storage;
mod timeout_and_retry_storage;
pub use debouncer::AsyncDebouncer;
pub(crate) use debouncer::DebouncedStorage;

pub use self::metrics::STORAGE_METRICS;
pub use self::payload::PutPayload;
pub use self::storage::Storage;

mod bundle_storage;
mod error;

mod local_file_storage;
mod object_storage;
#[cfg(feature = "gcs")]
mod opendal_storage;
mod payload;
mod prefix_storage;
mod ram_storage;
mod split;
mod split_cache;
mod storage_factory;
mod storage_resolver;
mod versioned_component;

use quickwit_common::uri::Uri;
pub use split_cache::SplitCache;
pub use tantivy::directory::OwnedBytes;
pub use versioned_component::VersionedComponent;

pub use self::bundle_storage::{BundleStorage, BundleStorageFileOffsets};
#[cfg(any(test, feature = "testsuite"))]
pub use self::cache::MockStorageCache;
pub use self::cache::{
    ByteRangeCache, MemorySizedCache, QuickwitCache, StorageCache, wrap_storage_with_cache,
};
pub use self::local_file_storage::{LocalFileStorage, LocalFileStorageFactory};
#[cfg(feature = "azure")]
pub use self::object_storage::{AzureBlobStorage, AzureBlobStorageFactory};
pub use self::object_storage::{
    MultiPartPolicy, S3CompatibleObjectStorage, S3CompatibleObjectStorageFactory,
};
#[cfg(feature = "gcs")]
pub use self::opendal_storage::GoogleCloudStorageFactory;
#[cfg(all(feature = "gcs", feature = "integration-testsuite"))]
pub use self::opendal_storage::test_config_helpers;
pub use self::ram_storage::{RamStorage, RamStorageBuilder};
pub use self::split::{SplitPayload, SplitPayloadBuilder};
#[cfg(any(test, feature = "testsuite"))]
pub use self::storage::MockStorage;
#[cfg(any(test, feature = "testsuite"))]
pub use self::storage_factory::MockStorageFactory;
pub use self::storage_factory::{StorageFactory, UnsupportedStorage};
pub use self::storage_resolver::StorageResolver;
#[cfg(feature = "integration-testsuite")]
pub use self::test_suite::{
    storage_test_multi_part_upload, storage_test_single_part_upload, storage_test_suite,
    test_write_and_bulk_delete,
};
pub use self::timeout_and_retry_storage::TimeoutAndRetryStorage;
pub use crate::error::{
    BulkDeleteError, DeleteFailure, StorageError, StorageErrorKind, StorageResolverError,
    StorageResult,
};

/// Loads an entire local or remote file into memory.
pub async fn load_file(
    storage_resolver: &StorageResolver,
    uri: &Uri,
) -> anyhow::Result<OwnedBytes> {
    let parent = uri
        .parent()
        .ok_or_else(|| anyhow::anyhow!("URI `{uri}` is not a valid file URI"))?;
    let storage = storage_resolver.resolve(&parent).await?;
    let file_name = uri
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("URI `{uri}` is not a valid file URI"))?;
    let bytes = storage.get_all(file_name).await?;
    Ok(bytes)
}

// this function isn't meant to be called, just to break compilation if
// serde_json::Map is an ordered map and not a btree map
#[allow(dead_code)]
#[cfg(not(any(test, feature = "testsuite", feature = "integration-testsuite")))]
unsafe fn serde_json_preserve_order_canary(
    val: serde_json::Map<String, serde_json::Value>,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    use std::mem::transmute as assert_serde_json__preserve_order__disabled;
    unsafe { assert_serde_json__preserve_order__disabled(val) }
}

#[cfg(any(test, feature = "testsuite", feature = "integration-testsuite"))]
mod for_test {
    use std::sync::Arc;

    use crate::{RamStorage, Storage};

    /// Returns a storage backed by an "in-memory file" for testing.
    pub fn storage_for_test() -> Arc<dyn Storage> {
        Arc::new(RamStorage::default())
    }
}

#[cfg(any(test, feature = "testsuite", feature = "integration-testsuite"))]
pub use for_test::storage_for_test;

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[tokio::test]
    async fn test_load_file() {
        let storage_resolver = StorageResolver::builder()
            .register(LocalFileStorageFactory)
            .build()
            .unwrap();
        let expected_bytes = tokio::fs::read_to_string("Cargo.toml").await.unwrap();
        assert_eq!(
            load_file(&storage_resolver, &Uri::from_str("Cargo.toml").unwrap())
                .await
                .unwrap()
                .as_slice(),
            expected_bytes.as_bytes()
        );
    }
}

#[cfg(any(test, feature = "integration-testsuite"))]
pub(crate) mod test_suite {

    use std::path::Path;

    use anyhow::Context;
    use tokio::io::AsyncReadExt;

    use crate::{Storage, StorageErrorKind};

    async fn test_get_inexistent_file(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let err = storage
            .get_slice(Path::new("missingfile"), 0..3)
            .await
            .map_err(|err| err.kind());
        assert!(matches!(err, Err(StorageErrorKind::NotFound)));
        Ok(())
    }

    async fn test_write_and_get_slice(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_read_slice");
        storage
            .put(
                test_path,
                Box::new(b"abcdefghiklmnopqrstuvxyz"[..].to_vec()),
            )
            .await?;
        let payload = storage.get_slice(test_path, 3..6).await?;
        assert_eq!(&payload[..], b"def");
        Ok(())
    }

    async fn test_write_and_get_slice_stream(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_read_slice_stream");
        storage
            .put(
                test_path,
                Box::new(b"abcdefghiklmnopqrstuvxyz"[..].to_vec()),
            )
            .await?;
        let mut reader = storage.get_slice_stream(test_path, 3..6).await?;
        let mut buf = vec![0; 3];
        reader.read_exact(&mut buf).await?;
        assert_eq!(&buf[..], b"def");
        Ok(())
    }

    async fn test_write_get_all(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_read_all");
        storage
            .put(test_path, Box::new(b"abcdef"[..].to_vec()))
            .await?;
        let payload = storage.get_all(test_path).await?;
        assert_eq!(&payload[..], &b"abcdef"[..]);
        Ok(())
    }

    async fn test_write_and_cp(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_cp");
        let payload_bytes = b"abcdefghijklmnopqrstuvwxyz";
        storage
            .put(test_path, Box::new(payload_bytes.to_vec()))
            .await?;
        let temp_dir = tempfile::tempdir()?;
        let dest_path = temp_dir.path().to_path_buf();
        let local_copy = dest_path.join("local_copy");
        storage.copy_to_file(test_path, &local_copy).await?;
        let payload = std::fs::read(&local_copy)?;
        assert_eq!(&payload[..], payload_bytes);
        Ok(())
    }

    async fn test_write_and_delete(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_and_delete");
        let payload_bytes = b"abcdefghijklmnopqrstuvwxyz";
        storage
            .put(test_path, Box::new(payload_bytes.to_vec()))
            .await?;
        assert!(storage.exists(test_path).await?);
        storage.delete(test_path).await?;
        assert!(!storage.exists(test_path).await?);
        storage.delete(test_path).await?;
        Ok(())
    }

    /// Tests `Storage::bulk_delete`.
    pub async fn test_write_and_bulk_delete(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_paths = [
            Path::new("foo"),
            Path::new("bar"),
            Path::new("qux"),
            Path::new("baz"),
            Path::new("file-does-not-exist"),
        ];
        for test_path in &test_paths[0..4] {
            storage
                .put(Path::new(test_path), Box::new(b"123".to_vec()))
                .await?;
            assert!(storage.exists(test_path).await?);
        }
        storage.bulk_delete(&test_paths).await?;

        for test_path in test_paths {
            assert!(!storage.exists(test_path).await?);
        }
        Ok(())
    }

    async fn test_file_size(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("write_for_filesize");
        let payload_bytes = b"abcdefghijklmnopqrstuvwxyz";
        storage
            .put(test_path, Box::new(payload_bytes.to_vec()))
            .await?;
        assert_eq!(storage.file_num_bytes(test_path).await?, 26u64);
        storage.delete(test_path).await?;
        Ok(())
    }

    async fn test_exists(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("exists");
        assert!(!storage.exists(test_path).await.unwrap());
        storage
            .put(test_path, Box::<std::vec::Vec<u8>>::default())
            .await?;
        assert!(storage.exists(test_path).await.unwrap());
        storage.delete(test_path).await.unwrap();
        Ok(())
    }

    async fn test_delete_missing_file(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("missing_file");
        assert!(!storage.exists(test_path).await.unwrap());
        assert!(storage.delete(test_path).await.is_ok());
        Ok(())
    }

    async fn test_write_and_delete_with_dir_separator(
        storage: &mut dyn Storage,
    ) -> anyhow::Result<()> {
        let test_path = Path::new("foo/bar/write_and_delete_with_separator");
        let payload_bytes = b"abcdefghijklmnopqrstuvwxyz";
        storage
            .put(test_path, Box::new(payload_bytes.to_vec()))
            .await?;
        assert!(matches!(
            storage.exists(Path::new("foo/bar")).await,
            Ok(false)
        ));
        storage.delete(test_path).await?;

        assert!(matches!(
            storage.exists(Path::new("foo/bar")).await,
            Ok(false)
        ));
        assert!(matches!(storage.exists(Path::new("foo")).await, Ok(false)));
        Ok(())
    }

    /// Generic test suite for a storage.
    pub async fn storage_test_suite(storage: &mut dyn Storage) -> anyhow::Result<()> {
        test_get_inexistent_file(storage)
            .await
            .context("get_inexistent_file")?;
        test_write_and_get_slice(storage)
            .await
            .context("write_and_get_slice")?;
        test_write_and_get_slice_stream(storage)
            .await
            .context("write_and_get_slice_stream")?;
        test_write_get_all(storage)
            .await
            .context("write_and_get_all")?;
        test_write_and_cp(storage).await.context("write_and_cp")?;
        test_write_and_delete(storage)
            .await
            .context("write_and_delete")?;
        test_write_and_bulk_delete(storage)
            .await
            .context("write_and_bulk_delete")?;
        test_exists(storage).await.context("exists")?;
        test_write_and_delete_with_dir_separator(storage)
            .await
            .context("write_and_delete_with_separator")?;
        test_file_size(storage).await.context("file_size")?;
        test_delete_missing_file(storage)
            .await
            .context("delete_missing_file")?;
        Ok(())
    }

    /// Generic single-part upload test.
    #[cfg(feature = "integration-testsuite")]
    pub async fn storage_test_single_part_upload(storage: &mut dyn Storage) -> anyhow::Result<()> {
        use std::ops::Range;

        let test_path = Path::new("hello_small.txt");
        let data = b"hello, happy tax payer!";
        let data_size = data.len() as u64;
        storage.put(test_path, Box::new(data.to_vec())).await?;
        // file_num_bytes
        assert_eq!(storage.file_num_bytes(test_path).await?, data_size);
        // get_all
        let all_bytes = storage.get_all(test_path).await?;
        assert_eq!(all_bytes.as_slice(), data);
        // get_slice
        let happy_bytes = storage
            .get_slice(test_path, Range { start: 7, end: 12 })
            .await?;
        assert_eq!(happy_bytes.as_slice(), &data[7..12]);
        // get_slice_stream
        let mut happy_byte_stream = storage
            .get_slice_stream(test_path, Range { start: 7, end: 12 })
            .await?;
        let mut happy_bytes_read = Vec::new();
        happy_byte_stream.read_to_end(&mut happy_bytes_read).await?;
        assert_eq!(happy_bytes_read.as_slice(), &data[7..12]);
        Ok(())
    }

    /// Generic multi-part upload test.
    #[cfg(feature = "integration-testsuite")]
    pub async fn storage_test_multi_part_upload(storage: &mut dyn Storage) -> anyhow::Result<()> {
        let test_path = Path::new("hello_large.txt");

        let mut test_buffer = Vec::with_capacity(15_000_000);
        for i in 0..15_000_000u32 {
            test_buffer.push((i % 256) as u8);
        }

        storage
            .put(test_path, Box::new(test_buffer.clone()))
            .await?;

        assert_eq!(storage.file_num_bytes(test_path).await?, 15_000_000);

        let downloaded_data = storage.get_all(test_path).await?;

        assert_eq!(test_buffer.len(), downloaded_data.len(), "Length mismatch");
        // dont use assert_eq since we dont want large buffers to be printed
        // if assert fails
        assert!(
            test_buffer.as_slice() == downloaded_data.as_slice(),
            "Content mismatch - data corruption detected!"
        );

        Ok(())
    }
}
