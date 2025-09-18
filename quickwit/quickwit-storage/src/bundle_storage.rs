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

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use anyhow::Context;
use async_trait::async_trait;
use quickwit_common::chunk_range;
use quickwit_common::uri::Uri;
use serde::{Deserialize, Serialize};
use tantivy::HasLen;
use tantivy::directory::FileSlice;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tracing::error;

use crate::storage::SendableAsync;
use crate::{
    BulkDeleteError, OwnedBytes, Storage, StorageError, StorageResult, VersionedComponent,
};

/// BundleStorage bundles together multiple files into a single file.
/// with some metadata
pub struct BundleStorage {
    storage: Arc<dyn Storage>,
    /// The file path of the bundle in the storage.
    bundle_filepath: PathBuf,
    metadata: BundleStorageFileOffsets,
}

impl BundleStorage {
    /// Opens a BundleStorage.
    ///
    /// The provided data must include the footer_bytes at the end of the slice, but it can have
    /// more up front.
    ///
    /// Returns (Hotcache, Self)
    pub fn open_from_split_data_with_owned_bytes(
        storage: Arc<dyn Storage>,
        bundle_filepath: PathBuf,
        split_data: OwnedBytes,
    ) -> anyhow::Result<(FileSlice, Self)> {
        Self::open_from_split_data(
            storage,
            bundle_filepath,
            FileSlice::new(Arc::new(split_data)),
        )
    }
    /// Opens a BundleStorage.
    ///
    /// The provided data must include the footer_bytes at the end of the slice, but it can have
    /// more up front.
    ///
    /// Returns (Hotcache, Self)
    pub fn open_from_split_data(
        storage: Arc<dyn Storage>,
        bundle_filepath: PathBuf,
        split_data: FileSlice,
    ) -> anyhow::Result<(FileSlice, Self)> {
        let (hotcache, metadata) = BundleStorageFileOffsets::open_from_split_data(split_data)?;
        Ok((
            hotcache,
            BundleStorage {
                storage,
                bundle_filepath,
                metadata,
            },
        ))
    }

    /// Returns Iterator over files contained in the bundle.
    pub fn iter_files(&self) -> impl Iterator<Item = &PathBuf> {
        self.metadata.files.keys()
    }
}

const SPLIT_HOTBYTES_FOOTER_LENGTH_NUM_BYTES: usize = std::mem::size_of::<u32>();
const BUNDLE_METADATA_LENGTH_NUM_BYTES: usize = std::mem::size_of::<u32>();

#[derive(Copy, Clone, Default)]
#[repr(u32)]
pub enum BundleStorageFileOffsetsVersions {
    #[default]
    V1 = 1,
}

impl VersionedComponent for BundleStorageFileOffsetsVersions {
    const MAGIC_NUMBER: u32 = 403_881_646u32;

    type Component = BundleStorageFileOffsets;

    fn to_version_code(self) -> u32 {
        self as u32
    }

    fn try_from_version_code_impl(version_code: u32) -> Option<Self> {
        match version_code {
            1 => Some(Self::V1),
            _ => None,
        }
    }

    fn serialize_impl(component: &BundleStorageFileOffsets, output: &mut Vec<u8>) {
        let metadata_json = serde_json::to_string(component).unwrap();
        output.extend_from_slice(metadata_json.as_bytes());
    }

    fn deserialize_impl(&self, bytes: &mut OwnedBytes) -> anyhow::Result<Self::Component> {
        serde_json::from_reader(bytes).context("deserializing bundle storage file offsets failed")
    }
}

/// Returns the file offsets in the file bundle.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct BundleStorageFileOffsets {
    /// The files and their offsets in the body
    pub files: HashMap<PathBuf, Range<u64>>,
}

impl BundleStorageFileOffsets {
    /// File need to include split data (with hotcache at the end).
    /// See docs/internals/split-format.md
    /// [Files, FileMetadata, FileMetadata Len, HotCache, HotCache Len]
    /// Returns (Hotcache, Self)
    fn open_from_split_data(file: FileSlice) -> anyhow::Result<(FileSlice, Self)> {
        let (bundle_and_hotcache_bytes, hotcache_num_bytes_data) =
            file.split_from_end(SPLIT_HOTBYTES_FOOTER_LENGTH_NUM_BYTES);
        let hotcache_num_bytes: u32 = u32::from_le_bytes(
            hotcache_num_bytes_data
                .read_bytes()?
                .as_ref()
                .try_into()
                .unwrap(),
        );
        let (bundle, hotcache) =
            bundle_and_hotcache_bytes.split_from_end(hotcache_num_bytes as usize);
        Ok((hotcache, Self::open(bundle)?))
    }

    /// FileSlice needs to end with the bundle (without hotcache from the split at the end).
    /// See docs/internals/split-format.md
    /// [Files, FileMetadata, FileMetadata Len]
    pub fn open(file: FileSlice) -> anyhow::Result<Self> {
        let (tantivy_files_data, num_bytes_file_metadata) =
            file.split_from_end(BUNDLE_METADATA_LENGTH_NUM_BYTES);
        let footer_num_bytes: u32 = u32::from_le_bytes(
            num_bytes_file_metadata
                .read_bytes()?
                .as_slice()
                .try_into()
                .unwrap(),
        );

        let mut bundle_storage_file_offsets_data = tantivy_files_data
            .slice_from_end(footer_num_bytes as usize)
            .read_bytes()?;
        BundleStorageFileOffsetsVersions::try_read_component(&mut bundle_storage_file_offsets_data)
    }

    /// Returns file offsets for given path.
    pub fn get(&self, path: &Path) -> Option<Range<u64>> {
        self.files.get(path).cloned()
    }

    /// Returns whether file exists in metadata.
    pub fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }
}

#[async_trait]
impl Storage for BundleStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        if !self
            .storage
            .exists(&self.bundle_filepath)
            .await
            .unwrap_or(false)
        {
            anyhow::bail!("`{}` not found in storage", self.bundle_filepath.display())
        }
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        _payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        Err(unsupported_operation(&[path]))
    }

    async fn copy_to(
        &self,
        path: &Path,
        output: &mut dyn SendableAsync,
    ) -> crate::StorageResult<()> {
        let file_num_bytes = self.file_num_bytes(path).await? as usize;
        let block_size = 100_000_000;
        for block in chunk_range(0..file_num_bytes, block_size) {
            let file_content = self.get_slice(path, block).await?;
            output.write_all(&file_content).await?;
        }
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> crate::StorageResult<OwnedBytes> {
        let file_offsets = self.metadata.get(path).ok_or_else(|| {
            crate::StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("missing file `{}`", path.display()))
        })?;
        let new_range =
            file_offsets.start as usize + range.start..file_offsets.start as usize + range.end;
        self.storage
            .get_slice(&self.bundle_filepath, new_range)
            .await
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        _range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        Err(unsupported_operation(&[path]))
    }

    async fn get_all(&self, path: &Path) -> crate::StorageResult<OwnedBytes> {
        let file_offsets = self.metadata.get(path).ok_or_else(|| {
            crate::StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("missing file `{}`", path.display()))
        })?;
        self.storage
            .get_slice(
                &self.bundle_filepath,
                file_offsets.start as usize..file_offsets.end as usize,
            )
            .await
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        Err(unsupported_operation(&[path]))
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        Err(BulkDeleteError {
            error: Some(unsupported_operation(paths)),
            ..Default::default()
        })
    }

    async fn exists(&self, path: &Path) -> crate::StorageResult<bool> {
        // also check if self.bundle_file_name exists ?
        Ok(self.metadata.exists(path))
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let file_range = self.metadata.get(path).ok_or_else(|| {
            crate::StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("missing file `{}`", path.display()))
        })?;
        Ok(file_range.end - file_range.start)
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }
}

impl HasLen for BundleStorage {
    fn len(&self) -> usize {
        unimplemented!()
    }
}

impl fmt::Debug for BundleStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BundleStorage({:?}, files={:?})",
            &self.bundle_filepath, self.metadata
        )
    }
}

fn unsupported_operation(paths: &[&Path]) -> StorageError {
    let msg = "Unsupported operation. BundleStorage only supports async reads";
    error!(paths=?paths, msg);
    io::Error::other(format!("{msg}: {paths:?}")).into()
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use super::*;
    use crate::{PutPayload, RamStorageBuilder, SplitPayloadBuilder};

    #[tokio::test]
    async fn bundle_storage_file_offsets() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let buffer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[],
            &[5, 5, 5],
        )?
        .read_all()
        .await?;

        let bundle_filepath = Path::new("bundle");
        let bundle_file_slice = FileSlice::new(Arc::new(buffer.clone()));
        let (hotcache, metadata) =
            BundleStorageFileOffsets::open_from_split_data(bundle_file_slice)?;
        assert_eq!(hotcache.read_bytes().unwrap().as_ref(), &[5, 5, 5]);
        let ram_storage = RamStorageBuilder::default()
            .put(&bundle_filepath.to_string_lossy(), &buffer)
            .build();

        let bundle_storage = BundleStorage {
            metadata,
            bundle_filepath: bundle_filepath.to_path_buf(),
            storage: Arc::new(ram_storage),
        };
        let f1_data = bundle_storage.get_all(Path::new("f1")).await?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_storage.get_all(Path::new("f2")).await?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        Ok(())
    }
    #[tokio::test]
    async fn bundle_storage_test() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let buffer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[],
            &[1, 3, 3, 7],
        )?
        .read_all()
        .await?;

        let (hotcache, metadata) =
            BundleStorageFileOffsets::open_from_split_data(FileSlice::from(buffer.to_vec()))?;
        assert_eq!(hotcache.read_bytes().unwrap().as_ref(), &[1, 3, 3, 7]);

        let bundle_filepath = Path::new("bundle");
        let ram_storage = RamStorageBuilder::default()
            .put(&bundle_filepath.to_string_lossy(), &buffer)
            .build();

        let bundle_storage = BundleStorage {
            metadata,
            bundle_filepath: bundle_filepath.to_path_buf(),
            storage: Arc::new(ram_storage),
        };
        let f1_data = bundle_storage.get_all(Path::new("f1")).await?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_storage.get_all(Path::new("f2")).await?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        let copy_to_file = temp_dir.path().join("copy_file");
        bundle_storage
            .copy_to_file(Path::new("f2"), &copy_to_file)
            .await?;
        let file_content = fs::read(copy_to_file).unwrap();
        assert_eq!(&f2_data[..], file_content);

        Ok(())
    }

    #[tokio::test]
    async fn bundlestorage_test_empty() -> anyhow::Result<()> {
        let buffer = SplitPayloadBuilder::get_split_payload(&[], &[], &[])?
            .read_all()
            .await?;

        let (_hotcache, metadata) =
            BundleStorageFileOffsets::open_from_split_data(FileSlice::from(buffer.to_vec()))?;

        let bundle_filepath = PathBuf::from("bundle");
        let ram_storage = RamStorageBuilder::default()
            .put(&bundle_filepath.to_string_lossy(), &buffer)
            .build();
        let bundle_storage = BundleStorage {
            metadata,
            bundle_filepath,
            storage: Arc::new(ram_storage),
        };

        assert_eq!(bundle_storage.exists(Path::new("blub")).await?, false);

        Ok(())
    }
}
