/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use crate::Storage;
use crate::{StorageError, StorageResult};
use async_trait::async_trait;
use bytes::Bytes;
use quickwit_common::HOTCACHE_FILENAME;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::common::BinarySerializable;
use tantivy::HasLen;
use tracing::error;

/// Filename used for the bundle.
pub const BUNDLE_FILENAME: &str = "bundle";

/// BundleStorage bundles together multiple files into a single file.
/// with some metadata
pub struct BundleStorage {
    storage: Arc<dyn Storage>,
    bundle_file_name: PathBuf,
    metadata: BundleStorageFileOffsets,
}

impl BundleStorage {
    /// Creates a new BundleStorage.
    ///
    /// The provided data must include the footer_bytes at the end of the slice, but it can have more up front.
    pub fn new(
        storage: Arc<dyn Storage>,
        bundle_file_name: PathBuf,
        meta_data: &[u8],
    ) -> io::Result<Self> {
        let metadata = BundleStorageFileOffsets::open(meta_data)?;
        Ok(BundleStorage {
            storage,
            bundle_file_name,
            metadata,
        })
    }

    /// Return the file statistics for the files that have been added.
    pub fn file_statistics(&self) -> FileStatistics {
        self.metadata.file_statistics()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct BundleStorageFileOffsets {
    pub(crate) files: HashMap<PathBuf, Range<usize>>,
}

impl BundleStorageFileOffsets {
    pub fn file_statistics(&self) -> FileStatistics {
        let mut file_statistics = FileStatistics::default();
        file_statistics.min_file_size_in_bytes = self
            .files
            .values()
            .map(|range| range.end - range.start)
            .min()
            .unwrap_or(0) as u64;
        file_statistics.max_file_size_in_bytes = self
            .files
            .values()
            .map(|range| range.end - range.start)
            .max()
            .unwrap_or(0) as u64;
        file_statistics.avg_file_size_in_bytes = self
            .files
            .values()
            .map(|range| range.end - range.start)
            .sum::<usize>() as u64
            / self.files.len() as u64;

        file_statistics
    }
}

/// Contains file size statistics.
#[derive(Default, Debug, Clone)]
pub struct FileStatistics {
    /// the min_file_size_in_bytesf
    pub min_file_size_in_bytes: u64,
    /// the max_file_size_in_bytesf
    pub max_file_size_in_bytes: u64,
    /// the avg_file_size_in_bytesf
    pub avg_file_size_in_bytes: u64,
}

impl BundleStorageFileOffsets {
    fn open(data: &[u8]) -> io::Result<Self> {
        let footer_size: u64 = BinarySerializable::deserialize(&mut &data[data.len() - 8..])?;

        let footer_slice = &data[data.len() - 8 - footer_size as usize..data.len() - 8];
        serde_json::from_slice(footer_slice)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))
    }
    fn get(&self, path: &Path) -> StorageResult<Range<usize>> {
        self.files
            .get(path)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "not found in metadata").into())
    }
    fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }
}

#[async_trait]
impl Storage for BundleStorage {
    async fn put(&self, path: &Path, _payload: crate::PutPayload) -> crate::StorageResult<()> {
        Err(unsupported_operation(path))
    }

    async fn copy_to_file(&self, path: &Path, _output_path: &Path) -> crate::StorageResult<()> {
        Err(unsupported_operation(path))
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> crate::StorageResult<Bytes> {
        let file_offsets = self.metadata.get(path)?;
        let new_range =
            file_offsets.start as usize + range.start..file_offsets.start as usize + range.end;
        self.storage
            .get_slice(&self.bundle_file_name, new_range)
            .await
    }

    async fn get_all(&self, path: &Path) -> crate::StorageResult<Bytes> {
        let file_offsets = self.metadata.get(path)?;
        self.storage
            .get_slice(
                &self.bundle_file_name,
                file_offsets.start as usize..file_offsets.end as usize,
            )
            .await
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        Err(unsupported_operation(path))
    }

    async fn exists(&self, path: &Path) -> crate::StorageResult<bool> {
        // also check if self.bundle_file_name exists ?
        Ok(self.metadata.exists(path))
    }

    fn uri(&self) -> String {
        self.storage.uri()
    }
}

impl HasLen for BundleStorage {
    fn len(&self) -> usize {
        unimplemented!()
    }
}

impl fmt::Debug for BundleStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BundleStorage({:?}, files={:?})",
            &self.bundle_file_name, self.metadata
        )
    }
}

fn unsupported_operation(path: &Path) -> StorageError {
    let msg = "Unsupported operation. BundleStorage only supports async reads";
    error!(path=?path, msg);
    io::Error::new(io::ErrorKind::Other, format!("{}: {:?}", msg, path)).into()
}

/// BundleStorage bundles together multiple files into a single file
/// with some metadata
pub struct BundleStorageBuilder {
    metadata: BundleStorageFileOffsets,
    current_offset: usize,
    /// The offset in the file where the hotcache begins. This is used to read
    /// the hotcache and footer in a single read.
    hotcache_offset: usize,
    bundle_file: File,
}

/// Contains hotcache and footer offset.
#[derive(Clone, Eq, PartialEq, Default, Debug, Serialize, Deserialize)]
pub struct BundleStorageOffsets {
    /// The offset in the file where the hotcache begins. This is used to read
    /// the hotcache and footer in a single read.
    pub hotcache_offset_start: usize,
    /// start and end (not inclusive) of the footer offsets.
    pub footer_offsets: Range<usize>,
    /// The total size of the bundle.
    pub bundle_file_size: usize,
}

impl BundleStorageBuilder {
    /// Creates a new BundleStorageBuilder, to which files can be added.
    pub fn new(path: &Path) -> io::Result<Self> {
        let sink = File::create(path)?;

        Ok(BundleStorageBuilder {
            bundle_file: sink,
            current_offset: 0,
            hotcache_offset: 0,
            metadata: Default::default(),
        })
    }

    /// Return the file statistics for the files that have been added.
    pub fn file_statistics(&self) -> FileStatistics {
        self.metadata.file_statistics()
    }

    /// Appends a file to the bundle file.
    ///
    /// The hotcache needs to be the last file that is added, in order to be able to read
    /// the hotcache and the metadata in one continous read.
    pub fn add_file_from_read<R: Read>(
        &mut self,
        mut read: R,
        file_name: PathBuf,
    ) -> io::Result<()> {
        let bytes_written = io::copy(&mut read, &mut self.bundle_file)? as usize;
        if file_name.to_string_lossy() == HOTCACHE_FILENAME {
            self.hotcache_offset = self.current_offset;
        }
        self.metadata.files.insert(
            file_name,
            self.current_offset..self.current_offset + bytes_written,
        );
        self.current_offset += bytes_written;
        Ok(())
    }

    /// Appends a file to the bundle file.
    ///
    /// The hotcache needs to be the last file that is added, in order to be able to read
    /// the hotcache and the metadata in one continous read.
    pub fn add_file(&mut self, path: &Path) -> io::Result<()> {
        let file = File::open(path)?;
        let file_name = PathBuf::from(path.file_name().ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidInput,
                format!("could not extract file_name from path {:?}", path),
            )
        })?);
        self.add_file_from_read(file, file_name)?;
        Ok(())
    }

    /// Writes the metadata into the footer.
    ///
    pub fn finalize(mut self) -> io::Result<BundleStorageOffsets> {
        // The footer offset, where the footer begins.
        // The footer includes the footer metadata as json encoded and the size of the footer in
        // bytes.
        let footer_offset_start = self.current_offset;
        let metadata_json = serde_json::to_string(&self.metadata)?;
        self.bundle_file.write_all(metadata_json.as_bytes())?;
        self.current_offset += metadata_json.as_bytes().len();
        let meta_data_json_len = metadata_json.len() as u64;
        BinarySerializable::serialize(&meta_data_json_len, &mut self.bundle_file)?;
        self.current_offset += 8;
        let footer_offset_end = self.current_offset;
        let offsets = BundleStorageOffsets {
            hotcache_offset_start: self.hotcache_offset,
            footer_offsets: footer_offset_start..footer_offset_end,
            bundle_file_size: self.current_offset,
        };
        Ok(offsets)
    }
}
#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use crate::LocalFileStorage;

    use super::*;

    #[tokio::test]
    async fn bundlestorage_test() -> anyhow::Result<()> {
        let temp_dir = temp_dir();
        let bundle_file_name = temp_dir.join("asdf");
        let mut create_bundle = BundleStorageBuilder::new(&bundle_file_name)?;

        let test_file1_name = temp_dir.join("f1");
        let test_file2_name = temp_dir.join(HOTCACHE_FILENAME);

        let mut file1 = File::create(&test_file1_name).unwrap();
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_file2_name).unwrap();
        file2.write_all(&[99, 55, 44])?;

        create_bundle.add_file(&test_file1_name)?;
        create_bundle.add_file(&test_file2_name)?;
        let offsets = create_bundle.finalize()?;
        assert!(offsets.footer_offsets.start > offsets.hotcache_offset_start);

        let bytes = std::fs::read(&bundle_file_name)?;

        let metadata = BundleStorageFileOffsets::open(&bytes).unwrap();

        let path_root = format!("file://{}", temp_dir.to_string_lossy());
        let file_storage = LocalFileStorage::from_uri(&path_root)?;
        let bundle_storage = BundleStorage {
            metadata,
            bundle_file_name,
            storage: Arc::new(file_storage),
        };
        let f1_data = bundle_storage.get_all(Path::new("f1")).await?;
        assert_eq!(f1_data, &vec![123, 76]);

        let f2_data = bundle_storage.get_all(Path::new(HOTCACHE_FILENAME)).await?;
        assert_eq!(f2_data, &vec![99, 55, 44]);

        let f2_data = bundle_storage
            .get_slice(Path::new(HOTCACHE_FILENAME), 1..3)
            .await?;
        assert_eq!(f2_data, &vec![55, 44]);

        Ok(())
    }

    #[tokio::test]
    async fn bundlestorage_test_empty() -> anyhow::Result<()> {
        let temp_dir = temp_dir();
        let bundle_file_name = temp_dir.join("asdf");
        let create_bundle = BundleStorageBuilder::new(&bundle_file_name)?;

        create_bundle.finalize()?;

        let bytes = std::fs::read(&bundle_file_name)?;

        let metadata = BundleStorageFileOffsets::open(&bytes).unwrap();

        let path_root = format!("file://{}", temp_dir.to_string_lossy());
        let file_storage = LocalFileStorage::from_uri(&path_root)?;
        let bundle_storage = BundleStorage {
            metadata,
            bundle_file_name,
            storage: Arc::new(file_storage),
        };

        assert_eq!(bundle_storage.exists(Path::new("blub")).await?, false);

        Ok(())
    }
}
