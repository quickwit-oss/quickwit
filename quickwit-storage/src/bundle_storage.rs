// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tantivy::common::CountingWriter;
use tantivy::directory::FileSlice;
use tantivy::HasLen;
use thiserror::Error;
use tracing::error;

use crate::{OwnedBytes, Storage, StorageError, StorageResult};

/// Filename used for the bundle.
pub const BUNDLE_FILENAME: &str = "bundle";

/// BundleStorage bundles together multiple files into a single file.
/// with some metadata
pub struct BundleStorage {
    storage: Arc<dyn Storage>,
    bundle_filepath: PathBuf,
    metadata: BundleStorageFileOffsets,
}

impl BundleStorage {
    /// Creates a new BundleStorage.
    ///
    /// The provided data must include the footer_bytes at the end of the slice, but it can have
    /// more up front.
    pub fn new(
        storage: Arc<dyn Storage>,
        bundle_filepath: PathBuf,
        meta_data: &[u8],
    ) -> Result<Self, CorruptedData> {
        let metadata = BundleStorageFileOffsets::open(meta_data)?;
        Ok(BundleStorage {
            storage,
            bundle_filepath,
            metadata,
        })
    }

    /// Returns Iterator over files contained in the bundle.
    pub fn iter_files(&self) -> impl Iterator<Item = &PathBuf> {
        self.metadata.files.keys()
    }
}

#[derive(Debug, Error)]
#[error("CorruptedData. error: {error:?}")]
pub struct CorruptedData {
    #[from]
    #[source]
    pub error: serde_json::Error,
}

const SPLIT_HOTBYTES_FOOTER_LENGTH_NUM_BYTES: usize = std::mem::size_of::<u64>();

/// Returns the file offsets in the file bundle.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct BundleStorageFileOffsets {
    /// The files and their offsets in the body
    pub files: HashMap<PathBuf, Range<usize>>,
}

impl BundleStorageFileOffsets {
    fn open(data: &[u8]) -> Result<Self, CorruptedData> {
        let (body_and_footer, footer_num_bytes_data) =
            data.split_at(data.len() - SPLIT_HOTBYTES_FOOTER_LENGTH_NUM_BYTES);
        let footer_num_bytes: u64 = u64::from_le_bytes(footer_num_bytes_data.try_into().unwrap());
        let footer_slice = &body_and_footer[body_and_footer.len() - footer_num_bytes as usize..];
        let bundle_storage_file_offsets = serde_json::from_slice(footer_slice)?;
        Ok(bundle_storage_file_offsets)
    }

    /// Read metadata from a file.
    pub fn open_from_file_slice(file: FileSlice) -> io::Result<Self> {
        let (body_and_footer, footer_num_bytes_data) =
            file.split_from_end(SPLIT_HOTBYTES_FOOTER_LENGTH_NUM_BYTES);
        let footer_num_bytes: u64 = u64::from_le_bytes(
            footer_num_bytes_data
                .read_bytes()?
                .as_slice()
                .try_into()
                .unwrap(),
        );

        let bundle_storage_file_offsets_data = body_and_footer
            .slice_from_end(footer_num_bytes as usize)
            .read_bytes()?;
        let bundle_storage_file_offsets =
            serde_json::from_slice(&bundle_storage_file_offsets_data)?;

        Ok(bundle_storage_file_offsets)
    }

    /// Returns file offsets for given path.
    pub fn get(&self, path: &Path) -> Option<Range<usize>> {
        self.files.get(path).cloned()
    }

    /// Returns whether file exists in metadata.
    pub fn exists(&self, path: &Path) -> bool {
        self.files.contains_key(path)
    }
}

#[async_trait]
impl Storage for BundleStorage {
    async fn put(
        &self,
        path: &Path,
        _payload: Box<dyn crate::PutPayloadProvider>,
    ) -> crate::StorageResult<()> {
        Err(unsupported_operation(path))
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> crate::StorageResult<()> {
        let file_num_bytes = self.file_num_bytes(path).await? as usize;

        let mut out_file = File::create(output_path)?;
        let block_size = 100_000_000;
        for block in chunk_range(0..file_num_bytes, block_size) {
            let file_content = self.get_slice(path, block).await?;
            out_file.write_all(&file_content)?;
        }

        Ok(())
    }

    async fn get_slice(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> crate::StorageResult<OwnedBytes> {
        let file_offsets = self.metadata.get(path).ok_or_else(|| {
            crate::StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Missing file `{}`", path.display()))
        })?;
        let new_range =
            file_offsets.start as usize + range.start..file_offsets.start as usize + range.end;
        self.storage
            .get_slice(&self.bundle_filepath, new_range)
            .await
    }

    async fn get_all(&self, path: &Path) -> crate::StorageResult<OwnedBytes> {
        let file_offsets = self.metadata.get(path).ok_or_else(|| {
            crate::StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Missing file `{}`", path.display()))
        })?;
        self.storage
            .get_slice(
                &self.bundle_filepath,
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

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let file_range = self.metadata.get(path).ok_or_else(|| {
            crate::StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Missing file `{}`", path.display()))
        })?;
        Ok(file_range.len() as u64)
    }

    fn uri(&self) -> String {
        self.storage.uri()
    }
}

fn chunk_range(range: Range<usize>, chunk_size: usize) -> impl Iterator<Item = Range<usize>> {
    range.clone().step_by(chunk_size).map(move |block_start| {
        let block_end = (block_start + chunk_size).min(range.end);
        block_start..block_end
    })
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
            &self.bundle_filepath, self.metadata
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
pub struct BundleStorageBuilder<W> {
    metadata: BundleStorageFileOffsets,
    counting_writer: CountingWriter<W>,
}

impl<W: io::Write> BundleStorageBuilder<W> {
    /// Creates a new BundleStorageBuilder, to which files can be added.
    pub fn new(wrt: W) -> io::Result<Self> {
        let counting_writer = CountingWriter::wrap(wrt);
        Ok(BundleStorageBuilder {
            counting_writer,
            metadata: Default::default(),
        })
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
        let start_offset = self.counting_writer.written_bytes() as usize;
        io::copy(&mut read, &mut self.counting_writer)?;
        let end_offset = self.counting_writer.written_bytes() as usize;
        self.metadata
            .files
            .insert(file_name, start_offset..end_offset);
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

    /// Writes the bundle file offsets metadata at the end of the bundle file,
    /// and returns the byte-range of this metadata information.
    pub fn finalize(mut self) -> io::Result<Range<u64>> {
        // The footer offset, where the footer begins.
        // The footer includes the footer metadata as json encoded and the size of the footer in
        // bytes.
        let file_offsets_metadata_start = self.counting_writer.written_bytes();
        let metadata_json = serde_json::to_string(&self.metadata)?;
        self.counting_writer.write_all(metadata_json.as_bytes())?;
        let metadata_json_len = metadata_json.len() as u64;
        self.counting_writer
            .write_all(&metadata_json_len.to_le_bytes())?;
        let file_offsets_metadata_end = self.counting_writer.written_bytes();
        Ok(file_offsets_metadata_start..file_offsets_metadata_end)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::RamStorageBuilder;

    #[tokio::test]
    async fn bundle_storage_file_offsets() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");
        let test_bundle_path = temp_dir.path().join("le_bundle");

        let mut bundle_file = File::create(test_bundle_path.to_owned())?;
        let mut bundle_builder = BundleStorageBuilder::new(&mut bundle_file)?;

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        bundle_builder.add_file(&test_filepath1)?;
        bundle_builder.add_file(&test_filepath2)?;
        bundle_builder.finalize()?;

        let bundle_filepath = Path::new("bundle");
        let buffer = fs::read(test_bundle_path)?;
        let bundle_file_slice = FileSlice::from(buffer.to_owned());
        let metadata = BundleStorageFileOffsets::open_from_file_slice(bundle_file_slice)?;
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
        let mut buffer = Vec::new();
        let mut bundle_builder = BundleStorageBuilder::new(&mut buffer)?;

        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        bundle_builder.add_file(&test_filepath1)?;
        bundle_builder.add_file(&test_filepath2)?;
        bundle_builder.finalize()?;

        let metadata = BundleStorageFileOffsets::open(&buffer)?;
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
        let mut buffer: Vec<u8> = Vec::new();

        let create_bundle = BundleStorageBuilder::new(&mut buffer)?;
        create_bundle.finalize()?;

        let metadata = BundleStorageFileOffsets::open(&buffer)?;

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
