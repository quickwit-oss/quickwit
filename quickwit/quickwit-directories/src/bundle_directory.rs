// Copyright (C) 2022 Quickwit, Inc.
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

use std::convert::TryInto;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use quickwit_storage::{BundleStorageFileOffsets, Storage, StorageResult};
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, FileSlice, OwnedBytes};
use tantivy::{Directory, HasLen};

/// BundleDirectory is a read-only directory that makes it possible to
/// open a split and serve the file it contains via tantivy's `Directory`.
///
/// It is the `Directory` equivalent of `BundleStorage`.
///
/// Split Format:
/// `[Files][FilesMetadata][FilesMetadata length 8 byte Little endian][Hotcache][Hotcache length 8
/// byte Little endian]`
#[derive(Clone)]
pub struct BundleDirectory {
    file: FileSlice,
    file_offsets: BundleStorageFileOffsets,
}

impl Debug for BundleDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BundleDirectory")
    }
}

/// Loads the split footer from a storage and path.
/// Returns (SplitFooter, BundleFooter)
/// SplitFooter [BundleMetadata, BundleMetadata Len, Hotcache, Hotcache len]
/// BundleFooter [BundleMetadata, BundleMetadata Len]
pub async fn read_split_footer(
    storage: Arc<dyn Storage>,
    path: &Path,
) -> StorageResult<(OwnedBytes, OwnedBytes)> {
    let file_len = storage.file_num_bytes(path).await? as usize;

    let hotcache_len_bytes = storage.get_slice(path, file_len - 8..file_len).await?;
    let hotcache_len = u64::from_le_bytes(hotcache_len_bytes.as_ref().try_into().unwrap()) as usize;

    let second_footer_start = file_len - 8 - hotcache_len - 8;
    let second_footer_bytes = storage
        .get_slice(path, second_footer_start..second_footer_start + 8)
        .await?;
    let second_footer_len =
        u64::from_le_bytes(second_footer_bytes.as_ref().try_into().unwrap()) as usize;

    let split_footer = storage
        .get_slice(path, second_footer_start - second_footer_len..file_len)
        .await?;
    let only_bundle_footer = split_footer.slice(0..second_footer_len + 8);

    Ok((split_footer, only_bundle_footer))
}

/// Return two slices for given split: `[body and bundle meta data] [hotcache]`
fn split_footer(file_slice: FileSlice) -> io::Result<(FileSlice, FileSlice)> {
    let (body_and_footer_slice, footer_len_slice) = file_slice.split_from_end(8);
    let footer_len_bytes = footer_len_slice.read_bytes()?;
    let footer_len = u64::from_le_bytes(footer_len_bytes.as_slice().try_into().unwrap());
    Ok(body_and_footer_slice.split_from_end(footer_len as usize))
}

/// Return two slices for given split: `[body and bundle meta data] [hotcache]`
pub fn get_hotcache_from_split(data: OwnedBytes) -> io::Result<OwnedBytes> {
    let split_file = FileSlice::new(Arc::new(data));
    let (_, hotcache) = split_footer(split_file)?;
    hotcache.read_bytes()
}

impl BundleDirectory {
    /// Get files and their sizes in a split.
    pub fn get_stats_split(data: OwnedBytes) -> io::Result<Vec<(PathBuf, u64)>> {
        let split_file = FileSlice::new(Arc::new(data));
        let (body_and_bundle_metadata, hot_cache) = split_footer(split_file)?;
        let file_offsets = BundleStorageFileOffsets::open(body_and_bundle_metadata)?;

        let mut files_and_size: Vec<(_, _)> = file_offsets
            .files
            .into_iter()
            .map(|(file, range)| (file, range.end - range.start))
            .collect();

        files_and_size.push((
            PathBuf::from("hotcache".to_string()),
            hot_cache.len() as u64,
        ));

        files_and_size.sort();
        Ok(files_and_size)
    }

    /// Opens a split file.
    pub fn open_split(split_file: FileSlice) -> io::Result<BundleDirectory> {
        // First we remove the hotcache from our file slice.
        let (body_and_bundle_metadata, _hot_cache) = split_footer(split_file)?;
        BundleDirectory::open_bundle(body_and_bundle_metadata)
    }

    /// Opens a BundleDirectory, given a file containing the bundle data.
    pub fn open_bundle(file: FileSlice) -> io::Result<BundleDirectory> {
        let file_offsets = BundleStorageFileOffsets::open(file.clone())?;
        Ok(BundleDirectory { file, file_offsets })
    }
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let byte_range = self
            .file_offsets
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(self
            .file
            .slice(byte_range.start as usize..byte_range.end as usize))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        let payload = file_slice
            .read_bytes()
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        Ok(payload.to_vec())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.file_offsets.exists(path))
    }

    crate::read_only_directory!();
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use quickwit_storage::{PutPayload, SplitPayloadBuilder};

    use super::*;

    #[tokio::test]
    async fn test_bundle_directory_stats() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            ],
        )?;

        let buffer = split_streamer.read_all().await?;

        // check stats
        let stats = BundleDirectory::get_stats_split(buffer)?;

        assert_eq!(stats[0], (PathBuf::from("f1".to_string()), 2_u64));
        assert_eq!(stats[1], (PathBuf::from("f2".to_string()), 3_u64));
        assert_eq!(stats[2], (PathBuf::from("hotcache".to_string()), 18_u64));

        Ok(())
    }

    #[tokio::test]
    async fn test_bundle_directory() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            ],
        )?;

        let buffer = split_streamer.read_all().await?;

        let bundle_file_slice = FileSlice::from(buffer.to_vec());

        let bundle_dir = BundleDirectory::open_split(bundle_file_slice)?;

        assert!(bundle_dir.exists(Path::new("f1")).unwrap());
        assert!(bundle_dir.exists(Path::new("f2")).unwrap());
        assert!(!bundle_dir.exists(Path::new("f3")).unwrap());

        let f1_data = bundle_dir.atomic_read(Path::new("f1"))?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_dir.atomic_read(Path::new("f2"))?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_split_to_bundle_and_open() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        let split_streamer = SplitPayloadBuilder::get_split_payload(
            &[test_filepath1.clone(), test_filepath2.clone()],
            &[1, 2, 3],
        )?;

        let data = split_streamer.read_all().await?;

        let bundle_dir = BundleDirectory::open_split(FileSlice::from(data.to_vec()))?;

        let f1_data = bundle_dir.atomic_read(Path::new("f1"))?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_dir.atomic_read(Path::new("f2"))?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        Ok(())
    }
}
