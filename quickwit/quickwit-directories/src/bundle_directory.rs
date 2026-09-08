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

use std::convert::TryInto;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use quickwit_storage::{
    BundleFileRanges, OwnedBytes, Storage, StorageErrorKind, StorageResult,
    locate_split_footer_range, strip_split_footer_trailer,
};
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, FileSlice};
use tantivy::{Directory, HasLen};

/// `BundleDirectory` is a read-only directory that opens a "split bundle" and serves its files
/// through Tantivy's [`Directory`] interface.
///
/// It is the `Directory` equivalent of `BundleStorage`.
///
/// A split has the following layout (all integer fields are little-endian):
///
/// ```text
/// Split
/// ├── Bundle
/// │   ├── Files
/// │   │   ├── Tantivy index files
/// │   │   ├── split_fields
/// │   │   └── split_recovery_metadata (optional)
/// │   └── Bundle footer
/// │       ├── Bundle metadata (`BundleFileRanges` serialized as versioned JSON)
/// │       └── Bundle metadata length (u32)
/// ├── Hotcache
/// ├── Hotcache length (u32)
/// └── Split footer trailer (optional)
///     ├── Split footer start (u64)
///     ├── Trailer version (u32)
///     └── Magic bytes (`QWFT`)
/// ```
///
/// In compact form:
///
/// ```text
/// Bundle = [files][bundle metadata][bundle metadata length: 4-byte u32 LE]
/// Split  = [bundle][hotcache][hotcache length: 4-byte u32 LE][optional trailer]
/// Trailer = [split footer start: 8-byte u64 LE][version: 4-byte u32 LE][QWFT]
/// ```
///
/// The split footer starts at the bundle footer, so it contains the bundle footer, hotcache, and
/// hotcache length. The optional trailer points to that start offset but is not part of the split
/// footer returned by [`read_split_footer`].
#[derive(Clone)]
pub struct BundleDirectory {
    file: FileSlice,
    file_ranges: BundleFileRanges,
}

fn read_u32_le(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(
        bytes
            .try_into()
            .expect("slice should be exactly 4-byte long"),
    )
}

impl Debug for BundleDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BundleDirectory")
    }
}

/// Loads the split footer and its nested bundle footer from storage.
///
/// The first returned slice is the complete split footer:
///
/// ```text
/// [bundle metadata (`BundleFileRanges`)][bundle metadata length][hotcache][hotcache length]
/// ```
///
/// The second returned slice is its bundle footer:
///
/// ```text
/// [bundle metadata (`BundleFileRanges`)][bundle metadata length]
/// ```
pub async fn read_split_footer(
    storage: Arc<dyn Storage>,
    path: &Path,
) -> StorageResult<(OwnedBytes, OwnedBytes)> {
    let split_len = storage.file_num_bytes(path).await?;
    let footer_range = locate_split_footer_range(storage.as_ref(), path, split_len)
        .await
        .map_err(|error| StorageErrorKind::Internal.with_error(error))?;
    let split_footer_with_trailer = storage
        .get_slice(path, footer_range.start as usize..footer_range.end as usize)
        .await?;
    let split_footer =
        strip_split_footer_trailer(FileSlice::new(Arc::new(split_footer_with_trailer)))
            .and_then(|footer| footer.read_bytes().map_err(anyhow::Error::from))
            .map_err(|error| StorageErrorKind::Internal.with_error(error))?;

    let hotcache_len = read_u32_le(&split_footer[split_footer.len() - 4..]) as usize;
    let metadata_len_offset = split_footer.len() - 4 - hotcache_len - 4;
    let metadata_len =
        read_u32_le(&split_footer[metadata_len_offset..metadata_len_offset + 4]) as usize;
    let bundle_footer = split_footer.slice(0..metadata_len + 4);

    Ok((split_footer, bundle_footer))
}

/// Splits a complete split into `[bundle][hotcache]`, discarding the hotcache length and optional
/// split footer trailer.
fn split_footer(split_slice: FileSlice) -> io::Result<(FileSlice, FileSlice)> {
    let split_footer_slice = strip_split_footer_trailer(split_slice).map_err(io::Error::other)?;
    let (body_and_footer_slice, footer_len_slice) = split_footer_slice.split_from_end(4);
    let footer_len_bytes = footer_len_slice.read_bytes()?;
    let footer_len = read_u32_le(footer_len_bytes.as_slice());
    Ok(body_and_footer_slice.split_from_end(footer_len as usize))
}

/// Extracts the hotcache from a complete split.
pub fn get_hotcache_from_split(split_bytes: OwnedBytes) -> io::Result<OwnedBytes> {
    let split_slice = FileSlice::new(Arc::new(split_bytes));
    let (_, hotcache) = split_footer(split_slice)?;
    hotcache.read_bytes()
}

impl BundleDirectory {
    /// Get files and their sizes in a split.
    pub fn get_stats_split(data: OwnedBytes) -> anyhow::Result<Vec<(PathBuf, u64)>> {
        let split_file = FileSlice::new(Arc::new(data));
        let (body_and_bundle_metadata, hot_cache) = split_footer(split_file)?;
        let file_ranges = BundleFileRanges::open(body_and_bundle_metadata)?;

        let mut files_and_sizes: Vec<(_, _)> = file_ranges
            .files
            .into_iter()
            .map(|(file, range)| (file, range.end - range.start))
            .collect();

        files_and_sizes.push((
            PathBuf::from("hotcache".to_string()),
            hot_cache.len() as u64,
        ));

        files_and_sizes.sort();
        Ok(files_and_sizes)
    }

    /// Opens a split file.
    pub fn open_split(split_file: FileSlice) -> io::Result<BundleDirectory> {
        // First we remove the hotcache from our file slice.
        let (body_and_bundle_metadata, _hot_cache) = split_footer(split_file)?;
        BundleDirectory::open_bundle(body_and_bundle_metadata).map_err(io::Error::other)
    }

    /// Opens a BundleDirectory, given a file containing the bundle data.
    pub fn open_bundle(file: FileSlice) -> anyhow::Result<BundleDirectory> {
        let file_ranges = BundleFileRanges::open(file.clone())?;
        Ok(BundleDirectory { file, file_ranges })
    }
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let byte_range = self
            .file_ranges
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
        Ok(self.file_ranges.exists(path))
    }

    crate::read_only_directory!();
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use quickwit_common::shared_consts::SPLIT_FIELDS_FILE_NAME;
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
            &[],
            None,
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
            &[],
            None,
            &[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            ],
        )?;

        let buffer = split_streamer.read_all().await?;

        let bundle_slice = FileSlice::from(buffer.to_vec());

        let bundle_dir = BundleDirectory::open_split(bundle_slice)?;

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
            &[5, 5, 5],
            None,
            &[1, 2, 3],
        )?;
        let footer_start = split_streamer.footer_range.start;

        let split_bytes = split_streamer.read_all().await?;
        let split_slice = FileSlice::new(Arc::new(split_bytes));

        let bundle_dir = BundleDirectory::open_split(split_slice.clone())?;

        let field_data = bundle_dir.atomic_read(Path::new(SPLIT_FIELDS_FILE_NAME))?;
        assert_eq!(&*field_data, &[5, 5, 5]);

        let f1_data = bundle_dir.atomic_read(Path::new("f1"))?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_dir.atomic_read(Path::new("f2"))?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        // Phase-one readers must also accept the trailer format before writers enable it.
        let split_footer = strip_split_footer_trailer(split_slice)?.read_bytes()?;
        let mut split_footer_with_trailer = split_footer.to_vec();
        split_footer_with_trailer.extend_from_slice(&footer_start.to_le_bytes());
        split_footer_with_trailer.extend_from_slice(&1u32.to_le_bytes());
        split_footer_with_trailer.extend_from_slice(b"QWFT");

        let bundle_dir_with_trailer =
            BundleDirectory::open_split(FileSlice::from(split_footer_with_trailer))?;
        assert_eq!(
            bundle_dir_with_trailer.atomic_read(Path::new(SPLIT_FIELDS_FILE_NAME))?,
            [5, 5, 5]
        );
        Ok(())
    }
}
