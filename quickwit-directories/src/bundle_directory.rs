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
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::{fmt, io};

use quickwit_storage::BundleStorageFileOffsets;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{FileHandle, FileSlice, WatchCallback, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen};

/// BundleDirectory is a read-only directory that makes it possible to
/// open a split and serve the file it contains via tantivy's `Directory`.
///
/// It is the `Directory` equivalent of `BundleStorage`.
#[derive(Clone)]
pub struct BundleDirectory {
    file: FileSlice,
    file_offsets: BundleStorageFileOffsets,
}

impl Debug for BundleDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BundleDirectory")
    }
}

fn split_footer(file_slice: FileSlice) -> io::Result<(FileSlice, FileSlice)> {
    let (body_and_footer_slice, footer_len_slice) = file_slice.split_from_end(8);
    let footer_len_bytes = footer_len_slice.read_bytes()?;
    let footer_len = u64::from_le_bytes(footer_len_bytes.as_slice().try_into().unwrap());
    Ok(body_and_footer_slice.split_from_end(footer_len as usize))
}

impl BundleDirectory {
    /// Get files and their sizes in a split.
    pub fn get_stats_split(split_file: FileSlice) -> io::Result<HashMap<PathBuf, usize>> {
        // First we remove the hotcache from our file slice.
        let (body_and_bundle_metadata, hot_cache) = split_footer(split_file)?;
        let file_offsets =
            BundleStorageFileOffsets::open_from_file_slice(body_and_bundle_metadata)?;

        let mut files_and_size: HashMap<_, _> = file_offsets
            .files
            .into_iter()
            .map(|(file, range)| (file, range.end - range.start))
            .collect();

        files_and_size.insert(PathBuf::from("hotcache".to_string()), hot_cache.len());
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
        let file_offsets = BundleStorageFileOffsets::open_from_file_slice(file.clone())?;
        Ok(BundleDirectory { file, file_offsets })
    }
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Box::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let byte_range = self
            .file_offsets
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(self.file.slice(byte_range))
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

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        unimplemented!();
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        if self.file_offsets.exists(path) {
            unimplemented!("the bundle directory is read-only");
        } else {
            // We actually handle delete docs that are not there,
            // in order to be used in the union directory.
            Err(DeleteError::FileDoesNotExist(path.to_path_buf()))
        }
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        unimplemented!();
    }

    fn watch(&self, _callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use quickwit_storage::BundleStorageBuilder;
    use tantivy::common::CountingWriter;

    use super::*;

    #[test]
    fn test_bundle_directory_stats() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let test_filepath1 = temp_dir.path().join("f1");
        let test_filepath2 = temp_dir.path().join("f2");
        let test_bundle_path = temp_dir.path().join("le_bundle");

        // Simulating split file which is created in packager.rs, which contains the bundle and
        // appends the footer. The bundle directory can read the format created by
        // packager.rs
        let mut split_file = CountingWriter::wrap(File::create(test_bundle_path.to_owned())?);
        let mut bundle_builder = BundleStorageBuilder::new(&mut split_file)?;

        let mut file1 = File::create(&test_filepath1)?;
        file1.write_all(&[123, 76])?;

        let mut file2 = File::create(&test_filepath2)?;
        file2.write_all(&[99, 55, 44])?;

        bundle_builder.add_file(&test_filepath1)?;
        bundle_builder.add_file(&test_filepath2)?;
        bundle_builder.finalize()?;

        // append hotcache and hotcache len
        let hotcache_offset_start = split_file.written_bytes();
        split_file.write_all(&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ])?;

        let hotcache_offset_end = split_file.written_bytes();
        let hotcache_num_bytes = hotcache_offset_end - hotcache_offset_start;

        split_file.write_all(&hotcache_num_bytes.to_le_bytes())?;

        let buffer = fs::read(test_bundle_path)?;
        let bundle_file_slice = FileSlice::from(buffer);

        // check stats
        let stats = BundleDirectory::get_stats_split(bundle_file_slice)?;

        assert_eq!(
            *stats.get(&PathBuf::from("hotcache".to_string())).unwrap(),
            18_usize
        );
        assert_eq!(
            *stats.get(&PathBuf::from("f1".to_string())).unwrap(),
            2_usize
        );
        assert_eq!(
            *stats.get(&PathBuf::from("f2".to_string())).unwrap(),
            3_usize
        );

        Ok(())
    }

    #[test]
    fn test_bundle_directory() -> anyhow::Result<()> {
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

        let buffer = fs::read(test_bundle_path)?;
        let bundle_file_slice = FileSlice::from(buffer);

        let bundle_dir = BundleDirectory::open_bundle(bundle_file_slice.clone())?;

        assert!(bundle_dir.exists(Path::new("f1")).unwrap());
        assert!(bundle_dir.exists(Path::new("f2")).unwrap());
        assert!(!bundle_dir.exists(Path::new("f3")).unwrap());

        let f1_data = bundle_dir.atomic_read(Path::new("f1"))?;
        assert_eq!(&*f1_data, &[123u8, 76u8]);

        let f2_data = bundle_dir.atomic_read(Path::new("f2"))?;
        assert_eq!(&f2_data[..], &[99, 55, 44]);

        // check stats
        // let stats = BundleDirectory::get_stats_split(bundle_file_slice)?;

        Ok(())
    }
}
