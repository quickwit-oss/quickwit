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

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use quickwit_cache::CacheMetrics;
use serde::{Deserialize, Serialize};
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, FileSlice, OwnedBytes};
use tantivy::error::DataCorruption;
use tantivy::{AsyncIoResult, Directory, HasLen, Index, IndexReader, ReloadPolicy};

use crate::{CachingDirectory, DebugProxyDirectory};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SliceCacheIndexEntry {
    start: usize, //< legacy. We keep this instead of range due to existing indices.
    stop: usize,
    addr: usize,
}

impl SliceCacheIndexEntry {
    pub fn len(&self) -> usize {
        self.range().len()
    }

    pub fn range(&self) -> Range<usize> {
        self.start..self.stop
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct SliceCacheIndex {
    total_len: u64,
    slices: Vec<SliceCacheIndexEntry>,
}
impl SliceCacheIndex {
    pub fn is_complete(&self) -> bool {
        if self.slices.len() != 1 {
            return false;
        }
        self.slices[0].len() as u64 == self.total_len
    }

    pub fn get(&self, byte_range: Range<usize>) -> Option<usize> {
        let entry_idx = match self
            .slices
            .binary_search_by_key(&byte_range.start, |entry| entry.range().start)
        {
            Ok(idx) => idx,
            Err(0) => {
                return None;
            }
            Err(idx_after) => idx_after - 1,
        };
        let entry = &self.slices[entry_idx];
        if entry.range().start > byte_range.start || entry.range().end < byte_range.end {
            return None;
        }
        Some(entry.addr + byte_range.start - entry.range().start)
    }
}

#[derive(Default)]
struct StaticDirectoryCacheBuilder {
    file_cache_builder: HashMap<PathBuf, StaticSliceCacheBuilder>,
    file_lengths: HashMap<PathBuf, u64>, // a mapping from file path to file size in bytes
}

impl StaticDirectoryCacheBuilder {
    pub fn add_file(&mut self, path: &Path, file_len: u64) -> &mut StaticSliceCacheBuilder {
        self.file_lengths.insert(path.to_owned(), file_len);
        self.file_cache_builder
            .entry(path.to_owned())
            .or_insert_with(|| StaticSliceCacheBuilder::new(file_len))
    }

    /// Flush needs to be called afterwards.
    pub fn write(self, wrt: &mut dyn io::Write) -> tantivy::Result<()> {
        // Write format version
        wrt.write_all(b"\x00")?;

        let file_lengths_bytes = serde_cbor::to_vec(&self.file_lengths).unwrap();
        wrt.write_all(&(file_lengths_bytes.len() as u64).to_le_bytes())?;
        wrt.write_all(&file_lengths_bytes[..])?;

        let mut data_buffer = Vec::new();
        let mut data_idx: Vec<(PathBuf, u64)> = Vec::new();
        let mut offset = 0u64;
        for (path, cache) in self.file_cache_builder {
            let buf = cache.flush()?;
            data_idx.push((path, offset));
            offset += buf.len() as u64;
            data_buffer.extend_from_slice(&buf);
        }
        let idx_bytes = serde_cbor::to_vec(&data_idx).unwrap();
        wrt.write_all(&(idx_bytes.len() as u64).to_le_bytes())?;
        wrt.write_all(&idx_bytes[..])?;
        wrt.write_all(&data_buffer[..])?;

        Ok(())
    }
}

fn deserialize_cbor<T>(bytes: &mut OwnedBytes) -> serde_cbor::Result<T>
where T: serde::de::DeserializeOwned {
    let len = bytes.read_u64();
    let value = serde_cbor::from_reader(&bytes.as_slice()[..len as usize]);
    bytes.advance(len as usize);
    value
}

#[derive(Debug)]
struct StaticDirectoryCache {
    file_lengths: HashMap<PathBuf, u64>,
    slices: HashMap<PathBuf, Arc<StaticSliceCache>>,
}

impl StaticDirectoryCache {
    pub fn open(mut bytes: OwnedBytes) -> tantivy::Result<StaticDirectoryCache> {
        let format_version = bytes.read_u8();

        if format_version != 0 {
            return Err(tantivy::TantivyError::DataCorruption(
                DataCorruption::comment_only(format!(
                    "Format version not supported: `{}`",
                    format_version
                )),
            ));
        }

        let file_lengths: HashMap<PathBuf, u64> = deserialize_cbor(&mut bytes).unwrap();

        let mut slice_offsets: Vec<(PathBuf, u64)> = deserialize_cbor(&mut bytes).unwrap();
        slice_offsets.push((PathBuf::default(), bytes.len() as u64));

        let slices = slice_offsets
            .windows(2)
            .map(|slice_offsets_window| {
                let path = slice_offsets_window[0].0.clone();
                let start = slice_offsets_window[0].1 as usize;
                let end = slice_offsets_window[1].1 as usize;
                StaticSliceCache::open(bytes.slice(start..end)).map(|s| (path, Arc::new(s)))
            })
            .collect::<tantivy::Result<_>>()?;

        Ok(StaticDirectoryCache {
            file_lengths,
            slices,
        })
    }

    pub fn get_slice(&self, path: &Path) -> Arc<StaticSliceCache> {
        self.slices.get(path).cloned().unwrap_or_default()
    }

    pub fn get_file_length(&self, path: &Path) -> Option<u64> {
        self.file_lengths.get(path).map(u64::clone)
    }

    /// return the files and their cached lengths
    pub fn get_stats(&self) -> Vec<(PathBuf, usize)> {
        let mut entries = self
            .slices
            .iter()
            .map(|(path, cache)| (path.to_owned(), cache.len()))
            .collect::<Vec<_>>();

        entries.sort_by_key(|el| el.0.to_owned());
        entries
    }
}

/// A SliceCache is a static toring
pub struct StaticSliceCache {
    bytes: OwnedBytes,
    index: SliceCacheIndex,
}

impl Default for StaticSliceCache {
    fn default() -> StaticSliceCache {
        StaticSliceCache {
            bytes: OwnedBytes::empty(),
            index: SliceCacheIndex::default(),
        }
    }
}

impl StaticSliceCache {
    pub fn open(owned_bytes: OwnedBytes) -> tantivy::Result<Self> {
        let owned_bytes_len = owned_bytes.len();
        assert!(owned_bytes_len >= 8);
        let (body, len_bytes) = owned_bytes.split(owned_bytes_len - 8);
        let mut body_len_bytes = [0u8; 8];
        body_len_bytes.copy_from_slice(len_bytes.as_slice());
        let body_len = u64::from_le_bytes(body_len_bytes);
        let (body, idx) = body.split(body_len as usize);
        let mut idx_bytes = idx.as_slice();
        let index: SliceCacheIndex = serde_cbor::from_reader(&mut idx_bytes).map_err(|err| {
            DataCorruption::comment_only(format!(
                "Failed to deserialize the slice index: {:?}",
                err
            ))
        })?;
        Ok(StaticSliceCache { bytes: body, index })
    }

    pub fn try_read_all(&self) -> Option<OwnedBytes> {
        if !self.index.is_complete() {
            return None;
        }
        Some(self.bytes.clone())
    }

    pub fn try_read_bytes(&self, byte_range: Range<usize>) -> Option<OwnedBytes> {
        if byte_range.is_empty() {
            return Some(OwnedBytes::empty());
        }
        if let Some(start) = self.index.get(byte_range.clone()) {
            return Some(self.bytes.slice(start..start + byte_range.len()));
        }
        None
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }
}

struct StaticSliceCacheBuilder {
    wrt: Vec<u8>,
    slices: Vec<SliceCacheIndexEntry>,
    offset: u64,
    total_len: u64,
}

impl StaticSliceCacheBuilder {
    pub fn new(total_len: u64) -> StaticSliceCacheBuilder {
        StaticSliceCacheBuilder {
            wrt: Vec::new(),
            slices: Vec::new(),
            offset: 0u64,
            total_len,
        }
    }

    pub fn add_bytes(&mut self, bytes: &[u8], start: usize) {
        self.wrt.extend_from_slice(bytes);
        let end = start + bytes.len();
        self.slices.push(SliceCacheIndexEntry {
            start,
            stop: end,
            addr: self.offset as usize,
        });
        self.offset += bytes.len() as u64;
    }

    fn merged_slices(&mut self) -> tantivy::Result<Vec<SliceCacheIndexEntry>> {
        if self.slices.is_empty() {
            return Ok(Vec::new());
        }
        self.slices.sort_unstable_by_key(|e| e.range().start);
        let mut slices = Vec::with_capacity(self.slices.len());
        let mut last = self.slices[0].clone();
        for segment in &self.slices[1..] {
            if segment.range().start < last.range().end {
                return Err(tantivy::TantivyError::InvalidArgument(format!(
                    "Two segments are overlapping on byte {}",
                    segment.range().start
                )));
            }
            if last.stop == segment.range().start
                && (last.addr + last.range().len() == segment.addr)
            {
                // We merge the current segment with the previous one
                last.stop += segment.range().len();
            } else {
                slices.push(last);
                last = segment.clone();
            }
        }
        slices.push(last);
        Ok(slices)
    }

    pub fn flush(mut self) -> tantivy::Result<Vec<u8>> {
        let merged_slices = self.merged_slices()?;
        let slices_idx = SliceCacheIndex {
            total_len: self.total_len,
            slices: merged_slices,
        };
        serde_cbor::to_writer(&mut self.wrt, &slices_idx)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        self.wrt.extend_from_slice(&self.offset.to_le_bytes()[..]);
        Ok(self.wrt)
    }
}

impl fmt::Debug for StaticSliceCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SliceCache()")
    }
}

/// The hot directory accelerates a given directory,
/// by placing a static cache in front of a directory.
///
/// The `HotDirectory` does not implement write operations. It has been
/// designed for quickwit in order to regroup all of the small random
/// read operations required to open an index.
/// All of these operations are gather into a single file called the
/// hotcache.
#[derive(Clone)]
pub struct HotDirectory {
    inner: Arc<InnerHotDirectory>,
}

impl HotDirectory {
    /// Wraps an index, with a static cache serialized into `hot_cache_bytes`.
    pub fn open<D: Directory>(
        underlying: D,
        hot_cache_bytes: OwnedBytes,
    ) -> tantivy::Result<HotDirectory> {
        let static_cache = StaticDirectoryCache::open(hot_cache_bytes)?;
        Ok(HotDirectory {
            inner: Arc::new(InnerHotDirectory {
                underlying: Box::new(underlying),
                cache: Arc::new(static_cache),
            }),
        })
    }
    /// Get files and their cached sizes.
    pub fn get_stats_per_file(
        hot_cache_bytes: OwnedBytes,
    ) -> tantivy::Result<Vec<(PathBuf, usize)>> {
        let static_cache = StaticDirectoryCache::open(hot_cache_bytes)?;
        Ok(static_cache.get_stats())
    }
}

struct FileSliceWithCache {
    underlying: FileSlice,
    static_cache: Arc<StaticSliceCache>,
    file_length: u64,
}

#[async_trait]
impl FileHandle for FileSliceWithCache {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(found_bytes) = self.static_cache.try_read_bytes(byte_range.clone()) {
            return Ok(found_bytes);
        }
        self.underlying.read_bytes_slice(byte_range)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> AsyncIoResult<OwnedBytes> {
        if let Some(found_bytes) = self.static_cache.try_read_bytes(byte_range.clone()) {
            return Ok(found_bytes);
        }
        self.underlying.read_bytes_slice_async(byte_range).await
    }
}

impl fmt::Debug for FileSliceWithCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSliceWithCache({:?})", &self.underlying)
    }
}

impl HasLen for FileSliceWithCache {
    fn len(&self) -> usize {
        self.file_length as usize
    }
}

struct InnerHotDirectory {
    underlying: Box<dyn Directory>,
    cache: Arc<StaticDirectoryCache>,
}

impl fmt::Debug for HotDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HotDirectory(dir={:?}, cache={:?})",
            self.inner.underlying.as_ref(),
            self.inner.cache.as_ref()
        )
    }
}

impl Directory for HotDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_length = self
            .inner
            .cache
            .get_file_length(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_owned()))?;
        let underlying_filehandle = self.inner.underlying.get_file_handle(path)?;
        let underlying = FileSlice::new_with_num_bytes(underlying_filehandle, file_length as usize);
        let file_slice_with_cache = FileSliceWithCache {
            underlying,
            static_cache: self.inner.cache.get_slice(path),
            file_length,
        };
        Ok(Arc::new(file_slice_with_cache))
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, OpenReadError> {
        Ok(self.inner.cache.get_file_length(path).is_some())
    }

    fn atomic_read(&self, path: &std::path::Path) -> Result<Vec<u8>, OpenReadError> {
        let slice_cache = self.inner.cache.get_slice(path);
        if let Some(all_bytes) = slice_cache.try_read_all() {
            return Ok(all_bytes.as_slice().to_owned());
        }
        self.inner.underlying.atomic_read(path)
    }

    crate::read_only_directory!();
}

fn list_index_files(index: &Index) -> tantivy::Result<HashSet<PathBuf>> {
    let index_meta = index.load_metas()?;
    let mut files: HashSet<PathBuf> = index_meta
        .segments
        .into_iter()
        .flat_map(|segment_meta| segment_meta.list_files())
        .collect();
    files.insert(Path::new("meta.json").to_path_buf());
    files.insert(Path::new(".managed.json").to_path_buf());
    Ok(files)
}

/// Given a tantivy directory, automatically identify the parts that should be loaded on startup
/// and writes a static cache file called hotcache in the `output`.
///
/// See [`HotDirectory`] for more information.
pub fn write_hotcache<D: Directory>(
    directory: D,
    output: &mut dyn io::Write,
    shortlived_cache: Option<&'static CacheMetrics>,
) -> tantivy::Result<()> {
    // We use the caching directory here in order to defensively ensure that
    // the content of the directory that will be written in the hotcache is precisely
    // the same that was read on the first pass.
    let caching_directory = CachingDirectory::new_unbounded(Arc::new(directory), shortlived_cache);
    let debug_proxy_directory = DebugProxyDirectory::wrap(caching_directory);
    let index = Index::open(debug_proxy_directory.clone())?;
    let schema = index.schema();
    let reader: IndexReader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    for (field, field_entry) in schema.fields() {
        if !field_entry.is_indexed() {
            continue;
        }
        for reader in searcher.segment_readers() {
            let _inv_idx = reader.inverted_index(field)?;
        }
    }
    let mut cache_builder = StaticDirectoryCacheBuilder::default();
    let read_operations = debug_proxy_directory.drain_read_operations();
    let mut per_file_slices: HashMap<PathBuf, HashSet<Range<usize>>> = HashMap::default();
    for read_operation in read_operations {
        per_file_slices
            .entry(read_operation.path)
            .or_default()
            .insert(read_operation.offset..read_operation.offset + read_operation.num_bytes);
    }
    let index_files = list_index_files(&index)?;
    for file_path in index_files {
        let file_slice_res = debug_proxy_directory.open_read(&file_path);
        if let Err(tantivy::directory::error::OpenReadError::FileDoesNotExist(_)) = file_slice_res {
            continue;
        }
        let file_slice = file_slice_res?;
        let file_cache_builder = cache_builder.add_file(&file_path, file_slice.len() as u64);
        if let Some(intervals) = per_file_slices.get(&file_path) {
            for byte_range in intervals {
                let len = byte_range.len();
                // We do not want to store slices that are too large in the hotcache,
                // but on the other hand, the term dictionray index and the docstore
                // index are required for quickwit to work.
                //
                // Warning: we need to work on string here because `Path::ends_with`
                // has very different semantics.
                let file_path_str = file_path.to_string_lossy();
                if file_path_str.ends_with("store")
                    || file_path_str.ends_with("term")
                    || len < 10_000_000
                {
                    let bytes = file_slice.read_bytes_slice(byte_range.clone())?;
                    file_cache_builder.add_bytes(bytes.as_slice(), byte_range.start);
                }
            }
        }
    }
    cache_builder.write(output)?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_empty_slice_cache_index() -> tantivy::Result<()> {
        let slice_cache_builder = StaticSliceCacheBuilder::new(10u64);
        let cache_data = slice_cache_builder.flush()?;
        let owned_bytes = OwnedBytes::new(cache_data);
        let slice_cache = StaticSliceCache::open(owned_bytes)?;
        assert!(slice_cache.try_read_bytes(5..6).is_none());
        Ok(())
    }

    #[test]
    fn test_simple_slice_cache_index() -> tantivy::Result<()> {
        let mut slice_cache_builder = StaticSliceCacheBuilder::new(10u64);
        slice_cache_builder.add_bytes(b"abc", 2);
        let cache_data = slice_cache_builder.flush()?;
        let owned_bytes = OwnedBytes::new(cache_data);
        let slice_cache = StaticSliceCache::open(owned_bytes)?;
        assert_eq!(
            slice_cache.try_read_bytes(2..5).unwrap().as_slice(),
            &b"abc"[..]
        );
        assert_eq!(
            slice_cache.try_read_bytes(2..3).unwrap().as_slice(),
            &b"a"[..]
        );
        assert_eq!(
            slice_cache.try_read_bytes(3..5).unwrap().as_slice(),
            &b"bc"[..]
        );
        assert_eq!(
            slice_cache.try_read_bytes(4..5).unwrap().as_slice(),
            &b"c"[..]
        );
        assert!(slice_cache.try_read_bytes(5..6).is_none());
        assert!(slice_cache.try_read_bytes(4..6).is_none());
        assert!(slice_cache.try_read_bytes(6..7).is_none());
        assert_eq!(
            slice_cache.try_read_bytes(6..6).unwrap().as_slice(),
            &b""[..]
        );
        Ok(())
    }

    #[test]
    fn test_several_segments() -> tantivy::Result<()> {
        let mut slice_cache_builder = StaticSliceCacheBuilder::new(100u64);
        slice_cache_builder.add_bytes(b"def", 6);
        slice_cache_builder.add_bytes(b"ghi", 12);
        slice_cache_builder.add_bytes(b"abc", 2);
        let cache_data = slice_cache_builder.flush()?;
        let owned_bytes = OwnedBytes::new(cache_data);
        let slice_cache = StaticSliceCache::open(owned_bytes)?;
        assert_eq!(
            slice_cache.try_read_bytes(2..5).unwrap().as_slice(),
            &b"abc"[..]
        );
        assert_eq!(
            slice_cache.try_read_bytes(2..3).unwrap().as_slice(),
            &b"a"[..]
        );
        assert_eq!(
            slice_cache.try_read_bytes(3..5).unwrap().as_slice(),
            &b"bc"[..]
        );
        assert_eq!(
            slice_cache.try_read_bytes(4..5).unwrap().as_slice(),
            &b"c"[..]
        );
        assert!(slice_cache.try_read_bytes(5..6).is_none());
        assert!(slice_cache.try_read_bytes(4..6).is_none());
        assert_eq!(
            slice_cache.try_read_bytes(6..7).unwrap().as_slice(),
            &b"d"[..]
        );
        assert!(slice_cache.try_read_bytes(2..7).is_none());
        Ok(())
    }

    #[test]
    fn test_slice_cache_merged_entries() -> tantivy::Result<()> {
        let mut slice_cache_builder = StaticSliceCacheBuilder::new(100u64);
        slice_cache_builder.add_bytes(b"abc", 2);
        slice_cache_builder.add_bytes(b"def", 5);
        let cache_data = slice_cache_builder.flush()?;
        let owned_bytes = OwnedBytes::new(cache_data);
        let slice_cache = StaticSliceCache::open(owned_bytes)?;
        assert_eq!(
            slice_cache.try_read_bytes(3..7).unwrap().as_slice(),
            &b"bcde"[..]
        );
        Ok(())
    }

    #[test]
    fn test_slice_cache_unmergeable_entries() -> tantivy::Result<()> {
        let mut slice_cache_builder = StaticSliceCacheBuilder::new(100u64);
        slice_cache_builder.add_bytes(b"def", 5);
        slice_cache_builder.add_bytes(b"abc", 2);
        let cache_data = slice_cache_builder.flush()?;
        let owned_bytes = OwnedBytes::new(cache_data);
        let slice_cache = StaticSliceCache::open(owned_bytes)?;
        assert!(slice_cache.try_read_bytes(3..7).is_none());
        Ok(())
    }

    #[test]
    fn test_slice_cache_overlapping_entries() {
        let mut slice_cache_builder = StaticSliceCacheBuilder::new(100u64);
        slice_cache_builder.add_bytes(b"abcd", 2);
        slice_cache_builder.add_bytes(b"def", 5);
        assert!(slice_cache_builder.flush().is_err());
    }

    #[test]
    fn test_slice_entry_serialization() -> anyhow::Result<()> {
        let slice_entry = super::SliceCacheIndexEntry {
            start: 1,
            stop: 5,
            addr: 4,
        };
        let bytes = serde_cbor::ser::to_vec(&slice_entry)?;
        assert_eq!(
            &bytes[..],
            &[
                163, 101, 115, 116, 97, 114, 116, 1, 100, 115, 116, 111, 112, 5, 100, 97, 100, 100,
                114, 4
            ]
        );
        Ok(())
    }

    #[test]
    fn test_slice_directory_cache() -> tantivy::Result<()> {
        let one_path = Path::new("one.txt");
        let two_path = Path::new("two.txt");
        let three_path = Path::new("three.txt");
        let four_path = Path::new("four.txt");

        let mut directory_cache_builder = StaticDirectoryCacheBuilder::default();
        directory_cache_builder
            .add_file(one_path, 100)
            .add_bytes(b" happy t", 5);
        directory_cache_builder
            .add_file(two_path, 200)
            .add_bytes(b"my name", 0);
        directory_cache_builder.add_file(three_path, 300);

        let mut buffer = Vec::new();
        directory_cache_builder.write(&mut buffer)?;
        let directory_cache = StaticDirectoryCache::open(OwnedBytes::new(buffer))?;

        assert_eq!(directory_cache.get_file_length(one_path), Some(100));
        assert_eq!(directory_cache.get_file_length(two_path), Some(200));
        assert_eq!(directory_cache.get_file_length(three_path), Some(300));
        assert_eq!(directory_cache.get_file_length(four_path), None);

        let stats = directory_cache.get_stats();
        assert_eq!(stats[0], (one_path.to_owned(), 8));
        assert_eq!(stats[1], (three_path.to_owned(), 0));
        assert_eq!(stats[2], (two_path.to_owned(), 7));

        assert_eq!(
            directory_cache
                .get_slice(one_path)
                .try_read_bytes(6..11)
                .unwrap()
                .as_ref(),
            b"happy"
        );
        assert_eq!(
            directory_cache
                .get_slice(two_path)
                .try_read_bytes(3..7)
                .unwrap()
                .as_ref(),
            b"name"
        );

        Ok(())
    }
}
