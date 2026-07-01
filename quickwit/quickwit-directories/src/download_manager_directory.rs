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

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use quickwit_storage::{Resolution, SplitDownloadView};
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, OwnedBytes};
use tantivy::{Directory, HasLen};
use tracing::{error, instrument};

use crate::hot_directory::{StaticDirectoryCache, StaticSliceCache};

/// A tantivy [`Directory`] backed by the storage download manager.
///
/// It replaces the historical `StorageDirectory → CachingDirectory → HotDirectory`
/// tower with a single thin adapter:
/// - the static hotcache (built at index time, keyed by logical file) is consulted first,
///   synchronously;
/// - otherwise reads are delegated to the synchronous [`SplitDownloadView::resolve`] (for tantivy's
///   sync `read_bytes`) or the thin async [`SplitDownloadView::get`] (for warmup).
///
/// As with the former `StorageDirectory`, a synchronous read of a range that was
/// not warmed up (and is not in the hotcache) is a loud error rather than a silent
/// blocking download — search must warm up everything it reads.
#[derive(Clone)]
pub struct DownloadManagerDirectory {
    inner: Arc<InnerDownloadManagerDirectory>,
}

struct InnerDownloadManagerDirectory {
    view: Arc<SplitDownloadView>,
    hotcache: Arc<StaticDirectoryCache>,
}

impl DownloadManagerDirectory {
    /// Opens a directory over the given per-split download view, using the
    /// split's static hotcache bytes (the same bytes the former `HotDirectory`
    /// consumed).
    pub fn open(
        view: Arc<SplitDownloadView>,
        hotcache_bytes: OwnedBytes,
    ) -> anyhow::Result<DownloadManagerDirectory> {
        let hotcache = Arc::new(StaticDirectoryCache::open(hotcache_bytes)?);
        Ok(DownloadManagerDirectory {
            inner: Arc::new(InnerDownloadManagerDirectory { view, hotcache }),
        })
    }

    /// Returns all the files in the directory and their sizes.
    ///
    /// The actual cached data is a very small fraction of this length. This is
    /// the replacement for `HotDirectory::get_file_lengths`, used to estimate the
    /// in-memory index size.
    pub fn get_file_lengths(&self) -> Vec<(PathBuf, u64)> {
        self.inner.hotcache.get_file_lengths()
    }
}

impl fmt::Debug for DownloadManagerDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DownloadManagerDirectory")
    }
}

struct DownloadManagerFileHandle {
    view: Arc<SplitDownloadView>,
    static_cache: Arc<StaticSliceCache>,
    path: PathBuf,
    file_length: u64,
}

impl fmt::Debug for DownloadManagerFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DownloadManagerFileHandle({:?})", &self.path)
    }
}

impl HasLen for DownloadManagerFileHandle {
    fn len(&self) -> usize {
        self.file_length as usize
    }
}

#[async_trait]
impl FileHandle for DownloadManagerFileHandle {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(found_bytes) = self.static_cache.try_read_bytes(byte_range.clone()) {
            return Ok(found_bytes);
        }
        match self.view.resolve(&self.path, byte_range) {
            Resolution::Hit(bytes) => Ok(bytes),
            Resolution::Miss(_) => Err(unsupported_sync_read(&self.path)),
        }
    }

    #[instrument(level = "debug", fields(path = %self.path.to_string_lossy(), byte_range_size = byte_range.end - byte_range.start), skip(self))]
    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(found_bytes) = self.static_cache.try_read_bytes(byte_range.clone()) {
            return Ok(found_bytes);
        }
        let bytes = self.view.get(&self.path, byte_range).await?;
        Ok(bytes)
    }
}

/// Error returned when a synchronous read hits a range that was neither in the
/// static hotcache nor warmed up. Mirrors the former `StorageDirectory`
/// behavior: such a read is a bug, not a silent blocking download.
fn unsupported_sync_read(path: &Path) -> io::Error {
    let error = "unsupported operation: synchronous read of an un-warmed byte range";
    error!(error, ?path);
    io::Error::other(format!("{error}: {}", path.display()))
}

impl Directory for DownloadManagerDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_length = self
            .inner
            .hotcache
            .get_file_length(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_owned()))?;
        Ok(Arc::new(DownloadManagerFileHandle {
            view: self.inner.view.clone(),
            static_cache: self.inner.hotcache.get_slice(path),
            path: path.to_owned(),
            file_length,
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.inner.hotcache.get_file_length(path).is_some())
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let slice_cache = self.inner.hotcache.get_slice(path);
        if let Some(all_bytes) = slice_cache.try_read_all() {
            return Ok(all_bytes.as_slice().to_owned());
        }
        // Fallback for files not fully present in the hotcache: serve from the
        // synchronous cache tiers (must be warmed). This matches the former
        // tower, where a non-hotcached atomic_read fell through to a sync read
        // that errored if the data was not already cached.
        let file_length = self
            .inner
            .hotcache
            .get_file_length(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_owned()))?;
        match self.inner.view.resolve(path, 0..file_length as usize) {
            Resolution::Hit(bytes) => Ok(bytes.as_slice().to_owned()),
            Resolution::Miss(_) => Err(OpenReadError::wrap_io_error(
                unsupported_sync_read(path),
                path.to_owned(),
            )),
        }
    }

    crate::read_only_directory!();
}
