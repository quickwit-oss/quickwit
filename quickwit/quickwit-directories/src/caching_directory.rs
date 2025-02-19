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
use quickwit_storage::ByteRangeCache;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{FileHandle, OwnedBytes};
use tantivy::{Directory, HasLen};

/// The caching directory is a simple cache that wraps another directory.
#[derive(Clone)]
pub struct CachingDirectory {
    underlying: Arc<dyn Directory>,
    // TODO fixme: that's a pretty ugly cache we have here.
    cache: ByteRangeCache,
}

impl CachingDirectory {
    /// Creates a new CachingDirectory.
    ///
    /// Warning: The resulting CacheDirectory will cache all information without ever
    /// removing any item from the cache.
    pub fn new_unbounded(underlying: Arc<dyn Directory>) -> CachingDirectory {
        let byte_range_cache = ByteRangeCache::with_infinite_capacity(
            &quickwit_storage::STORAGE_METRICS.shortlived_cache,
        );
        CachingDirectory::new(underlying, byte_range_cache)
    }

    /// Creates a new CachingDirectory.
    ///
    /// Warning: The resulting CacheDirectory will cache all information without ever
    /// removing any item from the cache.
    pub fn new(underlying: Arc<dyn Directory>, cache: ByteRangeCache) -> CachingDirectory {
        CachingDirectory { underlying, cache }
    }
}

impl fmt::Debug for CachingDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CachingDirectory({:?})", self.underlying)
    }
}

struct CachingFileHandle {
    path: PathBuf,
    cache: ByteRangeCache,
    underlying_filehandle: Arc<dyn FileHandle>,
}

impl fmt::Debug for CachingFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CachingFileHandle(path={:?}, underlying={:?})",
            &self.path,
            self.underlying_filehandle.as_ref()
        )
    }
}

#[async_trait]
impl FileHandle for CachingFileHandle {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.cache.get_slice(&self.path, byte_range.clone()) {
            return Ok(bytes);
        }
        let owned_bytes = self.underlying_filehandle.read_bytes(byte_range.clone())?;
        self.cache
            .put_slice(self.path.clone(), byte_range, owned_bytes.clone());
        Ok(owned_bytes)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(owned_bytes) = self.cache.get_slice(&self.path, byte_range.clone()) {
            return Ok(owned_bytes);
        }
        let read_bytes = self
            .underlying_filehandle
            .read_bytes_async(byte_range.clone())
            .await?;
        self.cache
            .put_slice(self.path.clone(), byte_range, read_bytes.clone());
        Ok(read_bytes)
    }
}

impl HasLen for CachingFileHandle {
    fn len(&self) -> usize {
        self.underlying_filehandle.len()
    }
}

impl Directory for CachingDirectory {
    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        self.underlying.exists(path)
    }

    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let underlying_filehandle = self.underlying.get_file_handle(path)?;
        let caching_file_handle = CachingFileHandle {
            path: path.to_path_buf(),
            cache: self.cache.clone(),
            underlying_filehandle,
        };
        Ok(Arc::new(caching_file_handle))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let file_handle = self.get_file_handle(path)?;
        let len = file_handle.len();
        let owned_bytes = file_handle
            .read_bytes(0..len)
            .map_err(|io_error| OpenReadError::wrap_io_error(io_error, path.to_path_buf()))?;
        Ok(owned_bytes.as_slice().to_vec())
    }

    crate::read_only_directory!();
}

#[cfg(test)]
mod tests {

    use std::path::Path;
    use std::sync::Arc;

    use tantivy::directory::RamDirectory;
    use tantivy::Directory;

    use super::CachingDirectory;
    use crate::DebugProxyDirectory;

    #[test]
    fn test_caching_directory() -> tantivy::Result<()> {
        let ram_directory = RamDirectory::default();
        let test_path = Path::new("test");
        ram_directory.atomic_write(test_path, &b"test"[..])?;
        let debug_proxy_directory = Arc::new(DebugProxyDirectory::wrap(ram_directory));
        let caching_directory = CachingDirectory::new_unbounded(debug_proxy_directory.clone());
        caching_directory.atomic_read(test_path)?;
        caching_directory.atomic_read(test_path)?;
        assert_eq!(debug_proxy_directory.drain_read_operations().count(), 1);
        Ok(())
    }
}
