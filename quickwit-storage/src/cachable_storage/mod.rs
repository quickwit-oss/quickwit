// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

mod local_storage_cache;
mod ram_cache;
mod storage_with_local_cache;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{ops::Range, path::PathBuf};

use crate::{PutPayload, StorageErrorKind, StorageResult};

pub use storage_with_local_cache::{
    create_cachable_storage, CacheConfig, StorageWithLocalStorageCache,
};

const CACHE_STATE_FILE_NAME: &str = "cache-sate.json";

/// Capacity encapsulates the maximum number of items a cache can hold.
/// We need to account for the number of items as well as the size of each item.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct DiskCapacity {
    /// Maximum of number of files.
    max_num_files: usize,
    /// Maximum size in bytes.
    max_num_bytes: usize,
}

/// CacheState is a struct for serializing/deserializing the cache state.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct CacheState {
    /// Uri of the local storage.
    local_storage_uri: String,
    /// The disk capacity
    disk_capacity: DiskCapacity,
    /// The ram capacity in bytes.
    ram_capacity: usize,
    /// The list of items in the cache.
    items: Vec<(PathBuf, usize)>,
}

impl CacheState {
    /// Construct an instance of [`CacheState`] from a persisted cache state file.
    pub fn from_path(path: &Path) -> StorageResult<Self> {
        let file_path = path.to_path_buf().join(CACHE_STATE_FILE_NAME);
        let json_file = std::fs::File::open(file_path)?;
        let reader = std::io::BufReader::new(json_file);
        serde_json::from_reader(reader)
            .map_err(|err| StorageErrorKind::InternalError.with_error(err))
    }
}

/// The `Cache` trait is the abstraction used to describe the caching logic
/// used in front of a storage. See `LocalStorageCache`.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait StorageCache: Send + Sync + 'static {
    /// Try to get the entire file from the cache.
    async fn get(&mut self, path: &Path) -> StorageResult<Option<Bytes>>;

    /// Put a payload in the cache.
    async fn put(&mut self, path: &Path, payload: PutPayload) -> StorageResult<()>;

    /// Try to get a slice of data from the cache.
    async fn get_slice(
        &mut self,
        path: &Path,
        bytes_range: Range<usize>,
    ) -> StorageResult<Option<Bytes>>;

    /// Put a slice of data into the cache.
    async fn put_slice(
        &mut self,
        path: &Path,
        byte_range: Range<usize>,
        bytes: Bytes,
    ) -> StorageResult<()>;

    /// Directly copy a file from the cache.
    async fn copy_to_file(&mut self, path: &Path, output_path: &Path) -> StorageResult<bool>;

    /// Remove an item from the cache.
    async fn delete(&mut self, path: &Path) -> StorageResult<bool>;

    /// Get the size in bytes of a cache item.
    async fn file_num_bytes(&mut self, path: &Path) -> crate::StorageResult<Option<usize>>;

    /// List all items in the cache and their size.
    fn get_items(&self) -> Vec<(PathBuf, usize)>;

    /// Save the cache state on persistent storage.
    async fn save_state(&self) -> StorageResult<()>;
}
