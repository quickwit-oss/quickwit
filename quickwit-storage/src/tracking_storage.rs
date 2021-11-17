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

use std::ops::{Deref, Range};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::memory_usage::{MemoryUsage, MemoryUsageGuard};
use stable_deref_trait::StableDeref;
use tantivy::directory::OwnedBytes;

use crate::{PutPayload, Storage, StorageResult};

pub struct TrackingStorage {
    memory_usage: MemoryUsage,
    storage: Arc<dyn Storage>,
}

impl TrackingStorage {
    pub fn new(memory_usage: MemoryUsage, storage: Arc<dyn Storage>) -> TrackingStorage {
        TrackingStorage {
            memory_usage,
            storage,
        }
    }
}

struct OwnedBytesWithGuard {
    // Order matters here:
    // payload is dropped before the memory_guard
    payload: OwnedBytes,
    _memory_guard: MemoryUsageGuard,
}

unsafe impl StableDeref for OwnedBytesWithGuard {}

impl Deref for OwnedBytesWithGuard {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.payload.deref()
    }
}

fn associate_memory_guard(memory_guard: MemoryUsageGuard, payload: OwnedBytes) -> OwnedBytes {
    assert_eq!(memory_guard.num_bytes(), payload.len() as u64);
    let owned_bytes_with_guard = OwnedBytesWithGuard {
        payload,
        _memory_guard: memory_guard,
    };
    OwnedBytes::new(owned_bytes_with_guard)
}

#[async_trait]
impl Storage for TrackingStorage {
    async fn check(&self) -> anyhow::Result<()> {
        self.storage.check().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        self.storage.put(path, payload).await
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        self.storage.copy_to_file(path, output_path).await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let memory_guard = self.memory_usage.use_memory(range.len() as u64);
        let payload = self.storage.get_slice(path, range).await?;
        Ok(associate_memory_guard(memory_guard, payload))
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let num_bytes = self.storage.file_num_bytes(path).await?;
        let memory_guard = self.memory_usage.use_memory(num_bytes);
        let payload = self.storage.get_all(path).await?;
        Ok(associate_memory_guard(memory_guard, payload))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.storage.delete(path).await
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.storage.exists(path).await
    }

    /// Returns a file size.
    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }

    /// Returns an URI identifying the storage
    fn uri(&self) -> String {
        self.storage.uri()
    }
}
