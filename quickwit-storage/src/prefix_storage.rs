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

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::path_buf_to_slash;

use crate::{OwnedBytes, Storage};

/// This storage acts as a proxy to another storage that simply modifies each API call
/// by preceding each path with a given a prefix.
struct PrefixStorage {
    pub storage: Arc<dyn Storage>,
    pub prefix: PathBuf,
}

impl  PrefixStorage {

    fn full_path(&self, path: &Path) -> PathBuf {
        path_buf_to_slash(self.prefix.join(path))
    }
    
}

#[async_trait]
impl Storage for PrefixStorage {
    async fn check(&self) -> anyhow::Result<()> {
        self.storage.check().await
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        self.storage.put(&self.full_path(path), payload).await
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> crate::StorageResult<()> {
        self.storage
            .copy_to_file(&self.full_path(path), output_path)
            .await
    }

    async fn get_slice(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> crate::StorageResult<OwnedBytes> {
        self.storage.get_slice(&self.full_path(path), range).await
    }

    async fn get_all(&self, path: &Path) -> crate::StorageResult<OwnedBytes> {
        self.storage.get_all(&self.full_path(path)).await
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        self.storage.delete(&self.full_path(path)).await
    }

    async fn exists(&self, path: &Path) -> crate::StorageResult<bool> {
        self.storage.exists(&self.full_path(path)).await
    }

    fn uri(&self) -> String {
        Path::new(&self.storage.uri())
            .join(self.prefix.as_path())
            .to_string_lossy()
            .to_string()
        // Uri::new(&self.storage.uri())
        //     .join(&self.prefix.to_string_lossy())
        //     .unwrap()
        //     .into_string()
    }

    async fn file_num_bytes(&self, path: &Path) -> crate::StorageResult<u64> {
        self.storage.file_num_bytes(&self.full_path(path)).await
    }
}

/// Creates a [`PrefixStorage`] using an underlying storage and a prefix.
pub fn add_prefix_to_storage<P: Into<PathBuf>>(
    storage: Arc<dyn Storage>,
    prefix: P,
) -> Arc<dyn Storage> {
    Arc::new(PrefixStorage {
        storage,
        prefix: prefix.into(),
    })
}
