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
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::{
    add_prefix_to_storage, PutPayload, Storage, StorageErrorKind, StorageFactory, StorageResult,
};

/// In Ram implementation of quickwit's storage.
///
/// This implementation is mostly useful in unit tests.
#[derive(Default, Clone)]
pub struct RamStorage {
    files: Arc<RwLock<HashMap<PathBuf, Bytes>>>,
}

impl fmt::Debug for RamStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RamStorage")
    }
}

impl RamStorage {
    /// Creates a [`RamStorageBuilder`]
    pub fn builder() -> RamStorageBuilder {
        RamStorageBuilder::default()
    }

    async fn put_data(&self, path: &Path, payload: Bytes) {
        self.files.write().await.insert(path.to_path_buf(), payload);
    }

    async fn get_data(&self, path: &Path) -> Option<Bytes> {
        self.files.read().await.get(path).cloned()
    }

    /// Returns the list of files that are present in the RamStorage.
    pub async fn list_files(&self) -> Vec<PathBuf> {
        self.files.read().await.keys().cloned().collect()
    }
}

async fn read_all(put: &PutPayload) -> io::Result<Bytes> {
    match put {
        PutPayload::InMemory(data) => Ok(data.clone()),
        PutPayload::LocalFile(filepath) => tokio::fs::read(filepath).await.map(Bytes::from),
    }
}

#[async_trait]
impl Storage for RamStorage {
    async fn put(&self, path: &Path, payload: PutPayload) -> crate::StorageResult<()> {
        let payload_bytes = read_all(&payload).await?;
        self.put_data(path, payload_bytes).await;
        Ok(())
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Failed to find dest_path {:?}", path))
        })?;
        let mut file = File::create(output_path).await?;
        file.write_all(&payload_bytes).await?;
        file.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<Bytes> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes.slice(range.start as usize..range.end as usize))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.files.write().await.remove(path);
        Ok(())
    }

    async fn get_all(&self, path: &Path) -> StorageResult<Bytes> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes)
    }

    fn uri(&self) -> String {
        "ram://".to_string()
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        if let Some(file_bytes) = self.files.read().await.get(path) {
            Ok(file_bytes.len() as u64)
        } else {
            let err = anyhow::anyhow!("Missing file `{}`", path.display());
            Err(StorageErrorKind::DoesNotExist.with_error(err))
        }
    }
}

/// Builder to create a prepopulated [`RamStorage`]. This is mostly useful for tests.
#[derive(Default)]
pub struct RamStorageBuilder {
    files: HashMap<PathBuf, Bytes>,
}

impl RamStorageBuilder {
    /// Adds a new file into the [`RamStorageBuilder`].
    pub fn put(mut self, path: &str, payload: &[u8]) -> Self {
        self.files
            .insert(PathBuf::from(path), Bytes::from(payload.to_vec()));
        self
    }

    /// Finalizes the [`RamStorage`] creation.
    pub fn build(self) -> RamStorage {
        RamStorage {
            files: Arc::new(RwLock::new(self.files)),
        }
    }
}

/// In Ram storage resolver
pub struct RamStorageFactory {
    ram_storage: Arc<dyn Storage>,
}

impl Default for RamStorageFactory {
    fn default() -> Self {
        RamStorageFactory {
            ram_storage: Arc::new(RamStorage::default()),
        }
    }
}

impl StorageFactory for RamStorageFactory {
    fn protocol(&self) -> String {
        "ram".to_string()
    }

    fn resolve(&self, uri: &str) -> crate::StorageResult<Arc<dyn Storage>> {
        if !uri.starts_with("ram://") {
            let err_msg = anyhow::anyhow!(
                "{:?} is an invalid ram storage uri. Only ram:// is accepted.",
                uri
            );
            return Err(StorageErrorKind::DoesNotExist.with_error(err_msg));
        }

        let prefix = uri.split("://").nth(1).ok_or_else(|| {
            StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Invalid prefix path: {}", uri))
        })?;

        Ok(add_prefix_to_storage(self.ram_storage.clone(), prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::storage_test_suite;

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let mut ram_storage = RamStorage::default();
        storage_test_suite(&mut ram_storage).await?;
        Ok(())
    }

    #[test]
    fn test_ram_storage_factory() {
        let ram_storage_factory = RamStorageFactory::default();
        let err = ram_storage_factory.resolve("rom://toto").err().unwrap();
        assert_eq!(err.kind(), StorageErrorKind::DoesNotExist);

        let data_result = ram_storage_factory.resolve("ram://data").ok().unwrap();
        let home_result = ram_storage_factory.resolve("ram://home/data").ok().unwrap();
        assert_ne!(data_result.uri(), home_result.uri());

        let data_result_two = ram_storage_factory.resolve("ram://data").ok().unwrap();
        assert_eq!(data_result.uri(), data_result_two.uri());
    }

    #[tokio::test]
    async fn test_ram_storage_builder() -> anyhow::Result<()> {
        let storage = RamStorage::builder()
            .put("path1", b"path1_payload")
            .put("path2", b"path2_payload")
            .put("path1", b"path1_payloadb")
            .build();
        assert_eq!(
            &storage.get_all(Path::new("path1")).await?,
            &b"path1_payloadb"[..]
        );
        assert_eq!(
            &storage.get_all(Path::new("path2")).await?,
            &b"path2_payload"[..]
        );
        Ok(())
    }
}
