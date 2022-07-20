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

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::{Protocol, Uri};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::{
    add_prefix_to_storage, OwnedBytes, Storage, StorageErrorKind, StorageFactory,
    StorageResolverError, StorageResult,
};

/// In Ram implementation of quickwit's storage.
///
/// This implementation is mostly useful in unit tests.
#[derive(Clone)]
pub struct RamStorage {
    uri: Uri,
    files: Arc<RwLock<HashMap<PathBuf, OwnedBytes>>>,
}

impl fmt::Debug for RamStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RamStorage")
            .field("uri", &self.uri)
            .finish()
    }
}

impl Default for RamStorage {
    fn default() -> Self {
        Self {
            uri: Uri::new("ram:///".to_string()),
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl RamStorage {
    /// Creates a [`RamStorageBuilder`]
    pub fn builder() -> RamStorageBuilder {
        RamStorageBuilder::default()
    }

    async fn put_data(&self, path: &Path, payload: OwnedBytes) {
        self.files.write().await.insert(path.to_path_buf(), payload);
    }

    async fn get_data(&self, path: &Path) -> Option<OwnedBytes> {
        self.files.read().await.get(path).cloned()
    }

    /// Returns the list of files that are present in the RamStorage.
    pub async fn list_files(&self) -> Vec<PathBuf> {
        self.files.read().await.keys().cloned().collect()
    }
}

#[async_trait]
impl Storage for RamStorage {
    async fn check(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        let payload_bytes = payload.read_all().await?;
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

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
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

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes)
    }

    fn uri(&self) -> &Uri {
        &self.uri
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
    files: HashMap<PathBuf, OwnedBytes>,
}

impl RamStorageBuilder {
    /// Adds a new file into the [`RamStorageBuilder`].
    pub fn put(mut self, path: &str, payload: &[u8]) -> Self {
        self.files
            .insert(PathBuf::from(path), OwnedBytes::new(payload.to_vec()));
        self
    }

    /// Finalizes the [`RamStorage`] creation.
    pub fn build(self) -> RamStorage {
        RamStorage {
            uri: Uri::new("ram:///".to_string()),
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
    fn protocol(&self) -> Protocol {
        Protocol::Ram
    }

    fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        match uri.filepath() {
            Some(prefix) if uri.protocol().is_ram() => Ok(add_prefix_to_storage(
                self.ram_storage.clone(),
                prefix.to_path_buf(),
                uri.clone(),
            )),
            _ => {
                Err(StorageResolverError::InvalidUri{ message: format!(
                    "URI `{uri}` is not a valid RAM storage URI. `ram://` is the only protocol \
                     accepted."
                ) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_suite::storage_test_suite;

    #[tokio::test]
    async fn test_storage() -> anyhow::Result<()> {
        let mut ram_storage = RamStorage::default();
        storage_test_suite(&mut ram_storage).await?;
        Ok(())
    }

    #[test]
    fn test_ram_storage_factory() {
        let ram_storage_factory = RamStorageFactory::default();
        let ram_uri = Uri::new("s3:///foo".to_string());
        let err = ram_storage_factory.resolve(&ram_uri).err().unwrap();
        assert!(matches!(err, StorageResolverError::InvalidUri { .. }));

        let data_uri = Uri::new("ram:///data".to_string());
        let data_storage = ram_storage_factory.resolve(&data_uri).ok().unwrap();
        let home_uri = Uri::new("ram:///home".to_string());
        let home_storage = ram_storage_factory.resolve(&home_uri).ok().unwrap();
        assert_ne!(data_storage.uri(), home_storage.uri());

        let data_storage_two = ram_storage_factory.resolve(&data_uri).ok().unwrap();
        assert_eq!(data_storage.uri(), data_storage_two.uri());
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
