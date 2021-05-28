use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};
use async_trait::async_trait;

use crate::Storage;

/// This storage acts as a proxy to another storage that simply modifies each API call
/// by preceding each path with a given a prefix.
struct PrefixStorage {
    pub storage: Arc<dyn Storage>,
    pub prefix: PathBuf,
}

#[async_trait]
impl Storage for PrefixStorage {
    async fn put(&self, path: &Path, payload: crate::PutPayload) -> crate::StorageResult<()> {
        self.storage.put(&self.prefix.join(path), payload).await
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> crate::StorageResult<()> {
        self.storage
            .copy_to_file(&self.prefix.join(path), output_path)
            .await
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> crate::StorageResult<Vec<u8>> {
        self.storage.get_slice(&self.prefix.join(path), range).await
    }

    async fn get_all(&self, path: &Path) -> crate::StorageResult<Vec<u8>> {
        self.storage.get_all(&self.prefix.join(path)).await
    }

    async fn delete(&self, path: &Path) -> crate::StorageResult<()> {
        self.storage.delete(&self.prefix.join(path)).await
    }

    async fn exists(&self, path: &Path) -> crate::StorageResult<bool> {
        self.storage.exists(&self.prefix.join(path)).await
    }

    fn uri(&self) -> String {
        Path::new(&self.storage.uri())
            .join(&self.prefix)
            .to_string_lossy()
            .to_string()
    }
}

pub fn add_prefix_to_storage(storage: Arc<dyn Storage>, prefix: PathBuf) -> Arc<dyn Storage> {
    Arc::new(PrefixStorage { storage, prefix })
}
