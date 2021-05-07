/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use crate::{PutPayload, Storage, StoreErrorKind, StoreResult};
use async_trait::async_trait;
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, io};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

#[derive(Default, Clone)]
pub struct RamStorage {
    files: Arc<RwLock<HashMap<PathBuf, Arc<[u8]>>>>,
}

impl fmt::Debug for RamStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RamStorage")
    }
}

impl RamStorage {
    async fn put_data(&self, path: &Path, payload: Arc<[u8]>) {
        self.files.write().await.insert(path.to_path_buf(), payload);
    }

    async fn get_data(&self, path: &Path) -> Option<Arc<[u8]>> {
        self.files.read().await.get(path).cloned()
    }
}

async fn read_all(put: &PutPayload) -> io::Result<Arc<[u8]>> {
    match put {
        PutPayload::InMemory(data) => Ok(data.clone()),
        PutPayload::LocalFile(filepath) => tokio::fs::read(filepath)
            .await
            .map(|buf| buf.into_boxed_slice().into()),
    }
}

#[async_trait]
impl Storage for RamStorage {
    async fn put(&self, path: &Path, payload: PutPayload) -> crate::StoreResult<()> {
        let payload_bytes = read_all(&payload).await?;
        self.put_data(path, payload_bytes).await;
        Ok(())
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StoreResult<()> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StoreErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Failed to find dest_path {:?}", path))
        })?;
        let mut file = File::create(output_path).await?;
        file.write_all(&payload_bytes).await?;
        file.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StoreResult<Vec<u8>> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StoreErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes[range.start as usize..range.end as usize].to_vec())
    }

    async fn delete(&self, path: &Path) -> StoreResult<()> {
        self.files.write().await.remove(path);
        Ok(())
    }

    async fn get_all(&self, path: &Path) -> StoreResult<Vec<u8>> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StoreErrorKind::DoesNotExist
                .with_error(anyhow::anyhow!("Failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes.to_vec())
    }

    fn uri(&self) -> String {
        "ram://".to_string()
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
}
