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

use std::{sync::Arc, fmt, path::{PathBuf, Path}, ops::Range};



use async_trait::async_trait;
use azure_core::error::{ErrorKind, ResultExt};
use azure_storage::core::prelude::*;
use azure_storage_blobs::prelude::*;

use once_cell::sync::OnceCell;
use regex::Regex;
use tantivy::directory::OwnedBytes;
use tracing::instrument;

use crate::{StorageFactory, Storage, StorageResult};

/// S3 Object storage Uri Resolver
#[derive(Default)]
pub struct AzureCompatibleBlobStorageFactory;

impl StorageFactory for AzureCompatibleBlobStorageFactory {
    fn protocol(&self) -> String {
        "azure".to_string()
    }

    fn resolve(&self, uri: &str) -> crate::StorageResult<std::sync::Arc<dyn crate::Storage>> {
        let storage = AzureCompatibleBlobStorage::from_uri(uri)?;
        Ok(Arc::new(storage))
    }
}


/// S3 Compatible object storage implementation.
pub struct AzureCompatibleBlobStorage {
    container_client: ContainerClient,
    container: String,
    prefix: PathBuf,
    // multipart_policy: MultiPartPolicy,
    // retry_params: RetryParams,
}

impl fmt::Debug for AzureCompatibleBlobStorage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AzureCompatibleBlobStorage")
            .field("container", &self.container)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl AzureCompatibleBlobStorage {

    /// Creates an object storage.
    pub fn new(account: &str, access_key: &str, container: &str) -> anyhow::Result<AzureCompatibleBlobStorage> {
        let container_client = StorageClient::new_access_key(account, access_key)
            .container_client(container);
        Ok(AzureCompatibleBlobStorage {
            container_client,
            container: container.to_string(),
            prefix: PathBuf::new(),
        })
    }

    pub fn from_uri(uri: &str) -> crate::StorageResult<AzureCompatibleBlobStorage> {
        // azure://account_name/container        
        let (account_name, container) = parse_uri(uri).ok_or_else(|| {
            crate::StorageErrorKind::Io.with_error(anyhow::anyhow!("Invalid uri: {}", uri))
        })?;
        let access_key =
            std::env::var("AZURE_ACCESS_KEY").expect("AZURE_ACCESS_KEY!");

        let azure_compatible_storage = AzureCompatibleBlobStorage::new(&account, &access_key, &container)
            .map_err(|err| crate::StorageErrorKind::Service.with_error(anyhow::anyhow!(err)))?;
        Ok(azure_compatible_storage)
    }

    
}

pub fn parse_uri(uri: &str) -> Option<(String, String)> {
    static URI_PTN: OnceCell<Regex> = OnceCell::new();
    URI_PTN
        .get_or_init(|| {
            // azure://account_name/container 
            Regex::new(r"azure(\+[^:]+)?://(?P<account>[^/]+)(/(?P<container>.+))?").unwrap()
        })
        .captures(uri)
        .and_then(|cap| {
           cap.name("account")
                .and_then(|account_match| cap.name("container")
                    .map(|container_match| (account_match.as_str().to_string(), container_match.as_str().to_string())))
        })
}


#[async_trait]
impl Storage for AzureCompatibleBlobStorage {

    async fn check(&self) -> anyhow::Result<()>{
        todo!("")
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::PutPayload>,
    ) -> crate::StorageResult<()> {
        todo!("")
    }

    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        todo!("")
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let v = self
            .container_client
            .blob_client(path.to_string_lossy().to_string())
            .delete().into_future().await?;

        todo!("")
    }

    #[instrument(level = "debug", skip(self, range), fields(range.start = range.start, range.end = range.end))]
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> { 
        todo!("")
    }

    #[instrument(level = "debug", skip(self), fields(fetched_bytes_len))]
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        todo!("")
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64>{
        todo!("")
    }

    fn uri(&self) -> String {
        format!("azure://{}/{}", self.container, self.prefix.to_string_lossy())
    }
}
