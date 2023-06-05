// Copyright (C) 2023 Quickwit, Inc.
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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{StorageBackend, StorageConfig};

use crate::{
    DebouncedStorage, S3CompatibleObjectStorage, Storage, StorageFactory, StorageResolverError,
};

/// S3 compatible object storage resolver.
#[derive(Default)]
pub struct S3CompatibleObjectStorageFactory;

#[async_trait]
impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn resolve(
        &self,
        storage_config: &StorageConfig,
        uri: &Uri,
    ) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let s3_storage_config = storage_config.as_s3().ok_or_else(|| {
            let message = format!(
                "Expected S3 storage config, got `{:?}`.",
                storage_config.backend()
            );
            StorageResolverError::InvalidConfig(message)
        })?;
        let storage = S3CompatibleObjectStorage::from_uri(s3_storage_config, uri).await?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}
