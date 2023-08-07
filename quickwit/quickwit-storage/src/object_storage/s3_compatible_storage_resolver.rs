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
use quickwit_config::{S3StorageConfig, StorageBackend};

use crate::{
    DebouncedStorage, S3CompatibleObjectStorage, Storage, StorageFactory, StorageResolverError,
};

/// S3 compatible object storage resolver.
pub struct S3CompatibleObjectStorageFactory {
    storage_config: S3StorageConfig,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3-compatible storage factory.
    pub fn new(storage_config: S3StorageConfig) -> Self {
        Self { storage_config }
    }
}

#[async_trait]
impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let storage = S3CompatibleObjectStorage::from_uri(&self.storage_config, uri).await?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}
