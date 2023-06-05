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

use crate::{Storage, StorageResolverError};

/// A storage factory builds a [`Storage`] object for a target [`StorageBackend`] from a
/// [`StorageConfig`] and a [`Uri`].
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait StorageFactory: Send + Sync + 'static {
    /// Returns the storage backend targeted by the factory.
    fn backend(&self) -> StorageBackend;

    /// Returns the appropriate [`Storage`] object for the URI.
    async fn resolve(
        &self,
        storage_config: &StorageConfig,
        uri: &Uri,
    ) -> Result<Arc<dyn Storage>, StorageResolverError>;
}

/// A storage factory for handling unsupported or unavailable storage backends.
#[derive(Debug, Clone)]
pub struct UnsupportedStorage {
    backend: StorageBackend,
    message: &'static str,
}

impl UnsupportedStorage {
    /// Creates a new [`UnsupportedStorage`].
    pub fn new(backend: StorageBackend, message: &'static str) -> Self {
        Self { backend, message }
    }
}

#[async_trait]
impl StorageFactory for UnsupportedStorage {
    fn backend(&self) -> StorageBackend {
        self.backend
    }

    async fn resolve(
        &self,
        _storage_config: &StorageConfig,
        _uri: &Uri,
    ) -> Result<Arc<dyn Storage>, StorageResolverError> {
        Err(StorageResolverError::UnsupportedBackend(
            self.message.to_string(),
        ))
    }
}
