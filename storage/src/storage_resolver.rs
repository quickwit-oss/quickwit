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
use std::collections::HashMap;
use std::sync::Arc;

use rusoto_core::Region;

use crate::ram_storage::RamStorageFactory;
use crate::{S3CompatibleObjectStorageFactory, Storage, StorageResolverError};

/// A storage factory builds a storage given an Uri.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
pub trait StorageFactory: Send + Sync + 'static {
    /// Returns the protocol this uri resolver is serving.
    fn protocol(&self) -> String;
    /// Given an uri, returns a Storage object.
    fn resolve(&self, uri: &str) -> crate::StorageResult<Arc<dyn Storage>>;
}

/// Resolve an uri by dispatching it to the right [`StorageFactory`]
/// based on its protocol.
pub struct StorageUriResolver {
    per_protocol_resolver: HashMap<String, Arc<dyn StorageFactory>>,
}

impl Default for StorageUriResolver {
    fn default() -> Self {
        let mut resolver = StorageUriResolver {
            per_protocol_resolver: Default::default(),
        };
        resolver.register(RamStorageFactory::default());
        resolver.register(S3CompatibleObjectStorageFactory::new(Region::default()));
        resolver
    }
}

impl StorageUriResolver {
    /// Registers another resolver.
    ///
    /// If a previous resolver was registered for this protocol, it is discarded
    /// and replaced by this one.
    pub fn register<S: StorageFactory>(&mut self, resolver: S) {
        self.per_protocol_resolver
            .insert(resolver.protocol(), Arc::new(resolver));
    }

    /// Resolves the given uri.
    pub fn resolve(&self, uri: &str) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let protocol = uri.split("://").next().ok_or_else(|| {
            StorageResolverError::InvalidUri(format!("Protocol not found in storage uri: {}", uri))
        })?;
        let resolver = self
            .per_protocol_resolver
            .get(protocol)
            .ok_or_else(|| StorageResolverError::ProtocolUnsupported(protocol.to_string()))?;
        let storage = resolver
            .resolve(uri)
            .map_err(StorageResolverError::FailedToOpenStorage)?;
        Ok(storage)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::RamStorage;

    use super::*;

    #[tokio::test]
    async fn test_storage_resolver_simple() -> anyhow::Result<()> {
        let mut storage_resolver = StorageUriResolver::default();
        let mut first = MockStorageFactory::new();
        first.expect_protocol().returning(|| "first".to_string());
        let mut second = MockStorageFactory::new();
        second.expect_protocol().returning(|| "second".to_string());
        second.expect_resolve().returning(|_uri| {
            Ok(Arc::new(
                RamStorage::builder()
                    .put("hello", b"hello_content_second")
                    .build(),
            ))
        });
        storage_resolver.register(first);
        storage_resolver.register(second);
        let resolved = storage_resolver.resolve("second://")?;
        let data = resolved.get_all(Path::new("hello")).await?;
        assert_eq!(data, b"hello_content_second");
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_resolver_override() -> anyhow::Result<()> {
        let mut storage_resolver = StorageUriResolver::default();
        let mut first = MockStorageFactory::new();
        first.expect_protocol().returning(|| "protocol".to_string());
        let mut second = MockStorageFactory::new();
        second
            .expect_protocol()
            .returning(|| "protocol".to_string());
        second.expect_resolve().returning(|uri| {
            assert_eq!(uri, "protocol://mystorage");
            Ok(Arc::new(
                RamStorage::builder()
                    .put("hello", b"hello_content_second")
                    .build(),
            ))
        });
        storage_resolver.register(first);
        storage_resolver.register(second);
        let resolved = storage_resolver.resolve("protocol://mystorage")?;
        let data = resolved.get_all(Path::new("hello")).await?;
        assert_eq!(data, b"hello_content_second");
        Ok(())
    }

    #[test]
    fn test_storage_resolver_unsupported_protocol() -> anyhow::Result<()> {
        let storage_resolver = StorageUriResolver::default();
        assert!(matches!(
            storage_resolver.resolve("protocol://hello"),
            Err(crate::StorageResolverError::ProtocolUnsupported(protocol)) if protocol == "protocol"
        ));
        Ok(())
    }
}
