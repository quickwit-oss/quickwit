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
use std::sync::Arc;

use once_cell::sync::OnceCell;
use quickwit_common::uri::{Protocol, Uri};

use crate::local_file_storage::LocalFileStorageFactory;
use crate::ram_storage::RamStorageFactory;
#[cfg(feature = "azure")]
use crate::AzureBlobStorageFactory;
use crate::{S3CompatibleObjectStorageFactory, Storage, StorageResolverError};

/// Quickwit supported storage resolvers.
pub fn quickwit_storage_uri_resolver() -> &'static StorageUriResolver {
    static STORAGE_URI_RESOLVER: OnceCell<StorageUriResolver> = OnceCell::new();
    STORAGE_URI_RESOLVER.get_or_init(|| {
        #[allow(unused_mut)]
        let mut builder = StorageUriResolver::builder()
            .register(RamStorageFactory::default())
            .register(LocalFileStorageFactory::default())
            .register(S3CompatibleObjectStorageFactory::default());

        #[cfg(feature = "azure")]
        {
            builder = builder.register(AzureBlobStorageFactory::default());
        }

        #[cfg(not(feature = "azure"))]
        {
            builder = builder.register(UnsupportedStorage {
                protocol: Protocol::Azure,
            })
        }

        builder.build()
    })
}

/// A storage factory builds a [`Storage`] object from an URI.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
pub trait StorageFactory: Send + Sync + 'static {
    /// Returns the protocol handled by the storage factory.
    fn protocol(&self) -> Protocol;

    /// Returns the appropriate [`Storage`] object for the URI.
    fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError>;
}

/// A storage factory implementation for handling not supported features.
#[derive(Clone, Copy, Debug)]
pub struct UnsupportedStorage {
    protocol: Protocol,
}

impl StorageFactory for UnsupportedStorage {
    fn protocol(&self) -> Protocol {
        self.protocol
    }

    fn resolve(&self, _: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        Err(StorageResolverError::ProtocolUnsupported {
            protocol: self.protocol.to_string(),
        })
    }
}

/// Resolves an URI by dispatching it to the right [`StorageFactory`]
/// based on its protocol.
#[derive(Clone)]
pub struct StorageUriResolver {
    per_protocol_resolver: Arc<HashMap<Protocol, Arc<dyn StorageFactory>>>,
}

#[derive(Default)]
pub struct StorageUriResolverBuilder {
    per_protocol_resolver: HashMap<Protocol, Arc<dyn StorageFactory>>,
}

impl StorageUriResolverBuilder {
    /// Registers a storage factory.
    ///
    /// If a previous factory was registered for this protocol, it is discarded
    /// and replaced with the new one.
    pub fn register<S: StorageFactory>(mut self, factory: S) -> Self {
        self.per_protocol_resolver
            .insert(factory.protocol(), Arc::new(factory));
        self
    }

    /// Builds the `StorageUriResolver`.
    pub fn build(self) -> StorageUriResolver {
        StorageUriResolver {
            per_protocol_resolver: Arc::new(self.per_protocol_resolver),
        }
    }
}

impl StorageUriResolver {
    /// Creates an empty [`StorageUriResolverBuilder`].
    pub fn builder() -> StorageUriResolverBuilder {
        StorageUriResolverBuilder::default()
    }

    /// Creates `StorageUriResolver` for testing.
    #[doc(hidden)]
    pub fn for_test() -> Self {
        #[allow(unused_mut)]
        let mut builder = StorageUriResolver::builder()
            .register(RamStorageFactory::default())
            .register(LocalFileStorageFactory::default())
            .register(S3CompatibleObjectStorageFactory::default());

        #[cfg(feature = "azure")]
        {
            builder = builder.register(AzureBlobStorageFactory::default());
        }

        builder.build()
    }

    /// Resolves the given URI.
    pub fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        let resolver = self
            .per_protocol_resolver
            .get(&uri.protocol())
            .ok_or_else(|| StorageResolverError::ProtocolUnsupported {
                protocol: uri.protocol().to_string(),
            })?;
        let storage = resolver.resolve(uri)?;
        Ok(storage)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::RamStorage;

    #[tokio::test]
    async fn test_storage_resolver_simple() -> anyhow::Result<()> {
        let mut file_storage_factory = MockStorageFactory::new();
        file_storage_factory
            .expect_protocol()
            .returning(|| Protocol::File);

        let mut ram_storage_factory = MockStorageFactory::new();
        ram_storage_factory
            .expect_protocol()
            .returning(|| Protocol::Ram);
        ram_storage_factory.expect_resolve().returning(|_uri| {
            Ok(Arc::new(
                RamStorage::builder()
                    .put("hello", b"hello_content_second")
                    .build(),
            ))
        });
        let storage_resolver = StorageUriResolver::builder()
            .register(file_storage_factory)
            .register(ram_storage_factory)
            .build();
        let storage = storage_resolver.resolve(&Uri::new("ram:///".to_string()))?;
        let data = storage.get_all(Path::new("hello")).await?;
        assert_eq!(&data[..], b"hello_content_second");
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_resolver_override() -> anyhow::Result<()> {
        let mut first_ram_storage_factory = MockStorageFactory::new();
        first_ram_storage_factory
            .expect_protocol()
            .returning(|| Protocol::Ram);

        let mut second_ram_storage_factory = MockStorageFactory::new();
        second_ram_storage_factory
            .expect_protocol()
            .returning(|| Protocol::Ram);
        second_ram_storage_factory
            .expect_resolve()
            .returning(|uri| {
                assert_eq!(uri.as_str(), "ram:///home");
                Ok(Arc::new(
                    RamStorage::builder()
                        .put("hello", b"hello_content_second")
                        .build(),
                ))
            });
        let storage_resolver = StorageUriResolver::builder()
            .register(first_ram_storage_factory)
            .register(second_ram_storage_factory)
            .build();
        let storage = storage_resolver.resolve(&Uri::new("ram:///home".to_string()))?;
        let data = storage.get_all(Path::new("hello")).await?;
        assert_eq!(&data[..], b"hello_content_second");
        Ok(())
    }

    #[test]
    fn test_storage_resolver_unsupported_protocol() {
        let storage_resolver = StorageUriResolver::for_test();
        let storage_uri = Uri::new("postgresql://localhost:5432/metastore".to_string());
        assert!(matches!(
            storage_resolver.resolve(&storage_uri),
            Err(crate::StorageResolverError::ProtocolUnsupported { protocol }) if protocol == "postgresql"
        ));
    }
}
