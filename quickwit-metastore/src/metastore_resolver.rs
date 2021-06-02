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

use async_trait::async_trait;
use quickwit_storage::StorageResolverError;
use quickwit_storage::StorageUriResolver;
use std::collections::HashMap;
use std::sync::Arc;

use crate::MetastoreErrorKind;
use crate::SingleFileMetastore;
use crate::{Metastore, MetastoreResolverError};

/// A metastore factory builds a [`Metastore`] object from an URI.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait MetastoreFactory: Send + Sync + 'static {
    /// Returns the protocol this URI resolver is serving.
    fn protocol(&self) -> String;
    /// Given an URI, returns a [`Metastore`] object.
    async fn resolve(&self, uri: String) -> Result<Arc<dyn Metastore>, MetastoreResolverError>;
}

/// Resolves an URI by dispatching it to the right [`MetastoreFactory`]
/// based on its protocol.
pub struct MetastoreUriResolver {
    per_protocol_resolver: HashMap<String, Arc<dyn MetastoreFactory>>,
    default_storage_resolver: StorageUriResolver,
}

impl Default for MetastoreUriResolver {
    fn default() -> Self {
        MetastoreUriResolver {
            per_protocol_resolver: Default::default(),
            default_storage_resolver: Default::default(),
        }
    }
}

impl MetastoreUriResolver {
    /// Registers a resolver.
    ///
    /// If a previous resolver was registered for this protocol, it is discarded
    /// and replaced with the new one.
    pub fn register<S: MetastoreFactory>(&mut self, resolver: S) {
        self.per_protocol_resolver
            .insert(resolver.protocol(), Arc::new(resolver));
    }

    /// Resolves the given URI.
    pub async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        // TODO: be a little bit more restrictive on the uri, currently we accept
        // path like `file://` which will certainly not work.
        let protocol = uri.split("://").next().ok_or_else(|| {
            MetastoreResolverError::InvalidUri(format!(
                "Protocol not found in metastore uri: {}",
                uri
            ))
        })?;

        if let Some(resolver) = self.per_protocol_resolver.get(protocol) {
            let metastore = resolver.resolve(uri.to_string()).await?;
            return Ok(metastore);
        }

        let storage = self
            .default_storage_resolver
            .resolve(&uri)
            .map_err(|err| match err {
                StorageResolverError::InvalidUri(err_msg) => {
                    MetastoreResolverError::InvalidUri(err_msg)
                }
                StorageResolverError::ProtocolUnsupported(err_msg) => {
                    MetastoreResolverError::ProtocolUnsupported(err_msg)
                }
                StorageResolverError::FailedToOpenStorage(err) => {
                    MetastoreResolverError::FailedToOpenMetastore(
                        MetastoreErrorKind::InternalError.with_error(err),
                    )
                }
            })?;
        let single_file_metastore = Arc::new(SingleFileMetastore::new(storage));
        Ok(single_file_metastore)
    }
}

#[cfg(test)]
mod tests {
    use crate::MetastoreUriResolver;

    #[tokio::test]
    async fn test_metastore_resolver_should_not_raise_errors_on_file_and_s3() -> anyhow::Result<()>
    {
        let metastore_resolver = MetastoreUriResolver::default();
        metastore_resolver.resolve("file://").await?;
        metastore_resolver
            .resolve("s3://bucket/path/to/object")
            .await?;
        Ok(())
    }

    #[tokio::test]
    #[should_panic(expected = "ProtocolUnsupported(\"s4\")")]
    async fn test_metastore_resolver_should_raise_error_on_storage_error() {
        let metastore_resolver = MetastoreUriResolver::default();
        metastore_resolver
            .resolve("s4://bucket/path/to/object")
            .await
            .unwrap();
    }
}
