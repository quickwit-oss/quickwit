// Copyright (C) 2021 Quickwit, Inc.
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

use async_trait::async_trait;
use once_cell::sync::OnceCell;

use crate::metastore::file_backed_metastore::FileBackedMetastoreFactory;
#[cfg(feature = "postgres")]
use crate::metastore::postgresql_metastore::PostgresqlMetastoreFactory;
use crate::{Metastore, MetastoreResolverError};

/// A metastore factory builds a [`Metastore`] object from an URI.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait MetastoreFactory: Send + Sync + 'static {
    /// Given an URI, returns a [`Metastore`] object.
    async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError>;
}

#[derive(Default)]
pub struct MetastoreUriResolverBuilder {
    per_protocol_resolver: HashMap<String, Arc<dyn MetastoreFactory>>,
}

impl MetastoreUriResolverBuilder {
    pub fn register<S: MetastoreFactory>(mut self, protocol: &str, resolver: S) -> Self {
        self.per_protocol_resolver
            .insert(protocol.to_string(), Arc::new(resolver));
        self
    }

    pub fn build(self) -> MetastoreUriResolver {
        MetastoreUriResolver {
            per_protocol_resolver: Arc::new(self.per_protocol_resolver),
        }
    }
}

/// Resolves an URI by dispatching it to the right [`MetastoreFactory`]
/// based on its protocol.
pub struct MetastoreUriResolver {
    per_protocol_resolver: Arc<HashMap<String, Arc<dyn MetastoreFactory>>>,
}

/// Quickwit supported storage resolvers.
///
/// The returned metastore uri resolver is a Singleton.
pub fn quickwit_metastore_uri_resolver() -> &'static MetastoreUriResolver {
    static METASTORE_URI_RESOLVER: OnceCell<MetastoreUriResolver> = OnceCell::new();
    METASTORE_URI_RESOLVER.get_or_init(|| {
        #[allow(unused_mut)]
        let mut builder = MetastoreUriResolver::builder()
            .register("ram", FileBackedMetastoreFactory::default())
            .register("file", FileBackedMetastoreFactory::default())
            .register("s3", FileBackedMetastoreFactory::default());
        #[cfg(feature = "postgres")]
        {
            builder = builder
                .register("postgres", PostgresqlMetastoreFactory::default())
                .register("postgresql", PostgresqlMetastoreFactory::default());
        }

        #[cfg(not(feature = "postgres"))]
        {
            builder = builder
                .register(
                    "postgres",
                    UnsuportedMetastore {
                        message: "postgres unsupported, quickwit was compiled without the \
                                  'postgres' feature flag"
                            .to_string(),
                    },
                )
                .register(
                    "postgresql",
                    UnsuportedMetastore {
                        message: "postgresql unsupported, quickwit was compiled without the \
                                  'postgres' feature flag"
                            .to_string(),
                    },
                )
        }

        builder.build()
    })
}

/// A postgres metastore factory
#[derive(Clone, Default)]
pub struct UnsuportedMetastore {
    message: String,
}

#[async_trait]
impl MetastoreFactory for UnsuportedMetastore {
    async fn resolve(&self, _uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        Err(MetastoreResolverError::ProtocolUnsupported(
            self.message.to_string(),
        ))
    }
}

impl MetastoreUriResolver {
    /// Creates an empty `MetastoreUriResolver`.
    pub fn builder() -> MetastoreUriResolverBuilder {
        MetastoreUriResolverBuilder::default()
    }

    /// Resolves the given URI.
    pub async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let protocol = uri.split("://").next().ok_or_else(|| {
            MetastoreResolverError::InvalidUri(format!(
                "Protocol not found in metastore URI: {}",
                uri
            ))
        })?;

        let resolver = self
            .per_protocol_resolver
            .get(protocol)
            .ok_or_else(|| MetastoreResolverError::ProtocolUnsupported(protocol.to_string()))?;

        let metastore = resolver.resolve(uri).await?;
        Ok(metastore)
    }
}

#[cfg(test)]
mod tests {
    use crate::quickwit_metastore_uri_resolver;

    #[tokio::test]
    async fn test_metastore_resolver_should_not_raise_errors_on_file() -> anyhow::Result<()> {
        let metastore_resolver = quickwit_metastore_uri_resolver();
        metastore_resolver.resolve("file://").await?;
        Ok(())
    }

    #[tokio::test]
    #[should_panic(expected = "ProtocolUnsupported(\"s4\")")]
    async fn test_metastore_resolver_should_raise_error_on_storage_error() {
        let metastore_resolver = quickwit_metastore_uri_resolver();
        metastore_resolver
            .resolve("s4://bucket/path/to/object")
            .await
            .unwrap();
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn test_postgres_and_postgresql_protocol_accepted() {
        use std::env;
        let metastore_resolver = quickwit_metastore_uri_resolver();
        // If the database defined in the env var or the default one is not up, the
        // test block after making 10 attempts with a timeout of 10s each = 100s.
        let test_database_url = env::var("TEST_DATABASE_URL").unwrap_or(
            "postgres://quickwit-dev:quickwit-dev@localhost/quickwit-metastore-dev".to_string(),
        );
        let (_uri_protocol, uri_path) = test_database_url.split_once("://").unwrap();
        for protocol in &["postgres", "postgresql"] {
            let postgres_uri = format!("{}://{}", protocol, uri_path);
            metastore_resolver.resolve(&postgres_uri).await.unwrap();
        }
    }
}
