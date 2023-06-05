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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use anyhow::ensure;
use once_cell::sync::Lazy;
use quickwit_common::uri::{Protocol, Uri};
use quickwit_config::{MetastoreBackend, MetastoreConfig, MetastoreConfigs};
use quickwit_storage::StorageResolver;

use crate::metastore::file_backed_metastore::FileBackedMetastoreFactory;
#[cfg(feature = "postgres")]
use crate::metastore::postgresql_metastore::PostgresqlMetastoreFactory;
use crate::{Metastore, MetastoreFactory, MetastoreResolverError};

type FactoryAndConfig = (Box<dyn MetastoreFactory>, MetastoreConfig);

/// Returns the [`Metastore`] instance associated with the protocol of a URI. The actual creation of
/// metastore objects is delegated to pre-registered [`MetastoreFactory`]. The resolver is only
/// responsible for dispatching to the appropriate factory.
#[derive(Clone)]
pub struct MetastoreResolver {
    per_backend_factories: Arc<HashMap<MetastoreBackend, FactoryAndConfig>>,
}

impl fmt::Debug for MetastoreResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetastoreResolver").finish()
    }
}

impl MetastoreResolver {
    /// Creates an empty [`MetastoreResolverBuilder`].
    pub fn builder() -> MetastoreResolverBuilder {
        MetastoreResolverBuilder::default()
    }

    /// Resolves the given `uri`.
    pub async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let backend = match uri.protocol() {
            Protocol::Azure => MetastoreBackend::File,
            Protocol::File => MetastoreBackend::File,
            Protocol::Ram => MetastoreBackend::File,
            Protocol::S3 => MetastoreBackend::File,
            Protocol::PostgreSQL => MetastoreBackend::PostgreSQL,
            _ => {
                return Err(MetastoreResolverError::UnsupportedBackend(
                    "no implementation exists for this backend.".to_string(),
                ))
            }
        };
        let (metastore_factory, metastore_config) = self
            .per_backend_factories
            .get(&backend)
            .ok_or(MetastoreResolverError::UnsupportedBackend(
                "no metastore factory is registered for this backend.".to_string(),
            ))?;
        let metastore = metastore_factory.resolve(metastore_config, uri).await?;
        Ok(metastore)
    }

    /// Creates and returns a [`MetastoreResolver`] holding the default configuration for each
    /// backend. Note that if the environment (env vars, instance metadata, ...) fails
    /// to provide the necessary credentials, the default Azure or S3 file-backed metastores
    /// returned by this resolver will not work.
    pub fn unconfigured() -> Self {
        static METASTORE_RESOLVER: Lazy<MetastoreResolver> = Lazy::new(|| {
            MetastoreResolver::configured(
                StorageResolver::unconfigured(),
                &MetastoreConfigs::default(),
            )
        });
        METASTORE_RESOLVER.clone()
    }

    /// Creates and returns a [`MetastoreResolver`].
    pub fn configured(
        storage_resolver: StorageResolver,
        metastore_configs: &MetastoreConfigs,
    ) -> Self {
        let mut builder = MetastoreResolver::builder().register(
            FileBackedMetastoreFactory::new(storage_resolver),
            metastore_configs
                .find_file()
                .cloned()
                .unwrap_or_default()
                .into(),
        );
        #[cfg(feature = "postgres")]
        {
            builder = builder.register(
                PostgresqlMetastoreFactory::default(),
                metastore_configs
                    .find_postgres()
                    .cloned()
                    .unwrap_or_default()
                    .into(),
            );
        }
        #[cfg(not(feature = "postgres"))]
        {
            use quickwit_config::PostgresMetastoreConfig;

            use crate::UnsupportedMetastore;

            builder = builder.register(
                UnsupportedMetastore::new(
                    MetastoreBackend::PostgreSQL,
                    "Quickwit was compiled without the `postgres` feature.",
                ),
                PostgresMetastoreConfig::default().into(),
            );
        }
        builder
            .build()
            .expect("Metastore factory and config backends should match.")
    }
}

#[derive(Default)]
pub struct MetastoreResolverBuilder {
    per_protocol_factories: HashMap<MetastoreBackend, (Box<dyn MetastoreFactory>, MetastoreConfig)>,
}

impl MetastoreResolverBuilder {
    pub fn register<S: MetastoreFactory>(
        mut self,
        metastore_factory: S,
        metastore_config: MetastoreConfig,
    ) -> Self {
        self.per_protocol_factories.insert(
            metastore_factory.backend(),
            (Box::new(metastore_factory), metastore_config),
        );
        self
    }

    pub fn build(self) -> anyhow::Result<MetastoreResolver> {
        for (metastore_factory, metastore_config) in self.per_protocol_factories.values() {
            ensure!(
                metastore_factory.backend() == metastore_config.backend(),
                "Metastore factory and config backends do not match: {:?} vs. {:?}.",
                metastore_factory.backend(),
                metastore_config.backend(),
            );
        }
        let metastore_resolver = MetastoreResolver {
            per_backend_factories: Arc::new(self.per_protocol_factories),
        };
        Ok(metastore_resolver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metastore_resolver_should_not_raise_errors_on_file() {
        let metastore_resolver = MetastoreResolver::unconfigured();
        let tmp_dir = tempfile::tempdir().unwrap();
        let metastore_filepath = format!("file://{}/metastore", tmp_dir.path().display());
        let metastore_uri = Uri::from_well_formed(&metastore_filepath);
        metastore_resolver.resolve(&metastore_uri).await.unwrap();
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn test_postgres_and_postgresql_protocol_accepted() {
        use std::env;
        let metastore_resolver = MetastoreResolver::unconfigured();
        // If the database defined in the env var or the default one is not up, the
        // test block after making 10 attempts with a timeout of 10s each = 100s.
        let test_database_url = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgres://quickwit-dev:quickwit-dev@localhost/quickwit-metastore-dev".to_string()
        });
        let (_uri_protocol, uri_path) = test_database_url.split_once("://").unwrap();
        for protocol in &["postgres", "postgresql"] {
            let postgres_uri = Uri::from_well_formed(format!("{protocol}://{uri_path}"));
            metastore_resolver.resolve(&postgres_uri).await.unwrap();
        }
    }
}
