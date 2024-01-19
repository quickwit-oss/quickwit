// Copyright (C) 2024 Quickwit, Inc.
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
use quickwit_common::uri::Uri;
use quickwit_config::{MetastoreBackend, MetastoreConfig};
use quickwit_proto::metastore::MetastoreServiceClient;
use tokio::sync::Mutex;
use tracing::debug;

use crate::metastore::instrument_metastore;
use crate::{MetastoreFactory, MetastoreResolverError, PostgresqlMetastore};

#[derive(Clone, Default)]
pub struct PostgresqlMetastoreFactory {
    // Under normal conditions of use, this cache will contain a single `Metastore`.
    //
    // In contrast to the file-backed metastore, we use a strong pointer here, so that the
    // `Metastore` doesn't get dropped. This is done in order to keep the underlying connection
    // pool to Postgres alive.
    cache: Arc<Mutex<HashMap<Uri, MetastoreServiceClient>>>,
}

impl PostgresqlMetastoreFactory {
    async fn get_from_cache(&self, uri: &Uri) -> Option<MetastoreServiceClient> {
        let cache_lock = self.cache.lock().await;
        cache_lock.get(uri).map(MetastoreServiceClient::clone)
    }

    /// If there is a valid entry in the cache to begin with, we trash the new
    /// one and return the old one.
    ///
    /// This way we make sure that we keep only one instance associated
    /// to the key `uri` outside of this struct.
    async fn cache_metastore(
        &self,
        uri: Uri,
        metastore: MetastoreServiceClient,
    ) -> MetastoreServiceClient {
        let mut cache_lock = self.cache.lock().await;
        if let Some(metastore) = cache_lock.get(&uri) {
            return metastore.clone();
        }
        cache_lock.insert(uri, metastore.clone());
        metastore
    }
}

#[async_trait]
impl MetastoreFactory for PostgresqlMetastoreFactory {
    fn backend(&self) -> MetastoreBackend {
        MetastoreBackend::PostgreSQL
    }

    async fn resolve(
        &self,
        metastore_config: &MetastoreConfig,
        uri: &Uri,
    ) -> Result<MetastoreServiceClient, MetastoreResolverError> {
        if let Some(metastore) = self.get_from_cache(uri).await {
            debug!("using metastore from cache");
            return Ok(metastore);
        }
        debug!("metastore not found in cache");
        let postgresql_metastore_config = metastore_config.as_postgres().ok_or_else(|| {
            let message = format!(
                "expected PostgreSQL metastore config, got `{:?}`",
                metastore_config.backend()
            );
            MetastoreResolverError::InvalidConfig(message)
        })?;
        let postgresql_metastore = PostgresqlMetastore::new(postgresql_metastore_config, uri)
            .await
            .map_err(MetastoreResolverError::Initialization)?;
        let instrumented_metastore = instrument_metastore(postgresql_metastore);
        let unique_metastore_for_uri = self
            .cache_metastore(uri.clone(), instrumented_metastore)
            .await;
        Ok(unique_metastore_for_uri)
    }
}
