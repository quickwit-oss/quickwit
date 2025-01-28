// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{MetastoreBackend, MetastoreConfig};
use quickwit_proto::metastore::MetastoreServiceClient;
use tokio::sync::Mutex;
use tracing::debug;

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
        cache_lock.get(uri).cloned()
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
            .map(MetastoreServiceClient::new)
            .map_err(MetastoreResolverError::Initialization)?;
        let unique_metastore_for_uri = self
            .cache_metastore(uri.clone(), postgresql_metastore)
            .await;
        Ok(unique_metastore_for_uri)
    }
}
