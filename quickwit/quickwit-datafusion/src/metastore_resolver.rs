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

//! Production `MetricsIndexResolver` backed by the Quickwit metastore.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::ObjectStore;
use quickwit_metastore::{IndexMetadataResponseExt, ListIndexesMetadataResponseExt};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_storage::StorageResolver;
use tracing::debug;

use crate::catalog::MetricsIndexResolver;
use crate::metastore_provider::MetastoreSplitProvider;
use crate::storage::QuickwitObjectStore;
use crate::table_provider::MetricsSplitProvider;

/// Resolves index names via the metastore and storage resolver.
#[derive(Clone)]
pub struct MetastoreIndexResolver {
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
}

impl MetastoreIndexResolver {
    pub fn new(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
    ) -> Self {
        Self {
            metastore,
            storage_resolver,
        }
    }
}

impl std::fmt::Debug for MetastoreIndexResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastoreIndexResolver").finish()
    }
}

#[async_trait]
impl MetricsIndexResolver for MetastoreIndexResolver {
    async fn resolve(
        &self,
        index_name: &str,
    ) -> DFResult<(Arc<dyn MetricsSplitProvider>, Arc<dyn ObjectStore>, ObjectStoreUrl)> {
        debug!(index_name, "resolving metrics index");

        let request = IndexMetadataRequest::for_index_id(index_name.to_string());
        let response = self
            .metastore
            .clone()
            .index_metadata(request)
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let index_metadata = response.deserialize_index_metadata().map_err(|err| {
            datafusion::error::DataFusionError::External(Box::new(err))
        })?;

        let index_uid = index_metadata.index_uid.clone();
        let index_uri = &index_metadata.index_config.index_uri;

        debug!(%index_uid, %index_uri, "resolved index metadata");

        let storage = self
            .storage_resolver
            .resolve(index_uri)
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let object_store_url =
            ObjectStoreUrl::parse(format!("quickwit://{index_name}/")).map_err(|err| {
                datafusion::error::DataFusionError::Internal(format!(
                    "failed to parse object store url: {err}"
                ))
            })?;

        let object_store: Arc<dyn ObjectStore> = Arc::new(QuickwitObjectStore::new(storage));
        let split_provider: Arc<dyn MetricsSplitProvider> =
            Arc::new(MetastoreSplitProvider::new(self.metastore.clone(), index_uid));

        Ok((split_provider, object_store, object_store_url))
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        let request = ListIndexesMetadataRequest::all();
        let response = self
            .metastore
            .clone()
            .list_indexes_metadata(request)
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let indexes = response
            .deserialize_indexes_metadata()
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        Ok(indexes
            .into_iter()
            .map(|idx| idx.index_config.index_id)
            .collect())
    }
}
