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

//! Index resolution for the metrics data source.
//!
//! `MetastoreIndexResolver::resolve()` performs two RPCs per call:
//! 1. `index_metadata` — cheap primary-key lookup, always fresh.
//! 2. `storage_resolver.resolve(uri)` — constructs a `Storage` handle.
//!
//! Caching of the `Storage` handle (to amortise repeated resolve calls for the
//! same index) is intentionally deferred to a follow-up. The quickwit search
//! path also resolves storage on every leaf request without caching and
//! relies on the split-byte cache (`SplitCache`) instead.

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

use super::metastore_provider::MetastoreSplitProvider;
use super::table_provider::MetricsSplitProvider;
use crate::storage_bridge::QuickwitObjectStore;

/// Resolves per-index resources needed to scan a metrics index.
#[async_trait]
pub trait MetricsIndexResolver: Send + Sync + std::fmt::Debug {
    async fn resolve(
        &self,
        index_name: &str,
    ) -> DFResult<(Arc<dyn MetricsSplitProvider>, Arc<dyn ObjectStore>, ObjectStoreUrl)>;

    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}

// ── Test helper ──────────────────────────────────────────────────────

/// Single-store resolver — returns the same resources for every index name.
#[cfg(any(test, feature = "testsuite"))]
#[derive(Debug)]
pub struct SimpleIndexResolver {
    split_provider: Arc<dyn MetricsSplitProvider>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    index_names: Vec<String>,
}

#[cfg(any(test, feature = "testsuite"))]
impl SimpleIndexResolver {
    pub fn new(
        split_provider: Arc<dyn MetricsSplitProvider>,
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            split_provider,
            object_store,
            object_store_url,
            index_names: vec!["metrics".to_string()],
        }
    }

    pub fn with_index_names(mut self, names: Vec<String>) -> Self {
        self.index_names = names;
        self
    }
}

#[cfg(any(test, feature = "testsuite"))]
#[async_trait]
impl MetricsIndexResolver for SimpleIndexResolver {
    async fn resolve(
        &self,
        _index_name: &str,
    ) -> DFResult<(Arc<dyn MetricsSplitProvider>, Arc<dyn ObjectStore>, ObjectStoreUrl)> {
        Ok((
            Arc::clone(&self.split_provider),
            Arc::clone(&self.object_store),
            self.object_store_url.clone(),
        ))
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        Ok(self.index_names.clone())
    }
}

// ── Production implementation ─────────────────────────────────────────

/// Production `MetricsIndexResolver` backed by the Quickwit metastore.
///
/// Each `resolve()` call:
/// 1. Fetches `IndexMetadata` (cheap primary-key RPC) for a fresh `index_uid`.
/// 2. Calls `storage_resolver.resolve(uri)` to obtain a `Storage` handle.
#[derive(Clone)]
pub struct MetastoreIndexResolver {
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
}

impl MetastoreIndexResolver {
    pub fn new(metastore: MetastoreServiceClient, storage_resolver: StorageResolver) -> Self {
        Self { metastore, storage_resolver }
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

        let response = self
            .metastore
            .clone()
            .index_metadata(IndexMetadataRequest::for_index_id(index_name.to_string()))
            .await
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

        let index_metadata = response
            .deserialize_index_metadata()
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;

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
                    "failed to build object store url: {err}"
                ))
            })?;

        let object_store: Arc<dyn ObjectStore> = Arc::new(QuickwitObjectStore::new(storage));
        let split_provider: Arc<dyn MetricsSplitProvider> =
            Arc::new(MetastoreSplitProvider::new(self.metastore.clone(), index_uid));

        Ok((split_provider, object_store, object_store_url))
    }

    async fn list_index_names(&self) -> DFResult<Vec<String>> {
        let response = self
            .metastore
            .clone()
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
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
