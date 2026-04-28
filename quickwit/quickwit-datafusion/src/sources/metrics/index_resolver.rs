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
//! `MetastoreIndexResolver::resolve()` performs a single `index_metadata`
//! metastore RPC and returns the split provider plus the index's storage
//! `Uri`. The actual `Storage` (and its `ObjectStore` wrapper) is built
//! lazily by [`crate::object_store_registry::QuickwitObjectStoreRegistry`]
//! on the first read — see the docstring there for the overall flow.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use quickwit_common::uri::Uri;
use quickwit_metastore::{IndexMetadataResponseExt, ListIndexesMetadataResponseExt};
use quickwit_proto::metastore::{
    IndexMetadataRequest, ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use tracing::debug;

use super::metastore_provider::MetastoreSplitProvider;
use super::table_provider::MetricsSplitProvider;

/// Resolves per-index resources needed to scan a metrics index.
#[async_trait]
pub trait MetricsIndexResolver: Send + Sync + std::fmt::Debug {
    /// Returns the split provider and storage URI for `index_name`. The
    /// `ObjectStore` for that URI is built on demand by the registry the
    /// first time DataFusion reads from it.
    async fn resolve(&self, index_name: &str) -> DFResult<(Arc<dyn MetricsSplitProvider>, Uri)>;

    async fn list_index_names(&self) -> DFResult<Vec<String>>;
}

// ── Production implementation ─────────────────────────────────────────

/// Production `MetricsIndexResolver` backed by the Quickwit metastore.
///
/// `resolve()` fetches `IndexMetadata` and returns the URI. No
/// `StorageResolver` is held here — the object store registry resolves
/// storage lazily on first read.
#[derive(Clone)]
pub struct MetastoreIndexResolver {
    metastore: MetastoreServiceClient,
}

impl MetastoreIndexResolver {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        Self { metastore }
    }
}

impl std::fmt::Debug for MetastoreIndexResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetastoreIndexResolver").finish()
    }
}

#[async_trait]
impl MetricsIndexResolver for MetastoreIndexResolver {
    async fn resolve(&self, index_name: &str) -> DFResult<(Arc<dyn MetricsSplitProvider>, Uri)> {
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
        let index_uri = index_metadata.index_config.index_uri.clone();

        debug!(%index_uid, %index_uri, "resolved index metadata");

        let split_provider: Arc<dyn MetricsSplitProvider> = Arc::new(MetastoreSplitProvider::new(
            self.metastore.clone(),
            index_uid,
        ));

        Ok((split_provider, index_uri))
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
