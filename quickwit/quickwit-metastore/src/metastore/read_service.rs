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

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::metastore::{
    IndexMetadataRequest, IndexMetadataResponse, ListIndexesMetadataRequest,
    ListIndexesMetadataResponse, ListMetricsSplitsRequest, ListMetricsSplitsResponse,
    ListSketchSplitsRequest, ListSketchSplitsResponse, ListSplitsRequest, ListSplitsResponse,
    MetastoreResult, MetastoreService, MetastoreServiceClient, MetastoreServiceStream,
};

/// Read-only subset of the metastore RPC surface that is safe to route to a read replica.
///
/// Callers that only need stale-tolerant reads (e.g. searchers, the DataFusion analytics path)
/// should depend on this trait rather than on the full [`MetastoreService`], so that writes are
/// excluded at the type level. Add a method here only for RPCs that tolerate replication lag.
#[async_trait]
pub trait MetastoreReadService: fmt::Debug + Send + Sync + 'static {
    /// Fetches index metadata.
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse>;

    /// Lists indexes metadata.
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse>;

    /// Streams splits from indexes.
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>>;

    /// Lists metrics parquet splits.
    async fn list_metrics_splits(
        &self,
        request: ListMetricsSplitsRequest,
    ) -> MetastoreResult<ListMetricsSplitsResponse>;

    /// Lists sketch parquet splits.
    async fn list_sketch_splits(
        &self,
        request: ListSketchSplitsRequest,
    ) -> MetastoreResult<ListSketchSplitsResponse>;
}

/// Cloneable read-only metastore handle.
///
/// `Arc<dyn MetastoreReadService>` derefs to `dyn MetastoreReadService`, so a
/// `&MetastoreReadServiceClient` coerces to the `&dyn MetastoreReadService` taken by the read-only
/// helpers — no newtype or blanket `impl ... for Arc<T>` is needed.
pub type MetastoreReadServiceClient = Arc<dyn MetastoreReadService>;

#[async_trait]
impl MetastoreReadService for MetastoreServiceClient {
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        MetastoreService::index_metadata(self, request).await
    }

    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        MetastoreService::list_indexes_metadata(self, request).await
    }

    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        MetastoreService::list_splits(self, request).await
    }

    async fn list_metrics_splits(
        &self,
        request: ListMetricsSplitsRequest,
    ) -> MetastoreResult<ListMetricsSplitsResponse> {
        MetastoreService::list_metrics_splits(self, request).await
    }

    async fn list_sketch_splits(
        &self,
        request: ListSketchSplitsRequest,
    ) -> MetastoreResult<ListSketchSplitsResponse> {
        MetastoreService::list_sketch_splits(self, request).await
    }
}
