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

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, AddSourceRequest, CreateIndexRequest,
    CreateIndexResponse, CreateIndexTemplateRequest, DeleteIndexRequest,
    DeleteIndexTemplatesRequest, DeleteMetricsSplitsRequest, DeleteQuery, DeleteShardsRequest,
    DeleteShardsResponse, DeleteSketchSplitsRequest, DeleteSourceRequest, DeleteSplitsRequest,
    DeleteTask, EmptyResponse, FindIndexTemplateMatchesRequest, FindIndexTemplateMatchesResponse,
    GetClusterIdentityRequest, GetClusterIdentityResponse, GetIndexTemplateRequest,
    GetIndexTemplateResponse, IndexMetadataRequest, IndexMetadataResponse, IndexesMetadataRequest,
    IndexesMetadataResponse, LastDeleteOpstampRequest, LastDeleteOpstampResponse,
    ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexStatsRequest, ListIndexStatsResponse,
    ListIndexTemplatesRequest, ListIndexTemplatesResponse, ListIndexesMetadataRequest,
    ListIndexesMetadataResponse, ListMetricsSplitsRequest, ListMetricsSplitsResponse,
    ListShardsRequest, ListShardsResponse, ListSketchSplitsRequest, ListSketchSplitsResponse,
    ListSplitsRequest, ListSplitsResponse, ListStaleSplitsRequest,
    MarkMetricsSplitsForDeletionRequest, MarkSketchSplitsForDeletionRequest,
    MarkSplitsForDeletionRequest, MetastoreResult, MetastoreService, MetastoreServiceClient,
    MetastoreServiceStream, OpenShardsRequest, OpenShardsResponse, PruneShardsRequest,
    PublishMetricsSplitsRequest, PublishSketchSplitsRequest, PublishSplitsRequest,
    ResetSourceCheckpointRequest, StageMetricsSplitsRequest, StageSketchSplitsRequest,
    StageSplitsRequest, ToggleSourceRequest, UpdateIndexRequest, UpdateSourceRequest,
    UpdateSplitsDeleteOpstampRequest, UpdateSplitsDeleteOpstampResponse,
};
use tokio::sync::watch;

/// A [`MetastoreService`] implementation that routes stale-tolerant, read-only RPCs to a read
/// replica metastore when one is available, and everything else to the primary metastore.
///
/// This is meant to be handed only to read-only callers (the searcher and the DataFusion analytics
/// path). Write RPCs are delegated to the primary so that, even if one is ever issued, it is
/// served correctly rather than failing against the read-only replica.
///
/// Routing is decided per request from the live connection set of the read replica balance
/// channel: when no replica node is connected, reads fall back to the primary. The check is a
/// synchronous read of a [`watch`] channel and never holds a borrow across an await point.
#[derive(Clone)]
pub struct ReadReplicaRoutingMetastore {
    primary: MetastoreServiceClient,
    read_replica: MetastoreServiceClient,
    read_replica_connections: watch::Receiver<HashSet<SocketAddr>>,
}

impl fmt::Debug for ReadReplicaRoutingMetastore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ReadReplicaRoutingMetastore").finish()
    }
}

impl ReadReplicaRoutingMetastore {
    /// Creates a new [`ReadReplicaRoutingMetastore`].
    ///
    /// `read_replica_connections` is the connection-set watcher of the read replica balance
    /// channel (see `BalanceChannel::connection_keys_watcher`).
    pub fn new(
        primary: MetastoreServiceClient,
        read_replica: MetastoreServiceClient,
        read_replica_connections: watch::Receiver<HashSet<SocketAddr>>,
    ) -> Self {
        Self {
            primary,
            read_replica,
            read_replica_connections,
        }
    }

    /// Returns the metastore to use for stale-tolerant read RPCs: the read replica when at least
    /// one replica node is connected, the primary otherwise.
    fn read_metastore(&self) -> &MetastoreServiceClient {
        if self.read_replica_connections.borrow().is_empty() {
            &self.primary
        } else {
            &self.read_replica
        }
    }
}

#[async_trait]
impl MetastoreService for ReadReplicaRoutingMetastore {
    fn endpoints(&self) -> Vec<Uri> {
        self.primary.endpoints()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.primary.check_connectivity().await
    }

    // Stale-tolerant read RPCs issued by the search and analytics paths: routed to the read replica
    // when available.

    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        self.read_metastore().index_metadata(request).await
    }

    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> MetastoreResult<IndexesMetadataResponse> {
        self.read_metastore().indexes_metadata(request).await
    }

    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        self.read_metastore().list_indexes_metadata(request).await
    }

    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        self.read_metastore().list_splits(request).await
    }

    async fn list_metrics_splits(
        &self,
        request: ListMetricsSplitsRequest,
    ) -> MetastoreResult<ListMetricsSplitsResponse> {
        self.read_metastore().list_metrics_splits(request).await
    }

    async fn list_sketch_splits(
        &self,
        request: ListSketchSplitsRequest,
    ) -> MetastoreResult<ListSketchSplitsResponse> {
        self.read_metastore().list_sketch_splits(request).await
    }

    // All other RPCs are delegated to the primary. Writes must never hit the read-only replica, and
    // the remaining reads are not on the search/analytics hot path, so they keep read-your-writes
    // consistency against the primary.

    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        self.primary.create_index(request).await
    }

    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        self.primary.update_index(request).await
    }

    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.delete_index(request).await
    }

    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.add_source(request).await
    }

    async fn update_source(&self, request: UpdateSourceRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.update_source(request).await
    }

    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.toggle_source(request).await
    }

    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.delete_source(request).await
    }

    async fn prune_shards(&self, request: PruneShardsRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.prune_shards(request).await
    }

    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.stage_splits(request).await
    }

    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.publish_splits(request).await
    }

    async fn list_index_stats(
        &self,
        request: ListIndexStatsRequest,
    ) -> MetastoreResult<ListIndexStatsResponse> {
        self.primary.list_index_stats(request).await
    }

    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        self.primary.list_stale_splits(request).await
    }

    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.mark_splits_for_deletion(request).await
    }

    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.primary.delete_splits(request).await
    }

    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.reset_source_checkpoint(request).await
    }

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        self.primary.create_delete_task(delete_query).await
    }

    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        self.primary.last_delete_opstamp(request).await
    }

    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.primary.update_splits_delete_opstamp(request).await
    }

    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        self.primary.list_delete_tasks(request).await
    }

    async fn open_shards(&self, request: OpenShardsRequest) -> MetastoreResult<OpenShardsResponse> {
        self.primary.open_shards(request).await
    }

    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        self.primary.acquire_shards(request).await
    }

    async fn list_shards(&self, request: ListShardsRequest) -> MetastoreResult<ListShardsResponse> {
        self.primary.list_shards(request).await
    }

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        self.primary.delete_shards(request).await
    }

    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.create_index_template(request).await
    }

    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> MetastoreResult<GetIndexTemplateResponse> {
        self.primary.get_index_template(request).await
    }

    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> MetastoreResult<FindIndexTemplateMatchesResponse> {
        self.primary.find_index_template_matches(request).await
    }

    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> MetastoreResult<ListIndexTemplatesResponse> {
        self.primary.list_index_templates(request).await
    }

    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.delete_index_templates(request).await
    }

    async fn get_cluster_identity(
        &self,
        request: GetClusterIdentityRequest,
    ) -> MetastoreResult<GetClusterIdentityResponse> {
        self.primary.get_cluster_identity(request).await
    }

    async fn stage_metrics_splits(
        &self,
        request: StageMetricsSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.stage_metrics_splits(request).await
    }

    async fn publish_metrics_splits(
        &self,
        request: PublishMetricsSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.publish_metrics_splits(request).await
    }

    async fn mark_metrics_splits_for_deletion(
        &self,
        request: MarkMetricsSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.mark_metrics_splits_for_deletion(request).await
    }

    async fn delete_metrics_splits(
        &self,
        request: DeleteMetricsSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.delete_metrics_splits(request).await
    }

    async fn stage_sketch_splits(
        &self,
        request: StageSketchSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.stage_sketch_splits(request).await
    }

    async fn publish_sketch_splits(
        &self,
        request: PublishSketchSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.publish_sketch_splits(request).await
    }

    async fn mark_sketch_splits_for_deletion(
        &self,
        request: MarkSketchSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.mark_sketch_splits_for_deletion(request).await
    }

    async fn delete_sketch_splits(
        &self,
        request: DeleteSketchSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.primary.delete_sketch_splits(request).await
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::metastore::MockMetastoreService;
    use tokio::sync::watch;

    use super::*;
    use crate::ListIndexesMetadataResponseExt;

    /// Builds a metastore client whose `list_indexes_metadata` is expected to be called exactly
    /// `times` times.
    fn mock_metastore_expecting_list_indexes(times: usize) -> MetastoreServiceClient {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(times)
            .returning(|_request| Ok(ListIndexesMetadataResponse::for_test(Vec::new())));
        MetastoreServiceClient::from_mock(mock_metastore)
    }

    #[tokio::test]
    async fn test_routes_reads_to_primary_when_no_replica_is_connected() {
        let (_connections_tx, connections_rx) = watch::channel(HashSet::<SocketAddr>::new());
        let metastore = ReadReplicaRoutingMetastore::new(
            mock_metastore_expecting_list_indexes(1),
            mock_metastore_expecting_list_indexes(0),
            connections_rx,
        );
        metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_routes_reads_to_replica_when_connected() {
        let replica_addr: SocketAddr = "127.0.0.1:7281".parse().unwrap();
        let (_connections_tx, connections_rx) = watch::channel(HashSet::from([replica_addr]));
        let metastore = ReadReplicaRoutingMetastore::new(
            mock_metastore_expecting_list_indexes(0),
            mock_metastore_expecting_list_indexes(1),
            connections_rx,
        );
        metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
            .unwrap();
    }
}
