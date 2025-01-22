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

use std::fmt;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_proto::control_plane::{ControlPlaneService, ControlPlaneServiceClient};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, AddSourceRequest, CreateIndexRequest,
    CreateIndexResponse, CreateIndexTemplateRequest, DeleteIndexRequest,
    DeleteIndexTemplatesRequest, DeleteQuery, DeleteShardsRequest, DeleteShardsResponse,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse,
    FindIndexTemplateMatchesRequest, FindIndexTemplateMatchesResponse, GetIndexTemplateRequest,
    GetIndexTemplateResponse, IndexMetadataRequest, IndexMetadataResponse, IndexesMetadataRequest,
    IndexesMetadataResponse, LastDeleteOpstampRequest, LastDeleteOpstampResponse,
    ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexTemplatesRequest,
    ListIndexTemplatesResponse, ListIndexesMetadataRequest, ListIndexesMetadataResponse,
    ListShardsRequest, ListShardsResponse, ListSplitsRequest, ListSplitsResponse,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, MetastoreResult, MetastoreService,
    MetastoreServiceClient, MetastoreServiceStream, OpenShardsRequest, OpenShardsResponse,
    PruneShardsRequest, PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest,
    ToggleSourceRequest, UpdateIndexRequest, UpdateSplitsDeleteOpstampRequest,
    UpdateSplitsDeleteOpstampResponse,
};

/// A [`MetastoreService`] implementation that proxies some requests to the control plane so it can
/// track the state of the metastore accurately and react to events in real-time.
#[derive(Clone)]
pub struct ControlPlaneMetastore {
    control_plane: ControlPlaneServiceClient,
    metastore: MetastoreServiceClient,
}

impl fmt::Debug for ControlPlaneMetastore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ControlPlaneMetastore").finish()
    }
}

impl ControlPlaneMetastore {
    /// Creates a new [`ControlPlaneMetastore`].
    pub fn new(
        control_plane: ControlPlaneServiceClient,
        metastore: MetastoreServiceClient,
    ) -> Self {
        Self {
            control_plane,
            metastore,
        }
    }
}

#[async_trait]
impl MetastoreService for ControlPlaneMetastore {
    fn endpoints(&self) -> Vec<Uri> {
        self.metastore.endpoints()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.metastore.check_connectivity().await
    }

    // Proxied metastore API calls.

    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        let response = self.control_plane.create_index(request).await?;
        Ok(response)
    }

    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        let response = self.control_plane.update_index(request).await?;
        Ok(response)
    }

    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.delete_index(request).await?;
        Ok(response)
    }

    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.add_source(request).await?;
        Ok(response)
    }

    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.clone().toggle_source(request).await?;
        Ok(response)
    }

    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.delete_source(request).await?;
        Ok(response)
    }

    // Proxy through the control plane to debounce queries
    async fn prune_shards(&self, request: PruneShardsRequest) -> MetastoreResult<EmptyResponse> {
        self.control_plane.prune_shards(request).await?;
        Ok(EmptyResponse {})
    }

    // Other metastore API calls.

    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        self.metastore.index_metadata(request).await
    }

    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> MetastoreResult<IndexesMetadataResponse> {
        self.metastore.indexes_metadata(request).await
    }

    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        self.metastore.list_indexes_metadata(request).await
    }

    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.metastore.stage_splits(request).await
    }

    async fn publish_splits(
        &self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.publish_splits(request).await
    }

    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
        self.metastore.list_splits(request).await
    }

    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        self.metastore.list_stale_splits(request).await
    }

    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.mark_splits_for_deletion(request).await
    }

    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse> {
        self.metastore.delete_splits(request).await
    }

    async fn reset_source_checkpoint(
        &self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.reset_source_checkpoint(request).await
    }

    // Delete tasks API

    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        self.metastore.create_delete_task(delete_query).await
    }

    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        self.metastore.last_delete_opstamp(request).await
    }

    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.metastore.update_splits_delete_opstamp(request).await
    }

    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        self.metastore.list_delete_tasks(request).await
    }

    // Shard API

    async fn open_shards(&self, request: OpenShardsRequest) -> MetastoreResult<OpenShardsResponse> {
        self.metastore.open_shards(request).await
    }

    async fn acquire_shards(
        &self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        self.metastore.acquire_shards(request).await
    }

    async fn list_shards(&self, request: ListShardsRequest) -> MetastoreResult<ListShardsResponse> {
        self.metastore.list_shards(request).await
    }

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        self.metastore.delete_shards(request).await
    }

    // Index Template API

    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.create_index_template(request).await
    }

    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> MetastoreResult<GetIndexTemplateResponse> {
        self.metastore.get_index_template(request).await
    }

    async fn find_index_template_matches(
        &self,
        request: FindIndexTemplateMatchesRequest,
    ) -> MetastoreResult<FindIndexTemplateMatchesResponse> {
        self.metastore.find_index_template_matches(request).await
    }

    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> MetastoreResult<ListIndexTemplatesResponse> {
        self.metastore.list_index_templates(request).await
    }

    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.delete_index_templates(request).await
    }
}
