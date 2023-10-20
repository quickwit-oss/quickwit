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

use std::fmt;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_proto::control_plane::{ControlPlaneService, ControlPlaneServiceClient};
use quickwit_proto::metastore::{
    AcquireShardsRequest, AcquireShardsResponse, AddSourceRequest, CloseShardsRequest,
    CloseShardsResponse, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteQuery,
    DeleteShardsRequest, DeleteShardsResponse, DeleteSourceRequest, DeleteSplitsRequest,
    DeleteTask, EmptyResponse, IndexMetadataRequest, IndexMetadataResponse,
    LastDeleteOpstampRequest, LastDeleteOpstampResponse, ListDeleteTasksRequest,
    ListDeleteTasksResponse, ListIndexesMetadataRequest, ListIndexesMetadataResponse,
    ListShardsRequest, ListShardsResponse, ListSplitsRequest, ListSplitsResponse,
    ListStaleSplitsRequest, MarkSplitsForDeletionRequest, MetastoreResult, MetastoreService,
    MetastoreServiceClient, OpenShardsRequest, OpenShardsResponse, PublishSplitsRequest,
    ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest, UpdateSplitsDeleteOpstampResponse,
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

    async fn check_connectivity(&mut self) -> anyhow::Result<()> {
        self.metastore.check_connectivity().await
    }

    // Proxied metastore API calls.

    async fn create_index(
        &mut self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse> {
        let response = self.control_plane.create_index(request).await?;
        Ok(response)
    }

    async fn delete_index(
        &mut self,
        request: DeleteIndexRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.delete_index(request).await?;
        Ok(response)
    }

    async fn add_source(&mut self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.add_source(request).await?;
        Ok(response)
    }

    async fn toggle_source(
        &mut self,
        request: ToggleSourceRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.clone().toggle_source(request).await?;
        Ok(response)
    }

    async fn delete_source(
        &mut self,
        request: DeleteSourceRequest,
    ) -> MetastoreResult<EmptyResponse> {
        let response = self.control_plane.delete_source(request).await?;
        Ok(response)
    }

    // Other metastore API calls.

    async fn index_metadata(
        &mut self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse> {
        self.metastore.index_metadata(request).await
    }

    async fn list_indexes_metadata(
        &mut self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse> {
        self.metastore.list_indexes_metadata(request).await
    }

    async fn stage_splits(
        &mut self,
        request: StageSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.stage_splits(request).await
    }

    async fn publish_splits(
        &mut self,
        request: PublishSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.publish_splits(request).await
    }

    async fn list_splits(
        &mut self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        self.metastore.list_splits(request).await
    }

    async fn list_stale_splits(
        &mut self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse> {
        self.metastore.list_stale_splits(request).await
    }

    async fn mark_splits_for_deletion(
        &mut self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.mark_splits_for_deletion(request).await
    }

    async fn delete_splits(
        &mut self,
        request: DeleteSplitsRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.delete_splits(request).await
    }

    async fn reset_source_checkpoint(
        &mut self,
        request: ResetSourceCheckpointRequest,
    ) -> MetastoreResult<EmptyResponse> {
        self.metastore.reset_source_checkpoint(request).await
    }

    // Delete tasks API

    async fn create_delete_task(
        &mut self,
        delete_query: DeleteQuery,
    ) -> MetastoreResult<DeleteTask> {
        self.metastore.create_delete_task(delete_query).await
    }

    async fn last_delete_opstamp(
        &mut self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse> {
        self.metastore.last_delete_opstamp(request).await
    }

    async fn update_splits_delete_opstamp(
        &mut self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse> {
        self.metastore.update_splits_delete_opstamp(request).await
    }

    async fn list_delete_tasks(
        &mut self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse> {
        self.metastore.list_delete_tasks(request).await
    }

    // Shard API

    async fn open_shards(
        &mut self,
        request: OpenShardsRequest,
    ) -> MetastoreResult<OpenShardsResponse> {
        self.metastore.open_shards(request).await
    }

    async fn acquire_shards(
        &mut self,
        request: AcquireShardsRequest,
    ) -> MetastoreResult<AcquireShardsResponse> {
        self.metastore.acquire_shards(request).await
    }

    async fn list_shards(
        &mut self,
        request: ListShardsRequest,
    ) -> MetastoreResult<ListShardsResponse> {
        self.metastore.list_shards(request).await
    }

    async fn close_shards(
        &mut self,
        request: CloseShardsRequest,
    ) -> MetastoreResult<CloseShardsResponse> {
        self.metastore.close_shards(request).await
    }

    async fn delete_shards(
        &mut self,
        request: DeleteShardsRequest,
    ) -> MetastoreResult<DeleteShardsResponse> {
        self.metastore.delete_shards(request).await
    }
}
