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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteQuery,
    DeleteSourceRequest, DeleteSplitsRequest, DeleteTask, EmptyResponse, IndexMetadataRequest,
    IndexMetadataResponse, LastDeleteOpstampRequest, LastDeleteOpstampResponse,
    ListDeleteTasksRequest, ListDeleteTasksResponse, ListIndexesRequest, ListIndexesResponse,
    ListSplitsRequest, ListSplitsResponse, MarkSplitsForDeletionRequest, MetastoreService,
    PublishSplitsRequest, ResetSourceCheckpointRequest, StageSplitsRequest, ToggleSourceRequest,
    UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::set_parent_span_from_request_metadata;
use quickwit_proto::tonic::{Request, Response, Status};
use tracing::instrument;

use crate::{Metastore, MetastoreError};

#[allow(missing_docs)]
#[derive(Clone)]
pub struct GrpcMetastoreAdapter(Arc<dyn Metastore>);

impl From<Arc<dyn Metastore>> for GrpcMetastoreAdapter {
    fn from(metastore: Arc<dyn Metastore>) -> Self {
        Self(metastore)
    }
}

#[async_trait]
impl MetastoreService for GrpcMetastoreAdapter {
    #[instrument(skip(self, request))]
    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .create_index(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn index_metadata(
        &self,
        request: Request<IndexMetadataRequest>,
    ) -> Result<Response<IndexMetadataResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .index_metadata(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .list_indexes(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn delete_index(
        &self,
        request: Request<DeleteIndexRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .delete_index(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn list_splits(
        &self,
        request: Request<ListSplitsRequest>,
    ) -> Result<Response<ListSplitsResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .list_splits(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn stage_splits(
        &self,
        request: Request<StageSplitsRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .stage_splits(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn publish_splits(
        &self,
        request: Request<PublishSplitsRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .publish_splits(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn mark_splits_for_deletion(
        &self,
        request: Request<MarkSplitsForDeletionRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .mark_splits_for_deletion(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn delete_splits(
        &self,
        request: Request<DeleteSplitsRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .delete_splits(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn add_source(
        &self,
        request: Request<AddSourceRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .add_source(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn toggle_source(
        &self,
        request: Request<ToggleSourceRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .toggle_source(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .delete_source(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn reset_source_checkpoint(
        &self,
        request: Request<ResetSourceCheckpointRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .reset_source_checkpoint(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn last_delete_opstamp(
        &self,
        request: Request<LastDeleteOpstampRequest>,
    ) -> Result<Response<LastDeleteOpstampResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        let request = request.into_inner();
        let last_delete_opstamp = self.0.last_delete_opstamp(request.index_uid.into()).await?;
        let response = LastDeleteOpstampResponse {
            last_delete_opstamp,
        };
        Ok(Response::new(response))
    }

    #[instrument(skip(self, request))]
    async fn create_delete_task(
        &self,
        request: Request<DeleteQuery>,
    ) -> Result<Response<DeleteTask>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .create_delete_task(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn update_splits_delete_opstamp(
        &self,
        request: Request<UpdateSplitsDeleteOpstampRequest>,
    ) -> Result<Response<EmptyResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .update_splits_delete_opstamp(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }

    #[instrument(skip(self, request))]
    async fn list_delete_tasks(
        &self,
        request: Request<ListDeleteTasksRequest>,
    ) -> Result<Response<ListDeleteTasksResponse>, Status> {
        set_parent_span_from_request_metadata(request.metadata());
        self.0
            .list_delete_tasks(request.into_inner())
            .await
            .map(Response::new)
            .map_err(|error| error.into())
    }
}
