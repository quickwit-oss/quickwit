// Copyright (C) 2022 Quickwit, Inc.
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

use async_trait::async_trait;
use quickwit_actors::{AskError, Mailbox};
use quickwit_proto::index_management_service_server::{self as grpc};
use quickwit_proto::{
    tonic, CreateIndexRequest, CreateIndexResponse, DeleteIndexRequest, DeleteIndexResponse,
    IndexMetadataRequest, IndexMetadataResponse, ListAllSplitsRequest, ListIndexesMetadatasRequest,
    ListIndexesMetadatasResponse, ListSplitsRequest, ListSplitsResponse,
    MarkSplitsForDeletionRequest, PublishSplitsRequest, ReplaceSplitsRequest, SplitResponse,
    StageSplitRequest,
};

use crate::IndexManagementService;

#[derive(Clone)]
pub struct GrpcIndexManagementServiceAdapter(Mailbox<IndexManagementService>);

impl GrpcIndexManagementServiceAdapter {
    pub fn new(index_management_service_mailbox: Mailbox<IndexManagementService>) -> Self {
        Self(index_management_service_mailbox)
    }
}

impl From<Mailbox<IndexManagementService>> for GrpcIndexManagementServiceAdapter {
    fn from(index_service: Mailbox<IndexManagementService>) -> Self {
        Self(index_service)
    }
}

#[async_trait]
impl grpc::IndexManagementService for GrpcIndexManagementServiceAdapter {
    async fn create_index(
        &self,
        request: tonic::Request<CreateIndexRequest>,
    ) -> Result<tonic::Response<CreateIndexResponse>, tonic::Status> {
        let index_req = request.into_inner();
        let index_metadata = self.0.ask_for_res(index_req).await.map_err(convert_error)?;
        let index_metadata_serialized_json =
            serde_json::to_string(&index_metadata).map_err(convert_error)?;
        Ok(tonic::Response::new(CreateIndexResponse {
            index_metadata_serialized_json,
        }))
    }

    async fn index_metadata(
        &self,
        request: tonic::Request<IndexMetadataRequest>,
    ) -> Result<tonic::Response<IndexMetadataResponse>, tonic::Status> {
        let index_req = request.into_inner();
        let index_metadata = self.0.ask_for_res(index_req).await.map_err(convert_error)?;
        let index_metadata_serialized_json =
            serde_json::to_string(&index_metadata).map_err(convert_error)?;
        Ok(tonic::Response::new(IndexMetadataResponse {
            index_metadata_serialized_json,
        }))
    }

    async fn list_indexes_metadatas(
        &self,
        request: tonic::Request<ListIndexesMetadatasRequest>,
    ) -> Result<tonic::Response<ListIndexesMetadatasResponse>, tonic::Status> {
        let indexes_req = request.into_inner();
        let indexes_metadatas = self
            .0
            .ask_for_res(indexes_req)
            .await
            .map_err(convert_error)?;
        let indexes_metadatas_serialized_json =
            serde_json::to_string(&indexes_metadatas).map_err(convert_error)?;
        Ok(tonic::Response::new(ListIndexesMetadatasResponse {
            indexes_metadatas_serialized_json,
        }))
    }

    async fn delete_index(
        &self,
        request: tonic::Request<DeleteIndexRequest>,
    ) -> Result<tonic::Response<DeleteIndexResponse>, tonic::Status> {
        let delete_req = request.into_inner();
        let delete_reply = self
            .0
            .ask_for_res(delete_req)
            .await
            .map(|file_entries| DeleteIndexResponse { file_entries });
        convert_to_grpc_result(delete_reply)
    }

    async fn list_all_splits(
        &self,
        request: tonic::Request<ListAllSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        let splits_req = request.into_inner();
        let splits = self
            .0
            .ask_for_res(splits_req)
            .await
            .map_err(convert_error)?;
        let splits_serialized_json = serde_json::to_string(&splits).map_err(convert_error)?;
        Ok(tonic::Response::new(ListSplitsResponse {
            splits_serialized_json,
        }))
    }

    async fn list_splits(
        &self,
        request: tonic::Request<ListSplitsRequest>,
    ) -> Result<tonic::Response<ListSplitsResponse>, tonic::Status> {
        let splits_req = request.into_inner();
        let splits = self
            .0
            .ask_for_res(splits_req)
            .await
            .map_err(convert_error)?;
        let splits_serialized_json = serde_json::to_string(&splits).map_err(convert_error)?;
        Ok(tonic::Response::new(ListSplitsResponse {
            splits_serialized_json,
        }))
    }

    async fn stage_split(
        &self,
        request: tonic::Request<StageSplitRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        let stage_req = request.into_inner();
        let stage_reply = self
            .0
            .ask_for_res(stage_req)
            .await
            .map(|_| SplitResponse {});
        convert_to_grpc_result(stage_reply)
    }

    async fn publish_splits(
        &self,
        request: tonic::Request<PublishSplitsRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        let publish_req = request.into_inner();
        let publish_reply = self
            .0
            .ask_for_res(publish_req)
            .await
            .map(|_| SplitResponse {});
        convert_to_grpc_result(publish_reply)
    }

    async fn replace_splits(
        &self,
        request: tonic::Request<ReplaceSplitsRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        let replace_req = request.into_inner();
        let replace_reply = self
            .0
            .ask_for_res(replace_req)
            .await
            .map(|_| SplitResponse {});
        convert_to_grpc_result(replace_reply)
    }

    async fn mark_splits_for_deletion(
        &self,
        request: tonic::Request<MarkSplitsForDeletionRequest>,
    ) -> Result<tonic::Response<SplitResponse>, tonic::Status> {
        let marks_splits_for_deletion_req = request.into_inner();
        let replace_reply = self
            .0
            .ask_for_res(marks_splits_for_deletion_req)
            .await
            .map(|_| SplitResponse {});
        convert_to_grpc_result(replace_reply)
    }
}

// TODO: process errors correctly.
pub(crate) fn convert_to_grpc_result<T, E: ToString + std::fmt::Debug>(
    res: Result<T, AskError<E>>,
) -> Result<tonic::Response<T>, tonic::Status> {
    res.map(|outcome| tonic::Response::new(outcome))
        .map_err(|err| match err {
            AskError::MessageNotDelivered => tonic::Status::internal("message"),
            AskError::ProcessMessageError => tonic::Status::internal("message"),
            AskError::ErrorReply(err) => tonic::Status::internal(err.to_string()),
        })
}

pub(crate) fn convert_error<E: ToString>(error: E) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}
