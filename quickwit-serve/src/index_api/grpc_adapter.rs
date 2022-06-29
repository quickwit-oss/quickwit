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
use quickwit_actors::Mailbox;
use quickwit_control_plane::actors::IndexService;
use quickwit_proto::{
    index_service_server as grpc, tonic, GetIndexMetadataRequest, GetIndexMetadataResponse,
    GetSplitsMetadatasRequest, GetSplitsMetadatasResponse, PublishSplitRequest,
    ReplaceSplitsRequest, SplitResponse, StageSplitRequest,
};

use crate::error::convert_to_grpc_result;

#[derive(Clone)]
pub struct GrpcIndexServiceAdapter(Mailbox<IndexService>);

impl From<Mailbox<IndexService>> for GrpcIndexServiceAdapter {
    fn from(index_service: Mailbox<IndexService>) -> Self {
        Self(index_service)
    }
}

#[async_trait]
impl grpc::IndexService for GrpcIndexServiceAdapter {
    async fn get_index_metadata(
        &self,
        request: tonic::Request<GetIndexMetadataRequest>,
    ) -> Result<tonic::Response<GetIndexMetadataResponse>, tonic::Status> {
        let index_req = request.into_inner();
        let index_reply = self
            .0
            .ask_for_res(index_req)
            .await
            // TODO: remove unwrap.
            // Or serialize in the service?
            .map(|index_metadata| serde_json::to_string(&index_metadata).unwrap())
            .map(|index_metadata_serialized_json| GetIndexMetadataResponse {
                index_metadata_serialized_json,
            });
        convert_to_grpc_result(index_reply)
    }

    async fn get_splits_metadatas(
        &self,
        request: tonic::Request<GetSplitsMetadatasRequest>,
    ) -> Result<tonic::Response<GetSplitsMetadatasResponse>, tonic::Status> {
        let splits_req = request.into_inner();
        let splits_reply = self
            .0
            .ask_for_res(splits_req)
            .await
            // TODO: remove unwrap.
            // Or serialize in the service?
            .map(|splits| serde_json::to_string(&splits).unwrap())
            .map(
                |splits_metadatas_serialized_json| GetSplitsMetadatasResponse {
                    splits_metadatas_serialized_json,
                },
            );
        convert_to_grpc_result(splits_reply)
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

    async fn publish_split(
        &self,
        request: tonic::Request<PublishSplitRequest>,
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
}
