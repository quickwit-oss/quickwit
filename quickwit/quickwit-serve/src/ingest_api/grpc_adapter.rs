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
use quickwit_ingest_api::IngestApiService;
use quickwit_proto::ingest_api::{
    ingest_api_service_server as grpc, FetchRequest, FetchResponse, IngestRequest, IngestResponse,
    TailRequest,
};
use quickwit_proto::{convert_to_grpc_result, tonic};

#[derive(Clone)]
pub struct GrpcIngestApiAdapter(Mailbox<IngestApiService>);

impl From<Mailbox<IngestApiService>> for GrpcIngestApiAdapter {
    fn from(ingest_api_service: Mailbox<IngestApiService>) -> Self {
        GrpcIngestApiAdapter(ingest_api_service)
    }
}

#[async_trait]
impl grpc::IngestApiService for GrpcIngestApiAdapter {
    async fn ingest(
        &self,
        request: tonic::Request<IngestRequest>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let ingest_req = request.into_inner();
        let ingest_reply = self.0.ask_for_res(ingest_req).await;
        convert_to_grpc_result(ingest_reply)
    }

    async fn fetch(
        &self,
        request: tonic::Request<FetchRequest>,
    ) -> Result<tonic::Response<FetchResponse>, tonic::Status> {
        let fetch_req = request.into_inner();
        let fetch_reply = self.0.ask_for_res(fetch_req).await;
        convert_to_grpc_result(fetch_reply)
    }

    async fn tail(
        &self,
        request: tonic::Request<TailRequest>,
    ) -> Result<tonic::Response<FetchResponse>, tonic::Status> {
        let tail_req = request.into_inner();
        let tail_reply = self.0.ask_for_res(tail_req).await;
        convert_to_grpc_result(tail_reply)
    }
}
