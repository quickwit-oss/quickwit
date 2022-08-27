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
use quickwit_proto::indexing_api::indexing_service_server::{self as grpc};
use quickwit_proto::indexing_api::{ApplyIndexingPlanRequest, ApplyIndexingPlanResponse};
use quickwit_proto::{convert_to_grpc_result, tonic};

use crate::IndexingService;

#[allow(missing_docs)]
#[derive(Clone)]
pub struct GrpcIndexingAdapter(Mailbox<IndexingService>);

impl From<Mailbox<IndexingService>> for GrpcIndexingAdapter {
    fn from(indexing_service: Mailbox<IndexingService>) -> Self {
        Self(indexing_service)
    }
}

#[async_trait]
impl grpc::IndexingService for GrpcIndexingAdapter {
    async fn apply_indexing_plan(
        &self,
        request: tonic::Request<ApplyIndexingPlanRequest>,
    ) -> Result<tonic::Response<ApplyIndexingPlanResponse>, tonic::Status> {
        let plan_request = request.into_inner();
        let plan_reply = self.0.ask(plan_request).await;
        convert_to_grpc_result(plan_reply)
    }
}
