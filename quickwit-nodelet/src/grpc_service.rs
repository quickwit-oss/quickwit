// Copyright (C) 2021 Quickwit, Inc.
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
use quickwit_proto::{nodelet_service_server as grpc, StopNodeletRequest, StopNodeletResponse};
use tokio::sync::RwLock;
use tonic::Response;

use crate::Nodelet;

#[async_trait]
impl grpc::NodeletService for Nodelet {
    async fn stop_nodelet(
        &self,
        request: tonic::Request<StopNodeletRequest>,
    ) -> Result<tonic::Response<quickwit_proto::StopNodeletResponse>, tonic::Status> {
        let stop_nodelet_req = request.into_inner();
        let mut nodelet_guard = self.inner.write().await;
        let stop_nodelet_res = nodelet_guard.stop_nodelet(stop_nodelet_req).await.unwrap();
        Ok(tonic::Response::new(stop_nodelet_res))
    }
}
