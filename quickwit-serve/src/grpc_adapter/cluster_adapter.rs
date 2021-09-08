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
use quickwit_cluster::error::ClusterError;
use quickwit_cluster::service::{ClusterService, ClusterServiceImpl};
use quickwit_proto::cluster_service_server as grpc;

#[derive(Clone)]
pub struct GrpcClusterAdapter(Arc<dyn ClusterService>);

impl From<Arc<ClusterServiceImpl>> for GrpcClusterAdapter {
    fn from(cluster_service_arc: Arc<ClusterServiceImpl>) -> Self {
        GrpcClusterAdapter(cluster_service_arc)
    }
}

#[async_trait]
impl grpc::ClusterService for GrpcClusterAdapter {
    async fn members(
        &self,
        request: tonic::Request<quickwit_proto::MembersRequest>,
    ) -> Result<tonic::Response<quickwit_proto::MembersResult>, tonic::Status> {
        let members_request = request.into_inner();
        let members_result = self
            .0
            .members(members_request)
            .await
            .map_err(ClusterError::convert_to_tonic_status)?;
        Ok(tonic::Response::new(members_result))
    }

    async fn leave(
        &self,
        request: tonic::Request<quickwit_proto::LeaveRequest>,
    ) -> Result<tonic::Response<quickwit_proto::LeaveResult>, tonic::Status> {
        let leave_request = request.into_inner();
        let leave_result = self
            .0
            .leave(leave_request)
            .await
            .map_err(ClusterError::convert_to_tonic_status)?;
        Ok(tonic::Response::new(leave_result))
    }
}
