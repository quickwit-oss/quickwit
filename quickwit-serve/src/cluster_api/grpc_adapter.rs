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
use quickwit_cluster::service::{ClusterService, ClusterServiceImpl};
use quickwit_proto::{cluster_service_server as grpc, tonic};

#[derive(Clone)]
pub struct GrpcClusterAdapter(Arc<dyn ClusterService>);

impl From<Arc<ClusterServiceImpl>> for GrpcClusterAdapter {
    fn from(cluster_service_arc: Arc<ClusterServiceImpl>) -> Self {
        GrpcClusterAdapter(cluster_service_arc)
    }
}

#[async_trait]
impl grpc::ClusterService for GrpcClusterAdapter {
    async fn list_members(
        &self,
        request: tonic::Request<quickwit_proto::ListMembersRequest>,
    ) -> Result<tonic::Response<quickwit_proto::ListMembersResponse>, tonic::Status> {
        let list_members_req = request.into_inner();
        let list_members_resp = self
            .0
            .list_members(list_members_req)
            .await
            .map_err(Into::<tonic::Status>::into)?;
        Ok(tonic::Response::new(list_members_resp))
    }

    async fn leave_cluster(
        &self,
        request: tonic::Request<quickwit_proto::LeaveClusterRequest>,
    ) -> Result<tonic::Response<quickwit_proto::LeaveClusterResponse>, tonic::Status> {
        let leave_cluster_req = request.into_inner();
        let leave_cluster_resp = self
            .0
            .leave_cluster(leave_cluster_req)
            .await
            .map_err(Into::<tonic::Status>::into)?;
        Ok(tonic::Response::new(leave_cluster_resp))
    }

    async fn cluster_state(
        &self,
        request: tonic::Request<quickwit_proto::ClusterStateRequest>,
    ) -> Result<tonic::Response<quickwit_proto::ClusterStateResponse>, tonic::Status> {
        let cluster_state_req = request.into_inner();
        let cluster_state_resp = self
            .0
            .cluster_state(cluster_state_req)
            .await
            .map_err(Into::<tonic::Status>::into)?;
        Ok(tonic::Response::new(cluster_state_resp))
    }
}
