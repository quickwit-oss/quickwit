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

use async_trait::async_trait;
use quickwit_proto::{
    ClusterStateRequest, ClusterStateResponse, LeaveClusterRequest, LeaveClusterResponse,
    ListMembersRequest, ListMembersResponse, Member as PMember,
};

use crate::cluster::{Cluster, Member};
use crate::error::ClusterError;

/// Convert the member state to the protobuf one.
impl From<Member> for PMember {
    fn from(member: Member) -> Self {
        PMember {
            id: member.internal_id(),
            listen_address: member.gossip_public_address.to_string(),
            is_self: member.is_self,
            generation: member.generation,
        }
    }
}

#[async_trait]
pub trait ClusterService: Send + Sync + 'static {
    async fn list_members(
        &self,
        request: ListMembersRequest,
    ) -> Result<ListMembersResponse, ClusterError>;
    async fn leave_cluster(
        &self,
        request: LeaveClusterRequest,
    ) -> Result<LeaveClusterResponse, ClusterError>;
    async fn cluster_state(
        &self,
        request: ClusterStateRequest,
    ) -> Result<ClusterStateResponse, ClusterError>;
}

#[async_trait]
impl ClusterService for Cluster {
    /// This is the API to get the list of cluster members.
    async fn list_members(
        &self,
        _request: ListMembersRequest,
    ) -> Result<ListMembersResponse, ClusterError> {
        let cluster_id = self.cluster_id.clone();
        let members = self.members().into_iter().map(PMember::from).collect();
        Ok(ListMembersResponse {
            cluster_id,
            members,
        })
    }

    /// This is the API to leave the member from the cluster.
    async fn leave_cluster(
        &self,
        _request: LeaveClusterRequest,
    ) -> Result<LeaveClusterResponse, ClusterError> {
        self.leave().await;
        Ok(LeaveClusterResponse {})
    }

    /// This is the API to get the cluster state.
    async fn cluster_state(
        &self,
        _request: ClusterStateRequest,
    ) -> Result<ClusterStateResponse, ClusterError> {
        let cluster_state = self.state().await;
        let state_serialized_json = serde_json::to_string(&cluster_state).map_err(|err| {
            ClusterError::ClusterStateError {
                message: err.to_string(),
            }
        })?;
        Ok(ClusterStateResponse {
            state_serialized_json,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::ToSocketAddrs;

    use quickwit_proto::Member as PMember;
    use uuid::Uuid;

    use crate::cluster::Member;

    #[tokio::test]
    async fn test_cluster_convert_proto_member() {
        let host_id = Uuid::new_v4().to_string();
        let listen_addr = "localhost:12345".to_socket_addrs().unwrap().next().unwrap();
        let is_self = true;

        let member = Member {
            node_unique_id: host_id.clone(),
            gossip_public_address: listen_addr,
            generation: 1,
            is_self,
        };
        println!("member={:?}", member);

        let proto_member = PMember::from(member);
        println!("proto_member={:?}", proto_member);

        let expected = PMember {
            id: format!("{}/{}", host_id, 1),
            listen_address: listen_addr.to_string(),
            generation: 1,
            is_self,
        };
        println!("expected={:?}", expected);

        assert_eq!(proto_member, expected);
    }
}
