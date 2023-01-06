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

use std::time::Duration;

use quickwit_cluster::ClusterMember;
use quickwit_config::service::QuickwitService;
use quickwit_proto::control_plane_api::control_plane_service_client::ControlPlaneServiceClient;
use quickwit_proto::control_plane_api::NotifyIndexChangeRequest;
use tokio_stream::wrappers::WatchStream;
use tonic::transport::Channel;
use tower::timeout::Timeout;

use crate::balance_channel::create_balance_channel_from_watched_members;

/// The [`ControlPlaneGrpcClient`] sends gRPC requests to a cluster member running a `ControlPlane`
/// service. The client use a tonic load balancer to balance requests between nodes and
/// listen to cluster live nodes changes to keep updated the list of available nodes.
/// Currently Quickwit does not support multiple control plances but this is still useful during
/// transition periods:
/// - A control plane instance is killed
/// - Another one is started.
/// - The balancer will keep the two endpoints until chitchat detect the dead control plane.
///   Meanwhile, it will forward requests to the running control plane and it will detect the non
///   working control control plane and will stop send him the requests.
/// It is used by the concrete metastore to notify the control plane of index/source updates.
#[derive(Clone)]
pub struct ControlPlaneGrpcClient {
    underlying: ControlPlaneServiceClient<Timeout<Channel>>,
}

impl ControlPlaneGrpcClient {
    /// Creates a [`ControlPlaneGrpcClient`] from a channel.
    pub fn new(channel: Channel) -> Self {
        let timeout_channel = Timeout::new(channel, Duration::from_secs(5));
        Self {
            underlying: ControlPlaneServiceClient::new(timeout_channel),
        }
    }

    /// Creates a [`ControlPlaneGrpcClient`] that sends gRPC requests to nodes running
    /// `ControlPlane` service. It listens to cluster members changes to update the
    /// nodes.
    pub async fn create_and_update_from_members(
        members_watch_channel: WatchStream<Vec<ClusterMember>>,
    ) -> anyhow::Result<Self> {
        let (channel, _) = create_balance_channel_from_watched_members(
            members_watch_channel,
            QuickwitService::ControlPlane,
        )
        .await?;
        Ok(Self {
            underlying: ControlPlaneServiceClient::new(channel),
        })
    }

    /// Notifies a control plane that an index change happened.
    pub async fn notify_index_change(&mut self) -> anyhow::Result<()> {
        let _ = self
            .underlying
            .notify_index_change(NotifyIndexChangeRequest {})
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::time::Duration;

    use quickwit_cluster::ClusterMember;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::control_plane_api::control_plane_service_server::{
        self as grpc, ControlPlaneServiceServer,
    };
    use quickwit_proto::control_plane_api::{NotifyIndexChangeRequest, NotifyIndexChangeResponse};
    use tokio::sync::watch;
    use tokio_stream::wrappers::WatchStream;
    use tonic::async_trait;
    use tonic::transport::Server;

    use super::ControlPlaneGrpcClient;

    struct GrpcControlPlaneAdapterForTest;

    #[async_trait]
    impl grpc::ControlPlaneService for GrpcControlPlaneAdapterForTest {
        async fn notify_index_change(
            &self,
            _: tonic::Request<NotifyIndexChangeRequest>,
        ) -> Result<tonic::Response<NotifyIndexChangeResponse>, tonic::Status> {
            Ok(tonic::Response::new(NotifyIndexChangeResponse {}))
        }
    }

    async fn start_grpc_server(address: SocketAddr) -> anyhow::Result<()> {
        tokio::spawn(async move {
            Server::builder()
                .add_service(ControlPlaneServiceServer::new(
                    GrpcControlPlaneAdapterForTest,
                ))
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_grpc_balance_channel() {
        quickwit_common::setup_logging_for_tests();
        let grpc_port_1 = quickwit_common::net::find_available_tcp_port().unwrap();
        let grpc_addr_1: SocketAddr = ([127, 0, 0, 1], grpc_port_1).into();
        let grpc_addr_2: SocketAddr = ([127, 0, 0, 1], 1234).into();
        let grpc_addr_3: SocketAddr = ([127, 0, 0, 1], 1235).into();
        let grpc_addr_4: SocketAddr = ([127, 0, 0, 1], 1236).into();

        let member_1 = ClusterMember::new(
            "1".to_string(),
            0,
            HashSet::from([QuickwitService::ControlPlane]),
            grpc_addr_1,
            grpc_addr_1,
            None,
        );
        let member_2 = ClusterMember::new(
            "2".to_string(),
            0,
            HashSet::from([QuickwitService::ControlPlane]),
            grpc_addr_2,
            grpc_addr_2,
            None,
        );
        let member_3 = ClusterMember::new(
            "3".to_string(),
            0,
            HashSet::from([QuickwitService::ControlPlane]),
            grpc_addr_3,
            grpc_addr_3,
            None,
        );
        let member_4 = ClusterMember::new(
            "4".to_string(),
            0,
            HashSet::from([QuickwitService::Indexer]),
            grpc_addr_4,
            grpc_addr_4,
            None,
        );

        let (members_tx, members_rx) = watch::channel::<Vec<ClusterMember>>(vec![member_1.clone()]);
        let watch_members = WatchStream::new(members_rx);

        let mut control_plane_client =
            ControlPlaneGrpcClient::create_and_update_from_members(watch_members)
                .await
                .unwrap();

        // Only start member 1.
        start_grpc_server(grpc_addr_1).await.unwrap();

        assert!(control_plane_client.notify_index_change().await.is_ok());

        // Send 2 unavailable control plane and 1 unavailable indexer.
        members_tx
            .send(vec![member_2.clone(), member_3, member_4])
            .unwrap();

        let error = control_plane_client
            .notify_index_change()
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("tcp connect error: Connection refused"));

        // Send running control plane member and 1 dead control plane.
        members_tx.send(vec![member_1]).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(control_plane_client.notify_index_change().await.is_ok());
    }
}
