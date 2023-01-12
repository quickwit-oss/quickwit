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
use quickwit_proto::control_plane_api::control_plane_service_server::{self as grpc};
use quickwit_proto::control_plane_api::{NotifyIndexChangeRequest, NotifyIndexChangeResponse};
use quickwit_proto::tonic;

use crate::scheduler::IndexingScheduler;

#[allow(missing_docs)]
#[derive(Clone)]
pub struct GrpcControlPlaneAdapter(Mailbox<IndexingScheduler>);

impl From<Mailbox<IndexingScheduler>> for GrpcControlPlaneAdapter {
    fn from(indexing_scheduler: Mailbox<IndexingScheduler>) -> Self {
        Self(indexing_scheduler)
    }
}

#[async_trait]
impl grpc::ControlPlaneService for GrpcControlPlaneAdapter {
    async fn notify_index_change(
        &self,
        request: tonic::Request<NotifyIndexChangeRequest>,
    ) -> Result<tonic::Response<NotifyIndexChangeResponse>, tonic::Status> {
        let index_event_request = request.into_inner();
        let create_index_reply = self
            .0
            .ask(index_event_request)
            .await
            .map(|_| NotifyIndexChangeResponse {})
            .map_err(|send_error| {
                quickwit_proto::tonic::Status::new(tonic::Code::Internal, send_error.to_string())
            })?;
        Ok(tonic::Response::new(create_index_reply))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;

    use quickwit_actors::{Mailbox, Universe};
    use quickwit_cluster::ClusterMember;
    use quickwit_config::service::QuickwitService;
    use quickwit_grpc_clients::ControlPlaneGrpcClient;
    use quickwit_proto::control_plane_api::control_plane_service_server::ControlPlaneServiceServer;
    use quickwit_proto::control_plane_api::NotifyIndexChangeRequest;
    use quickwit_proto::tonic::transport::Server;
    use tokio::sync::watch;
    use tokio_stream::wrappers::WatchStream;

    use super::GrpcControlPlaneAdapter;
    use crate::scheduler::IndexingScheduler;

    async fn start_grpc_server(
        address: SocketAddr,
        indexing_scheduler: Mailbox<IndexingScheduler>,
    ) -> anyhow::Result<()> {
        let grpc_adapter = GrpcControlPlaneAdapter::from(indexing_scheduler);
        tokio::spawn(async move {
            Server::builder()
                .add_service(ControlPlaneServiceServer::new(grpc_adapter))
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_control_plane_grpc_client() -> anyhow::Result<()> {
        let universe = Universe::new();
        let (indexing_planner_mailbox, indexing_planner_inbox) = universe.create_test_mailbox();
        let control_plane_grpc_addr_port = quickwit_common::net::find_available_tcp_port().unwrap();
        let control_plane_grpc_addr: SocketAddr =
            ([127, 0, 0, 1], control_plane_grpc_addr_port).into();
        start_grpc_server(control_plane_grpc_addr, indexing_planner_mailbox).await?;
        let control_plane_service_member = ClusterMember::new(
            "1".to_string(),
            0,
            HashSet::from([QuickwitService::ControlPlane]),
            control_plane_grpc_addr,
            control_plane_grpc_addr,
            None,
        );
        let (_members_tx, members_rx) =
            watch::channel::<Vec<ClusterMember>>(vec![control_plane_service_member.clone()]);
        let watch_members = WatchStream::new(members_rx);
        let mut control_plane_client =
            ControlPlaneGrpcClient::create_and_update_from_members(watch_members)
                .await
                .unwrap();

        let result = control_plane_client.notify_index_change().await;
        assert!(result.is_ok());
        assert_eq!(
            indexing_planner_inbox
                .drain_for_test_typed::<NotifyIndexChangeRequest>()
                .len(),
            1
        );

        Ok(())
    }
}
