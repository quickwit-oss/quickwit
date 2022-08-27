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

use tokio_stream::wrappers::WatchStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tower::timeout::Timeout;

use crate::control_plane_api::control_plane_service_client::ControlPlaneServiceClient;
use crate::control_plane_api::RefreshIndexingPlanEventRequest;
use crate::create_balance_channel_and_udpate_from_members;
use crate::quickwit_models::{ClusterMember, QuickwitService};

/// The [`ControlPlaneGrpcClient`] sends gRPC requests to cluster members running a `ControlPlane`
/// service.
/// The [`ControlPlaneGrpcClient`] use tonic load balancer to balance requests between nodes and
/// listen to cluster live nodes changes to keep updated the list of available nodes.
// TODO: this balance channel can accept several control planes but currently
// our architecture does not support that. We need a balance
// channel with only one endpoint.
#[derive(Clone)]
pub struct ControlPlaneGrpcClient(ControlPlaneServiceClient<Timeout<Channel>>);

impl ControlPlaneGrpcClient {
    /// Create a [`ControlPlaneGrpcClient`] that sends gRPC requests to nodes running
    /// `ControlPlane` service. It listens to cluster members changes to update the
    /// nodes.
    pub async fn create_and_update_from_members(
        members_watch_channel: WatchStream<Vec<ClusterMember>>,
    ) -> anyhow::Result<Self> {
        let channel = create_balance_channel_and_udpate_from_members(
            members_watch_channel,
            QuickwitService::ControlPlane,
        )
        .await?;
        Ok(Self(ControlPlaneServiceClient::new(channel)))
    }

    /// Creates a [`MetastoreService`] from a duplex stream client for testing purpose.
    #[doc(hidden)]
    pub async fn from_duplex_stream(client: tokio::io::DuplexStream) -> anyhow::Result<Self> {
        let mut client = Some(client);
        let channel = Endpoint::try_from("http://test.server")?
            .connect_with_connector(service_fn(move |_: Uri| {
                let client = client.take();
                async move {
                    client.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "Client already taken")
                    })
                }
            }))
            .await?;
        let client =
            ControlPlaneServiceClient::new(Timeout::new(channel, Duration::from_millis(100)));
        Ok(Self(client))
    }

    pub async fn send_index_event(&mut self) -> anyhow::Result<()> {
        let _ = self
            .0
            .send_refresh_indexing_plan_event(RefreshIndexingPlanEventRequest {})
            .await?;
        Ok(())
    }
}
