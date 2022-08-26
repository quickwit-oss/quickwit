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

mod cluster;
mod error;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::bail;
use chitchat::transport::UdpTransport;
use chitchat::FailureDetectorConfig;
use quickwit_config::QuickwitConfig;

pub use crate::cluster::{
    create_cluster_for_test, grpc_addr_from_listen_addr_for_test, Cluster, ClusterMember,
    ClusterState,
};
pub use crate::error::{ClusterError, ClusterResult};

fn unix_timestamp() -> u64 {
    let duration_since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH!");
    duration_since_epoch.as_secs()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum QuickwitService {
    Indexer,
    Searcher,
    Janitor,
}

impl QuickwitService {
    pub fn as_str(&self) -> &'static str {
        match self {
            QuickwitService::Indexer => "indexer",
            QuickwitService::Searcher => "searcher",
            QuickwitService::Janitor => "janitor",
        }
    }
}

impl TryFrom<&str> for QuickwitService {
    type Error = anyhow::Error;

    fn try_from(service_str: &str) -> Result<Self, Self::Error> {
        match service_str {
            "indexer" => Ok(QuickwitService::Indexer),
            "searcher" => Ok(QuickwitService::Searcher),
            "janitor" => Ok(QuickwitService::Janitor),
            _ => {
                bail!("Service `{service_str}` unknown");
            }
        }
    }
}

pub async fn start_cluster_service(
    quickwit_config: &QuickwitConfig,
    services: &HashSet<QuickwitService>,
) -> anyhow::Result<Arc<Cluster>> {
    let member = ClusterMember::new(
        quickwit_config.node_id.clone(),
        unix_timestamp(),
        quickwit_config.gossip_advertise_addr,
        services.clone(),
        quickwit_config.grpc_listen_addr,
    );

    let cluster = Cluster::join(
        member,
        quickwit_config.gossip_listen_addr,
        quickwit_config.cluster_id.clone(),
        quickwit_config.peer_seed_addrs().await?,
        FailureDetectorConfig::default(),
        &UdpTransport,
    )
    .await?;

    Ok(Arc::new(cluster))
}
