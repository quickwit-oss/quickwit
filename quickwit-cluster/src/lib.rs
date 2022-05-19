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

mod cluster;
mod error;
mod service;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::bail;
use chitchat::transport::UdpTransport;
use chitchat::FailureDetectorConfig;
use quickwit_config::QuickwitConfig;

pub use crate::cluster::{
    create_cluster_for_test, grpc_addr_from_listen_addr_for_test, Cluster, Member,
};
pub use crate::error::{ClusterError, ClusterResult};
pub use crate::service::ClusterService;

fn unix_timestamp() -> u64 {
    let duration_since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH!");
    duration_since_epoch.as_secs()
}

#[derive(Debug, PartialEq, Eq, Copy, Hash, Clone)]
pub enum QuickwitService {
    Indexer,
    Searcher,
}

impl QuickwitService {
    pub fn as_str(&self) -> &'static str {
        match self {
            QuickwitService::Indexer => "indexer",
            QuickwitService::Searcher => "searcher",
        }
    }
}

impl TryFrom<&str> for QuickwitService {
    type Error = anyhow::Error;

    fn try_from(service_str: &str) -> Result<Self, Self::Error> {
        match service_str {
            "indexer" => Ok(QuickwitService::Indexer),
            "searcher" => Ok(QuickwitService::Searcher),
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
    let seed_nodes = quickwit_config
        .seed_socket_addrs()?
        .iter()
        .map(|addr| addr.to_string())
        .collect::<Vec<_>>();

    let member = Member::new(
        quickwit_config.node_id.clone(),
        unix_timestamp(),
        quickwit_config.gossip_public_addr()?,
    );

    let cluster = Cluster::join(
        member,
        services,
        quickwit_config.gossip_socket_addr()?,
        quickwit_config.cluster_id.clone(),
        quickwit_config.grpc_socket_addr()?,
        seed_nodes,
        FailureDetectorConfig::default(),
        &UdpTransport,
    )
    .await?;

    Ok(Arc::new(cluster))
}
