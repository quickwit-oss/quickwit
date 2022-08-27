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

use std::collections::HashSet;
use std::net::SocketAddr;

use std::fmt::Display;
use std::str::FromStr;

use anyhow::bail;
use enum_iterator::{all, Sequence};
use itertools::Itertools;
use serde::Serialize;

use crate::quickwit_indexing_api::IndexingTask;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Sequence)]
pub enum QuickwitService {
    Indexer,
    Searcher,
    Janitor,
    Metastore,
    ControlPlane,
}

impl QuickwitService {
    pub fn as_str(&self) -> &'static str {
        match self {
            QuickwitService::Indexer => "indexer",
            QuickwitService::Searcher => "searcher",
            QuickwitService::Janitor => "janitor",
            QuickwitService::Metastore => "metastore",
            QuickwitService::ControlPlane => "control_plane",
        }
    }
    pub fn supported_services() -> Vec<QuickwitService> {
        all::<QuickwitService>().into_iter().collect()
    }
}

impl Display for QuickwitService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for QuickwitService {
    type Err = anyhow::Error;

    fn from_str(service_str: &str) -> Result<Self, Self::Err> {
        match service_str {
            "indexer" => Ok(QuickwitService::Indexer),
            "searcher" => Ok(QuickwitService::Searcher),
            "janitor" => Ok(QuickwitService::Janitor),
            "metastore" => Ok(QuickwitService::Metastore),
            "control_plane" => Ok(QuickwitService::ControlPlane),
            _ => {
                bail!(
                    "Failed to parse service `{service_str}`. Supported services are: {}",
                    QuickwitService::supported_services().iter().join(", ")
                )
            }
        }
    }
}


/// Cluster member.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterMember {
    /// An ID that makes a member unique. Chitchat node ID is built from
    /// the concatenation `{node_unique_id}/{generation}`.
    pub node_unique_id: String,
    /// Timestamp (ms) when node starts.
    pub generation: u64,
    /// Gossip advertise address.
    pub gossip_advertise_addr: SocketAddr,
    /// Available services.
    pub available_services: HashSet<QuickwitService>,
    /// gRPC advertise address.
    pub grpc_advertise_addr: SocketAddr,
    /// Running indexing tasks.
    /// Empty if the ndoe is not an indexer.
    pub running_indexing_tasks: Vec<IndexingTask>,
}

impl ClusterMember {
    pub fn new(
        node_unique_id: String,
        generation: u64,
        gossip_advertise_addr: SocketAddr,
        available_services: HashSet<QuickwitService>,
        grpc_advertise_addr: SocketAddr,
    ) -> Self {
        Self {
            node_unique_id,
            generation,
            gossip_advertise_addr,
            available_services,
            grpc_advertise_addr,
            running_indexing_tasks: Vec::new(),
        }
    }

    pub fn chitchat_id(&self) -> String {
        format!("{}/{}", self.node_unique_id, self.generation)
    }
}