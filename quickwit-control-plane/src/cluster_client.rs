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

use std::{sync::{Arc, RwLock}, collections::HashMap};

use chitchat::NodeId;
use quickwit_cluster::Cluster;
use quickwit_indexing::indexing_service_client::IndexingServiceClient;

use crate::index_service_client::IndexServiceClient;

/// Cluster Client.
/// It is aware of the cluster state and of its changes, it keeps
/// up to date: 
/// - a `IndexServiceClient` with the gRPC address of the Control Plane.
/// - the list of indexers and their corresponding gRPC IndexingClient.
/// Note: the client can also track searchers. Should we track every clients here?
pub struct ClusterClient {
    cluster: Arc<Cluster>,
    index_service_client: Arc<RwLock<Option<IndexServiceClient>>>,
    indexers: Arc<RwLock<HashMap<NodeId, IndexingServiceClient>>>,
}

impl ClusterClient {
    /// Create a cluster client given a cluster.
    /// It starts a thread to monitor cluster members changes and update the control plane and
    /// indexers clients.
    pub async fn create_and_keep_updated(_cluster: Arc<Cluster>) -> anyhow::Result<Self> {
        todo!()
    }

    /// Gets indexers NodeIds.
    /// Method to be used by the `IndexingScheduler`. 
    pub async fn get_indexers_node_ids() -> Vec<NodeId> {
        todo!()
    }

    /// Gets IndexServiceClient.
    /// Method to be used by the `Indexer`. 
    pub async fn get_index_service_client() -> Option<IndexServiceClient> {
        todo!()
    }

    /// Gets IndexingServiceClient.
    /// Method to be used by the `IndexingScheduler` to then call for applying
    /// a phsyical indexing plan.
    pub async fn get_indexing_service_client(_node_id: String) -> Option<IndexingServiceClient> {
        todo!()
    }
}