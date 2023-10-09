// Copyright (C) 2023 Quickwit, Inc.
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

pub mod control_plane;
pub mod indexing_plan;
pub mod ingest;
pub mod scheduler;

use async_trait::async_trait;
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::tower::Pool;
use quickwit_proto::control_plane::{ControlPlaneService, ControlPlaneServiceClient};
use quickwit_proto::indexing::{IndexingServiceClient, IndexingTask};
use quickwit_proto::metastore::{CloseShardsRequest, DeleteShardsRequest};
use tracing::error;

/// Indexer-node specific information stored in the pool of available indexer nodes
#[derive(Debug, Clone)]
pub struct IndexerNodeInfo {
    pub client: IndexingServiceClient,
    pub indexing_tasks: Vec<IndexingTask>,
}

pub type IndexerPool = Pool<String, IndexerNodeInfo>;

/// Subscribes to various metastore events and forwards them to the control plane using the inner
/// client. The actual subscriptions are set up in `quickwit-serve`.
#[derive(Debug, Clone)]
pub struct ControlPlaneEventSubscriber(ControlPlaneServiceClient);

impl ControlPlaneEventSubscriber {
    pub fn new(control_plane: ControlPlaneServiceClient) -> Self {
        Self(control_plane)
    }
}

#[async_trait]
impl EventSubscriber<CloseShardsRequest> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, request: CloseShardsRequest) {
        if let Err(error) = self.0.close_shards(request).await {
            error!(
                "failed to notify control plane of close shards event: `{}`",
                error
            );
        }
    }
}

#[async_trait]
impl EventSubscriber<DeleteShardsRequest> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, request: DeleteShardsRequest) {
        if let Err(error) = self.0.delete_shards(request).await {
            error!(
                "failed to notify control plane of delete shards event: `{}`",
                error
            );
        }
    }
}

#[cfg(test)]
mod tests;
