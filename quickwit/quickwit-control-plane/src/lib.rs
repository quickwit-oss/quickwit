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
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, NotifyIndexChangeRequest,
};
use quickwit_proto::indexing::{IndexingServiceClient, IndexingTask};
use quickwit_proto::metastore::events::{
    AddSourceEvent, CreateIndexEvent, DeleteIndexEvent, DeleteSourceEvent, ToggleSourceEvent,
};
use quickwit_proto::metastore::SourceType;
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

    pub(crate) async fn notify_index_change(&mut self, event_name: &'static str) {
        if let Err(error) = self
            .0
            .notify_index_change(NotifyIndexChangeRequest {})
            .await
        {
            error!(error=?error, event=event_name, "Failed to notify control plane of index change.");
        }
    }
}

/// Notify the control plane when one of the following event occurs:
/// - an index is deleted.
/// - a source, other than the ingest CLI source, is created.
/// - a source is deleted.
/// Note: we don't need to send an event to the control plane on index creation.
/// A new index has no source and thus will not change the scheduling of indexing tasks.
// TODO(fmassot):
// - Forbid a `MetastoreWithControlPlaneTriggers` that wraps a gRPC client metastore.
// - We don't sent any data to the Control Plane. It could be nice to send the relevant data to the
//   control plane and let it decide to schedule or not indexing tasks.
#[async_trait]
impl EventSubscriber<CreateIndexEvent> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, _event: CreateIndexEvent) {
        self.notify_index_change("create-index").await;
    }
}

#[async_trait]
impl EventSubscriber<DeleteIndexEvent> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, _event: DeleteIndexEvent) {
        self.notify_index_change("delete-index").await;
    }
}

#[async_trait]
impl EventSubscriber<AddSourceEvent> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, event: AddSourceEvent) {
        if !matches!(event.source_type, SourceType::Cli | SourceType::File) {
            self.notify_index_change("add-source").await;
        };
    }
}

#[async_trait]
impl EventSubscriber<ToggleSourceEvent> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, _event: ToggleSourceEvent) {
        self.notify_index_change("toggle-source").await;
    }
}

#[async_trait]
impl EventSubscriber<DeleteSourceEvent> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, _event: DeleteSourceEvent) {
        self.notify_index_change("delete-source").await;
    }
}

#[cfg(test)]
mod tests {

    use quickwit_proto::control_plane::NotifyIndexChangeResponse;
    use quickwit_proto::metastore::SourceType;
    use quickwit_proto::IndexUid;

    use super::*;

    #[tokio::test]
    async fn test_metastore_event_handler() {
        let mut mock = ControlPlaneServiceClient::mock();
        mock.expect_notify_index_change()
            .return_once(|_| Ok(NotifyIndexChangeResponse {}));

        let mut control_plane_event_subscriber =
            ControlPlaneEventSubscriber(ControlPlaneServiceClient::new(mock));

        let index_uid = IndexUid::new("test-index");

        let event = AddSourceEvent {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            source_type: SourceType::Cli,
        };
        control_plane_event_subscriber.handle_event(event).await;

        let event = AddSourceEvent {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            source_type: SourceType::File,
        };
        control_plane_event_subscriber.handle_event(event).await;

        let event = AddSourceEvent {
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            source_type: SourceType::IngestV2,
        };
        control_plane_event_subscriber.handle_event(event).await;
    }
}
