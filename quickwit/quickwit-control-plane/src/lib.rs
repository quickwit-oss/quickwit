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

pub mod cache_storage_controller;
pub mod control_plane;
pub mod indexing_plan;
pub mod scheduler;

use std::sync::Arc;

use async_trait::async_trait;
use cache_storage_controller::{CacheStorageController, CacheStorageServicePool};
use futures::Stream;
use quickwit_actors::{Mailbox, Universe};
use quickwit_cluster::ClusterChange;
use quickwit_common::pubsub::EventSubscriber;
use quickwit_common::tower::Pool;
use quickwit_config::SourceParams;
use quickwit_metastore::{Metastore, MetastoreEvent};
use quickwit_proto::cache_storage::CacheStorageServiceClient;
use quickwit_proto::control_plane::{
    ControlPlaneService, ControlPlaneServiceClient, NotifyIndexChangeRequest,
    NotifySplitsChangeRequest,
};
use quickwit_proto::indexing::{IndexingServiceClient, IndexingTask};
use scheduler::IndexingScheduler;
use tracing::error;

/// Indexer-node specific information stored in the pool of available indexer nodes
#[derive(Debug, Clone)]
pub struct IndexerNodeInfo {
    pub client: IndexingServiceClient,
    pub indexing_tasks: Vec<IndexingTask>,
}

pub type IndexerPool = Pool<String, IndexerNodeInfo>;
// pub type CacheStorageServicePool = Pool<String, CacheStorageServiceClient>;

/// Starts the Indexing Schedule as part of Control Panel.
pub async fn start_indexing_scheduler(
    cluster_id: String,
    self_node_id: String,
    universe: &Universe,
    indexer_pool: IndexerPool,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<Mailbox<IndexingScheduler>> {
    let scheduler = IndexingScheduler::new(cluster_id, self_node_id, metastore, indexer_pool);
    let (scheduler_mailbox, _) = universe.spawn_builder().spawn(scheduler);
    Ok(scheduler_mailbox)
}

/// Starts the Cache Storage Controller as part of Control Panel.
pub async fn start_cache_storage_controller(
    universe: &Universe,
    cluster_change_stream: impl Stream<Item = ClusterChange> + Send + 'static,
    local_cache_storage_service_client: Option<CacheStorageServiceClient>,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<Mailbox<CacheStorageController>> {
    let service_pool = CacheStorageServicePool::default();
    let controller = CacheStorageController::new(metastore, service_pool.clone());
    let (controller_mailbox, _) = universe.spawn_builder().spawn(controller);
    service_pool.listen_for_changes(
        local_cache_storage_service_client,
        controller_mailbox.clone(),
        cluster_change_stream,
    );
    Ok(controller_mailbox)
}

#[derive(Debug, Clone)]
pub struct ControlPlaneEventSubscriber(pub ControlPlaneServiceClient);

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
impl EventSubscriber<MetastoreEvent> for ControlPlaneEventSubscriber {
    async fn handle_event(&mut self, event: MetastoreEvent) {
        if let Some(event) = match &event {
            MetastoreEvent::DeleteIndex { .. } => Some("delete-index"),
            MetastoreEvent::AddSource { source_config, .. } => {
                if matches!(
                    source_config.source_params,
                    SourceParams::File(_) | SourceParams::IngestCli
                ) {
                    return;
                }
                Some("add-source")
            }
            MetastoreEvent::ToggleSource { .. } => Some("toggle-source"),
            MetastoreEvent::DeleteSource { .. } => Some("delete-source"),
            _ => None,
        } {
            if let Err(error) = self
                .0
                .notify_index_change(NotifyIndexChangeRequest {})
                .await
            {
                error!(error=?error, event=event, "Failed to notify control plane of index change.");
            }
        } else if let Some((split_event_description, index_uid)) = match &event {
            MetastoreEvent::PublishSplit { index_uid } => {
                Some(("publish-split", index_uid.to_string()))
            }
            MetastoreEvent::DeleteSplit { index_uid } => {
                Some(("delete-split", index_uid.to_string()))
            }
            _ => None,
        } {
            if let Err(error) = self
                .0
                .notify_split_change(NotifySplitsChangeRequest { index_uid })
                .await
            {
                error!(error=?error, event=split_event_description, "Failed to notify control plane of split change.");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::SourceConfig;
    use quickwit_proto::control_plane::NotifyIndexChangeResponse;
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

        let event = MetastoreEvent::AddSource {
            index_uid: index_uid.clone(),
            source_config: SourceConfig::for_test("test-source", SourceParams::IngestApi),
        };
        control_plane_event_subscriber.handle_event(event).await;

        let event = MetastoreEvent::AddSource {
            index_uid: index_uid.clone(),
            source_config: SourceConfig::for_test("test-source", SourceParams::file("test-file")),
        };
        control_plane_event_subscriber.handle_event(event).await;

        let event = MetastoreEvent::AddSource {
            index_uid: index_uid.clone(),
            source_config: SourceConfig::for_test("test-source", SourceParams::IngestCli),
        };
        control_plane_event_subscriber.handle_event(event).await;
    }
}
