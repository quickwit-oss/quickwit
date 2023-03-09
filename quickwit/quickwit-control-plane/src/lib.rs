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

#[path = "codegen/control_plane_service.rs"]
mod control_plane_service;
pub mod indexing_plan;
pub mod scheduler;

use std::sync::Arc;

use async_trait::async_trait;
pub use control_plane_service::*;
use quickwit_actors::{AskError, Mailbox, Universe};
use quickwit_cluster::Cluster;
use quickwit_common::pubsub::EventSubscriber;
use quickwit_config::SourceParams;
use quickwit_grpc_clients::service_client_pool::ServiceClientPool;
use quickwit_metastore::{Metastore, MetastoreEvent};
use scheduler::IndexingScheduler;
use tracing::error;

pub type Result<T> = std::result::Result<T, ControlPlaneError>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("An internal error occurred: {0}.")]
    Internal(String),
    #[error("Control plane is unavailable: {0}.")]
    Unavailable(String),
}

impl From<ControlPlaneError> for tonic::Status {
    fn from(error: ControlPlaneError) -> Self {
        match error {
            ControlPlaneError::Internal(message) => tonic::Status::internal(message),
            ControlPlaneError::Unavailable(message) => tonic::Status::unavailable(message),
        }
    }
}

impl From<tonic::Status> for ControlPlaneError {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unavailable => {
                ControlPlaneError::Unavailable(status.message().to_string())
            }
            _ => ControlPlaneError::Internal(status.message().to_string()),
        }
    }
}

impl From<AskError<ControlPlaneError>> for ControlPlaneError {
    fn from(error: AskError<ControlPlaneError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => {
                ControlPlaneError::Unavailable("Request not delivered".to_string())
            }
            AskError::ProcessMessageError => ControlPlaneError::Internal(
                "An error occurred while processing the request".to_string(),
            ),
        }
    }
}

/// Starts the Control Plane.
pub async fn start_control_plane_service(
    universe: &Universe,
    cluster: Arc<Cluster>,
    metastore: Arc<dyn Metastore>,
) -> anyhow::Result<Mailbox<IndexingScheduler>> {
    let indexing_service_client_pool =
        ServiceClientPool::create_and_update_members(cluster.ready_member_change_watcher()).await?;
    let scheduler = IndexingScheduler::new(cluster, metastore, indexing_service_client_pool);
    let (scheduler_mailbox, _) = universe.spawn_builder().spawn(scheduler);
    Ok(scheduler_mailbox)
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
impl EventSubscriber<MetastoreEvent> for ControlPlaneServiceClient {
    async fn handle_event(&mut self, event: MetastoreEvent) {
        let event = match event {
            MetastoreEvent::DeleteIndex { .. } => "delete-index",
            MetastoreEvent::AddSource { source_config, .. } => {
                if matches!(
                    source_config.source_params,
                    SourceParams::File(_) | SourceParams::IngestCli
                ) {
                    return;
                }
                "add-source"
            }
            MetastoreEvent::ToggleSource { .. } => "toggle-source",
            MetastoreEvent::DeleteSource { .. } => "delete-source",
        };
        if let Err(error) = self.notify_index_change(NotifyIndexChangeRequest {}).await {
            error!(error=?error, event=event, "Failed to notify control plane of index change.");
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::SourceConfig;

    use super::*;

    #[tokio::test]
    async fn test_metastore_event_handler() {
        let mut mock = ControlPlaneServiceClient::mock();
        mock.expect_notify_index_change()
            .return_once(|_| Ok(NotifyIndexChangeResponse {}));

        let mut control_plane = ControlPlaneServiceClient::new(mock);

        let event = MetastoreEvent::AddSource {
            index_id: "test-index".to_string(),
            source_config: SourceConfig::for_test("test-source", SourceParams::IngestApi),
        };
        control_plane.handle_event(event).await;

        let event = MetastoreEvent::AddSource {
            index_id: "test-index".to_string(),
            source_config: SourceConfig::for_test("test-source", SourceParams::file("test-file")),
        };
        control_plane.handle_event(event).await;

        let event = MetastoreEvent::AddSource {
            index_id: "test-index".to_string(),
            source_config: SourceConfig::for_test("test-source", SourceParams::IngestCli),
        };
        control_plane.handle_event(event).await;
    }
}
