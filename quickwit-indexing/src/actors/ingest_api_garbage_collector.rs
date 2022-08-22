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
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_ingest_api::IngestApiService;
use quickwit_metastore::Metastore;
use quickwit_proto::ingest_api::{DropQueueRequest, ListQueuesRequest};
use tracing::{debug, error, info, instrument};

use super::IndexingService;
use crate::actors::indexing_service::INGEST_API_SOURCE_ID;
use crate::models::ShutdownPipelines;

const RUN_INTERVAL: Duration = if cfg!(test) {
    Duration::from_secs(60) // 1min
} else {
    Duration::from_secs(60 * 60) // 1h
};

#[derive(Clone, Debug, Default)]
pub struct IngestApiGarbageCollectorCounters {
    /// The number of passes the garbage collector has performed.
    pub num_passes: usize,
    /// The number of deleted queues.
    pub num_deleted_queues: usize,
}

#[derive(Debug)]
struct Loop;

/// An actor for deleting not needed ingest api queues.
///
/// This actor has been introduced for Quickwit 0.3, in which indexes are
/// deleted by the quickwit CLI without any communication with the (unique) indexing node.
///
/// This actor is meant to be removed in the future.
pub struct IngestApiGarbageCollector {
    metastore: Arc<dyn Metastore>,
    ingest_api_service: Mailbox<IngestApiService>,
    indexing_service: Mailbox<IndexingService>,
    counters: IngestApiGarbageCollectorCounters,
}

impl IngestApiGarbageCollector {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        ingest_api_service: Mailbox<IngestApiService>,
        indexing_service: Mailbox<IndexingService>,
    ) -> Self {
        Self {
            metastore,
            ingest_api_service,
            indexing_service,
            counters: IngestApiGarbageCollectorCounters::default(),
        }
    }

    async fn delete_queue(&self, queue_id: &str) -> anyhow::Result<()> {
        // shutdown the pipeline if any
        self.indexing_service
            .ask_for_res(ShutdownPipelines {
                index_id: queue_id.to_string(),
                source_id: Some(INGEST_API_SOURCE_ID.to_string()),
            })
            .await?;

        // delete the queue
        self.ingest_api_service
            .ask_for_res(DropQueueRequest {
                queue_id: queue_id.to_string(),
            })
            .await?;

        Ok(())
    }

    #[instrument(skip_all, "ingest-queues-gc")]
    async fn run_ingest_queues_gc(&mut self) -> anyhow::Result<()> {
        let queues: HashSet<String> = self
            .ingest_api_service
            .ask_for_res(ListQueuesRequest {})
            .await
            .context("Failed to list queues")?
            .queues
            .into_iter()
            .collect();
        debug!(queues=?queues, "list-queues");

        let index_ids: HashSet<String> = self
            .metastore
            .list_indexes_metadatas()
            .await
            .context("Failed to list queues")?
            .into_iter()
            .map(|index_metadata| index_metadata.index_id)
            .collect();
        debug!(index_ids=?index_ids, "list-index-ids");

        let queue_ids_to_delete = queues.difference(&index_ids);
        for queue_id in queue_ids_to_delete {
            if let Err(delete_queue_error) = self.delete_queue(queue_id).await {
                error!(error=?delete_queue_error, queue_id=%queue_id, "queue-delete-failure");
            } else {
                info!(queue_id=%queue_id, "queue-delete-success");
                self.counters.num_deleted_queues += 1;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Actor for IngestApiGarbageCollector {
    type ObservableState = IngestApiGarbageCollectorCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        "IngestApiGarbageCollector".to_string()
    }

    async fn initialize(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), quickwit_actors::ActorExitStatus> {
        self.handle(Loop, ctx).await
    }
}

#[async_trait]
impl Handler<Loop> for IngestApiGarbageCollector {
    type Reply = ();

    async fn handle(&mut self, _: Loop, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        info!("ingest-api-garbage-collect-operation");
        self.counters.num_passes += 1;

        if let Err(gc_err) = self.run_ingest_queues_gc().await {
            // We do not stop the actor here.
            // It will retry in one hour.
            error!(error=?gc_err, "ingest-queue-gc-failed");
        }

        ctx.schedule_self_msg(RUN_INTERVAL, Loop).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_common::uri::Uri;
    use quickwit_config::IndexerConfig;
    use quickwit_ingest_api::spawn_ingest_api_actor;
    use quickwit_metastore::{quickwit_metastore_uri_resolver, IndexMetadata};
    use quickwit_proto::ingest_api::CreateQueueIfNotExistsRequest;
    use quickwit_storage::StorageUriResolver;

    use super::*;

    #[tokio::test]
    async fn test_ingest_api_garbage_collector() -> anyhow::Result<()> {
        let index_id = "test-index".to_string();
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_metadata = IndexMetadata::for_test(&index_id, &index_uri);

        let metastore_uri = Uri::new("ram:///metastore".to_string());
        let metastore = quickwit_metastore_uri_resolver()
            .resolve(&metastore_uri)
            .await
            .unwrap();
        metastore.create_index(index_metadata).await.unwrap();

        // Setup ingest api objects
        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let ingest_api_mailbox =
            spawn_ingest_api_actor(&universe, temp_dir.path().join("queues").as_path())?;
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: index_id.clone(),
        };
        ingest_api_mailbox
            .ask_for_res(create_queue_req)
            .await
            .map_err(|error| anyhow::anyhow!(error))?;

        // Setup `IndexingService`
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let storage_resolver = StorageUriResolver::for_test();
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path,
            indexer_config,
            metastore.clone(),
            storage_resolver.clone(),
            Some(ingest_api_mailbox.clone()),
        );
        let (indexing_server_mailbox, _indexing_server_handle) =
            universe.spawn_actor(indexing_server).spawn();

        let ingest_api_garbage_collector = IngestApiGarbageCollector::new(
            metastore.clone(),
            ingest_api_mailbox,
            indexing_server_mailbox,
        );
        let (_maibox, handler) = universe.spawn_actor(ingest_api_garbage_collector).spawn();

        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);
        assert_eq!(state_after_initialization.num_deleted_queues, 0);

        // 30 seconds later
        universe.simulate_time_shift(Duration::from_secs(30)).await;
        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 1);
        assert_eq!(state_after_initialization.num_deleted_queues, 0);

        metastore.delete_index(&index_id).await.unwrap();

        // 1m later
        universe.simulate_time_shift(RUN_INTERVAL).await;
        let state_after_initialization = handler.process_pending_and_observe().await.state;
        assert_eq!(state_after_initialization.num_passes, 2);
        assert_eq!(state_after_initialization.num_deleted_queues, 1);

        Ok(())
    }
}
