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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox};
use quickwit_ingest_api::IngestApiService;
use quickwit_metastore::Metastore;
use quickwit_proto::ingest_api::{DropQueueRequest, ListQueuesRequest};
use tracing::info;

use super::IndexingService;
use crate::actors::indexing_service::INGEST_API_SOURCE_ID;
use crate::models::ShutdownPipeline;

const RUN_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hour

#[derive(Debug, Clone, Default)]
pub struct IngestApiGarbageCollectorCounters {
    /// The number of passes the garbage collector has performed.
    pub num_passes: usize,
    /// The number of deleted queues.
    pub num_deleted_queues: usize,
}

#[derive(Debug)]
struct Loop;

/// An actor for deleting not needed ingest api queues.
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

    async fn delete_queue(&self, queue_id: &str) -> Result<(), ActorExitStatus> {
        // shutdown the pipeline if any
        self.indexing_service
            .ask_for_res(ShutdownPipeline {
                index_id: queue_id.to_string(),
                source_id: INGEST_API_SOURCE_ID.to_string(),
            })
            .await
            .map_err(|error| anyhow::anyhow!(error))?;

        // delete the queue
        self.ingest_api_service
            .ask_for_res(DropQueueRequest {
                queue_id: queue_id.to_string(),
            })
            .await
            .map_err(|error| anyhow::anyhow!(error))?;

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

        let queues: HashSet<_> = self
            .ingest_api_service
            .ask_for_res(ListQueuesRequest {})
            .await
            .map_err(|error| anyhow::anyhow!(error))?
            .queues
            .into_iter()
            .collect();

        let indexes: HashSet<_> = self
            .metastore
            .list_indexes_metadatas()
            .await
            .map_err(|error| anyhow::anyhow!(error))?
            .into_iter()
            .map(|index_metadata| index_metadata.index_id)
            .collect();

        for queue_id in queues.difference(&indexes) {
            self.delete_queue(queue_id).await?;
            self.counters.num_deleted_queues += 1;
        }

        ctx.schedule_self_msg(RUN_INTERVAL, Loop).await;
        Ok(())
    }
}
