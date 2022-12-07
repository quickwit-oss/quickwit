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

use std::path::Path;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use quickwit_proto::ingest_api::{
    CreateQueueIfNotExistsRequest, CreateQueueRequest, DropQueueRequest, FetchRequest,
    FetchResponse, IngestRequest, IngestResponse, ListQueuesRequest, ListQueuesResponse,
    QueueExistsRequest, SuggestTruncateRequest, TailRequest,
};
use tracing::info;
use ulid::Ulid;

use crate::metrics::INGEST_METRICS;
use crate::{iter_doc_payloads, IngestApiError, Queues};

pub struct IngestApiService {
    partition_id: String,
    queues: Queues,
}

/// When we create our queue storage, we also generate and store
/// a random partition id associated to it.
///
/// That partition_id is used in the source checkpoint.
///
/// The idea is to make sure that if the entire queue storage is lost,
/// the old source checkpoint (stored in the metastore) do not apply.
/// (See #2310)
const PARTITION_ID_PATH: &str = "partition_id";

async fn get_or_initialize_partition_id(dir_path: &Path) -> crate::Result<String> {
    let partition_id_path = dir_path.join(PARTITION_ID_PATH);
    if let Ok(partition_id_bytes) = tokio::fs::read(&partition_id_path).await {
        let partition_id: &str = std::str::from_utf8(&partition_id_bytes).map_err(|_| {
            let msg = format!("Partition key ({partition_id_bytes:?}) is not utf8");
            IngestApiError::Corruption { msg }
        })?;
        return Ok(partition_id.to_string());
    }
    // We add a prefix here to make sure we don't mistake it for a split id when reading logs.
    let partition_id = format!("ingest_partition_{}", Ulid::new());
    tokio::fs::write(partition_id_path, partition_id.as_bytes()).await?;
    Ok(partition_id)
}

impl IngestApiService {
    pub async fn with_queues_dir(queues_dir_path: &Path) -> crate::Result<Self> {
        let queues = Queues::open(queues_dir_path).await?;
        let partition_id = get_or_initialize_partition_id(queues_dir_path).await?;
        info!(ingest_partition_id=%partition_id, "Ingest API partition id");
        Ok(IngestApiService {
            partition_id,
            queues,
        })
    }

    async fn ingest(
        &mut self,
        request: IngestRequest,
        ctx: &ActorContext<Self>,
    ) -> crate::Result<IngestResponse> {
        // Check all indexes exist assuming existing queues always have a corresponding index.
        let first_non_existing_queue_opt = request
            .doc_batches
            .iter()
            .map(|batch| batch.index_id.as_str())
            .find(|index_id| !self.queues.queue_exists(index_id));

        if let Some(index_id) = first_non_existing_queue_opt {
            return Err(IngestApiError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let mut num_docs = 0usize;
        for doc_batch in &request.doc_batches {
            // TODO better error handling.
            // If there is an error, we probably want a transactional behavior.
            let records_it = iter_doc_payloads(doc_batch);
            self.queues
                .append_batch(&doc_batch.index_id, records_it, ctx)
                .await?;
            let batch_num_docs = doc_batch.doc_lens.len();
            num_docs += batch_num_docs;
            INGEST_METRICS
                .ingested_num_docs
                .inc_by(batch_num_docs as u64);
        }
        Ok(IngestResponse {
            num_docs_for_processing: num_docs as u64,
        })
    }

    fn fetch(&mut self, fetch_req: FetchRequest) -> crate::Result<FetchResponse> {
        let num_bytes_limit_opt: Option<usize> = fetch_req
            .num_bytes_limit
            .map(|num_bytes_limit| num_bytes_limit as usize);
        self.queues.fetch(
            &fetch_req.index_id,
            fetch_req.start_after,
            num_bytes_limit_opt,
        )
    }

    async fn suggest_truncate(
        &mut self,
        request: SuggestTruncateRequest,
        ctx: &ActorContext<Self>,
    ) -> crate::Result<()> {
        self.queues
            .suggest_truncate(&request.index_id, request.up_to_position_included, ctx)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Actor for IngestApiService {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn runtime_handle(&self) -> tokio::runtime::Handle {
        RuntimeType::NonBlocking.get_runtime_handle()
    }

    /// The Actor's incoming mailbox queue capacity. It is set when the actor is spawned.
    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(3)
    }
}

#[async_trait]
impl Handler<QueueExistsRequest> for IngestApiService {
    type Reply = crate::Result<bool>;
    async fn handle(
        &mut self,
        queue_exists_req: QueueExistsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(Ok(self.queues.queue_exists(&queue_exists_req.queue_id)))
    }
}

#[derive(Debug)]
pub struct GetPartitionId;

#[async_trait]
impl Handler<GetPartitionId> for IngestApiService {
    type Reply = String;
    async fn handle(
        &mut self,
        _: GetPartitionId,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.partition_id.clone())
    }
}

#[async_trait]
impl Handler<CreateQueueRequest> for IngestApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        create_queue_req: CreateQueueRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self
            .queues
            .create_queue(&create_queue_req.queue_id, ctx)
            .await)
    }
}

#[async_trait]
impl Handler<CreateQueueIfNotExistsRequest> for IngestApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        create_queue_inf_req: CreateQueueIfNotExistsRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        if self.queues.queue_exists(&create_queue_inf_req.queue_id) {
            return Ok(Ok(()));
        }
        Ok(self
            .queues
            .create_queue(&create_queue_inf_req.queue_id, ctx)
            .await)
    }
}

#[async_trait]
impl Handler<DropQueueRequest> for IngestApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        drop_queue_req: DropQueueRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.drop_queue(&drop_queue_req.queue_id, ctx).await)
    }
}

#[async_trait]
impl Handler<IngestRequest> for IngestApiService {
    type Reply = crate::Result<IngestResponse>;
    async fn handle(
        &mut self,
        ingest_req: IngestRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.ingest(ingest_req, ctx).await)
    }
}

#[async_trait]
impl Handler<FetchRequest> for IngestApiService {
    type Reply = crate::Result<FetchResponse>;
    async fn handle(
        &mut self,
        request: FetchRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.fetch(request))
    }
}

#[async_trait]
impl Handler<TailRequest> for IngestApiService {
    type Reply = crate::Result<FetchResponse>;
    async fn handle(
        &mut self,
        request: TailRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.tail(&request.index_id))
    }
}

#[async_trait]
impl Handler<SuggestTruncateRequest> for IngestApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        request: SuggestTruncateRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.suggest_truncate(request, ctx).await)
    }
}

#[async_trait]
impl Handler<ListQueuesRequest> for IngestApiService {
    type Reply = crate::Result<ListQueuesResponse>;
    async fn handle(
        &mut self,
        _list_queue_req: ListQueuesRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.list_queues())
    }
}
