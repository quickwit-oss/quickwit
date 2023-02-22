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

use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use tokio::sync::{Notify, Semaphore};
use tracing::info;
use ulid::Ulid;

use crate::metrics::INGEST_METRICS;
use crate::{
    iter_doc_payloads, CreateQueueIfNotExistsRequest, CreateQueueRequest, DropQueueRequest,
    FetchRequest, FetchResponse, IngestRequest, IngestResponse, IngestService, IngestServiceError,
    ListQueuesRequest, ListQueuesResponse, QueueExistsRequest, Queues, SuggestTruncateRequest,
    TailRequest,
};

#[derive(Clone)]
pub struct IngestApiService {
    partition_id: String,
    queues: Queues,
    memory_limit: usize,
    disk_limit: usize,
    capacity: Arc<AtomicIsize>,
    capacity_notify: Arc<Notify>,
}

impl fmt::Debug for IngestApiService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngestApiService")
            .field("partition_id", &self.partition_id)
            .field("memory_limit", &self.memory_limit)
            .field("disk_limit", &self.disk_limit)
            .finish()
    }
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
            IngestServiceError::Corruption(msg)
        })?;
        return Ok(partition_id.to_string());
    }
    // We add a prefix here to make sure we don't mistake it for a split id when reading logs.
    let partition_id = format!("ingest_partition_{}", Ulid::new());
    tokio::fs::write(partition_id_path, partition_id.as_bytes()).await?;
    Ok(partition_id)
}

impl IngestApiService {
    pub async fn with_queues_dir(
        queues_dir_path: &Path,
        memory_limit: usize,
        disk_limit: usize,
    ) -> crate::Result<Self> {
        let queues = Queues::open(queues_dir_path).await?;
        let partition_id = get_or_initialize_partition_id(queues_dir_path).await?;
        info!(ingest_partition_id=%partition_id, "Ingest API partition id");
        Ok(IngestApiService {
            partition_id,
            queues,
            memory_limit,
            disk_limit,
            capacity: Arc::new(AtomicIsize::new(memory_limit as isize)),
            capacity_notify: Arc::new(Notify::new()),
        })
    }

    async fn ingest_inner(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        // Check all indexes exist assuming existing queues always have a corresponding index.
        for doc_batch in &request.doc_batches {
            if !self.queues.queue_exists(&doc_batch.index_id).await {
                return Err(IngestServiceError::IndexNotFound {
                    index_id: doc_batch.index_id.clone(),
                });
            }
        }
        let payload_size = request
            .doc_batches
            .iter()
            .map(|doc_batch| doc_batch.doc_lens.iter().sum::<u64>())
            .sum::<u64>() as isize;
        loop {
            let current_capacity = self.capacity.load(Ordering::Acquire);

            if current_capacity > 0 {
                let new_capacity = current_capacity - payload_size;

                if new_capacity >= 0 {
                    if self
                        .capacity
                        .compare_exchange(
                            current_capacity,
                            new_capacity,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if new_capacity > 0 {
                            self.capacity_notify.notify_one();
                        }
                        break;
                    }
                }
            }
            self.capacity_notify.notified().await;
        }
        let mut num_docs = 0usize;
        for doc_batch in &request.doc_batches {
            // TODO better error handling.
            // If there is an error, we probably want a transactional behavior.
            let records_it = iter_doc_payloads(doc_batch);
            self.queues
                .append_batch(&doc_batch.index_id, records_it)
                .await?;
            let batch_num_docs = doc_batch.doc_lens.len();
            num_docs += batch_num_docs;
            INGEST_METRICS
                .ingested_num_docs
                .inc_by(batch_num_docs as u64);
        }
        // TODO we could fsync here and disable autosync to have better i/o perfs.
        Ok(IngestResponse {
            num_docs_for_processing: num_docs as u64,
        })
    }

    async fn suggest_truncate(&mut self, request: SuggestTruncateRequest) -> crate::Result<()> {
        self.queues
            .suggest_truncate(&request.index_id, request.up_to_position_included)
            .await?;

        let (memory_usage, _disk_usage) = self.queues.ressource_usage().await;
        let new_capacity = self.memory_limit as isize - memory_usage as isize;

        if new_capacity > 0 {
            self.capacity.store(new_capacity, Ordering::Release);
            self.capacity_notify.notify_one();
        }
        Ok(())
    }
}

#[async_trait]
impl IngestService for IngestApiService {
    async fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        self.ingest_inner(request).await
    }

    async fn fetch(&mut self, request: FetchRequest) -> crate::Result<FetchResponse> {
        let num_bytes_limit_opt: Option<usize> = request
            .num_bytes_limit
            .map(|num_bytes_limit| num_bytes_limit as usize);
        self.queues
            .fetch(&request.index_id, request.start_after, num_bytes_limit_opt)
            .await
    }

    async fn tail(&mut self, request: TailRequest) -> crate::Result<FetchResponse> {
        self.queues.tail(&request.index_id).await
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
    type Reply = bool;
    async fn handle(
        &mut self,
        queue_exists_req: QueueExistsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.queue_exists(&queue_exists_req.queue_id).await)
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

#[derive(Debug)]
pub struct GetIngestServiceImpl;

#[async_trait]
impl Handler<GetIngestServiceImpl> for IngestApiService {
    type Reply = IngestApiService;
    async fn handle(
        &mut self,
        _: GetIngestServiceImpl,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.clone())
    }
}

#[async_trait]
impl Handler<CreateQueueRequest> for IngestApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        request: CreateQueueRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.create_queue(&request.queue_id).await)
    }
}

#[async_trait]
impl Handler<CreateQueueIfNotExistsRequest> for IngestApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        request: CreateQueueIfNotExistsRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        if self.queues.queue_exists(&request.queue_id).await {
            return Ok(Ok(()));
        }
        Ok(self.queues.create_queue(&request.queue_id).await)
    }
}

#[async_trait]
impl Handler<DropQueueRequest> for IngestApiService {
    type Reply = crate::Result<()>;
    async fn handle(
        &mut self,
        request: DropQueueRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.queues.drop_queue(&request.queue_id).await)
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
        Ok(self.fetch(request).await)
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
        Ok(self.suggest_truncate(request).await)
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
        Ok(self.queues.list_queues().await)
    }
}
