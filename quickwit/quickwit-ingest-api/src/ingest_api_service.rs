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
use std::ops::Bound;
use std::path::Path;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, QueueCapacity};
use quickwit_common::runtimes::RuntimeType;
use tracing::info;
use ulid::Ulid;

use crate::metrics::INGEST_METRICS;
use crate::{
    iter_doc_payloads, CreateQueueIfNotExistsRequest, CreateQueueRequest, DocBatch,
    DropQueueRequest, FetchRequest, FetchResponse, IngestRequest, IngestResponse,
    IngestServiceError, ListQueuesRequest, ListQueuesResponse, Queues, SuggestTruncateRequest,
    TailRequest,
};

pub struct IngestApiService {
    partition_id: String,
    queues: Queues,
    memory_limit: usize,
    disk_limit: usize,
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
        })
    }

    pub async fn create_queue(&mut self, queue_id: &str) -> crate::Result<()> {
        self.queues.create_queue(queue_id).await?;
        Ok(())
    }

    pub async fn create_queue_if_not_exists(&mut self, queue_id: &str) -> crate::Result<()> {
        self.queues.create_queue_if_not_exists(queue_id).await?;
        Ok(())
    }

    pub async fn list_queues(&self) -> crate::Result<ListQueuesResponse> {
        self.queues.list_queues().await
    }

    pub async fn drop_queue(&mut self, queue_id: &str) -> crate::Result<()> {
        self.queues.drop_queue(queue_id).await?;
        Ok(())
    }

    async fn ingest(&mut self, doc_batches: Vec<DocBatch>) -> crate::Result<IngestResponse> {
        // Check all indexes exist assuming existing queues always have a corresponding index.
        for doc_batch in &doc_batches {
            if !self.queues.queue_exists(&doc_batch.index_id).await {
                return Err(IngestServiceError::IndexNotFound {
                    index_id: doc_batch.index_id.clone(),
                });
            }
        }
        let (memory_usage, disk_usage) = self.queues.resource_usage().await;

        if memory_usage > self.memory_limit {
            info!("Ingestion rejected due to memory limit");
            return Err(IngestServiceError::RateLimited);
        }
        if disk_usage > self.disk_limit {
            info!("Ingestion rejected due to disk limit");
            return Err(IngestServiceError::RateLimited);
        }
        let mut num_docs = 0usize;
        for doc_batch in &doc_batches {
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

    async fn fetch(
        &mut self,
        queue_id: &str,
        start_after: Option<u64>,
        num_bytes_limit: Option<usize>,
    ) -> crate::Result<FetchResponse> {
        self.queues
            .fetch(queue_id, start_after, num_bytes_limit)
            .await
    }

    async fn tail(&mut self, queue_id: &str) -> crate::Result<FetchResponse> {
        self.queues.tail(&queue_id).await
    }

    pub async fn suggest_truncate(
        &mut self,
        queue_id: &str,
        position: Bound<u64>,
    ) -> crate::Result<()> {
        self.queues.suggest_truncate(queue_id, position).await?;
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

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(5)
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
        request: CreateQueueRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.create_queue(&request.queue_id).await)
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
        Ok(self.create_queue_if_not_exists(&request.queue_id).await)
    }
}

#[async_trait]
impl Handler<ListQueuesRequest> for IngestApiService {
    type Reply = crate::Result<ListQueuesResponse>;

    async fn handle(
        &mut self,
        _request: ListQueuesRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.list_queues().await)
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
        Ok(self.drop_queue(&request.queue_id).await)
    }
}

#[async_trait]
impl Handler<IngestRequest> for IngestApiService {
    type Reply = crate::Result<IngestResponse>;

    async fn handle(
        &mut self,
        request: IngestRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.ingest(request.doc_batches).await)
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
        let num_bytes_limit = request.num_bytes_limit.map(|num_bytes| num_bytes as usize);
        Ok(self
            .fetch(&request.index_id, request.start_after, num_bytes_limit)
            .await)
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
        Ok(self.tail(&request.index_id).await)
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
        Ok(self
            .suggest_truncate(
                &request.index_id,
                Bound::Included(request.up_to_position_included),
            )
            .await)
    }
}
