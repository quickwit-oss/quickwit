// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;
use std::{fmt, iter};

use async_trait::async_trait;
use bytes::Bytes;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, DeferableReplyHandler, Handler, QueueCapacity,
};
use quickwit_common::runtimes::RuntimeType;
use quickwit_common::tower::Cost;
use quickwit_proto::ingest::RateLimitingCause;
use tracing::{error, info};
use ulid::Ulid;

use crate::metrics::INGEST_METRICS;
use crate::notifications::Notifications;
use crate::{
    CommitType, CreateQueueIfNotExistsRequest, CreateQueueIfNotExistsResponse, CreateQueueRequest,
    DocCommand, DropQueueRequest, FetchRequest, FetchResponse, IngestRequest, IngestResponse,
    IngestServiceError, ListQueuesRequest, ListQueuesResponse, MemoryCapacity, Queues,
    SuggestTruncateRequest, TailRequest,
};

impl Cost for IngestRequest {
    fn cost(&self) -> u64 {
        self.doc_batches
            .iter()
            .map(|doc_batch| doc_batch.num_bytes())
            .sum::<usize>() as u64
    }
}

pub struct IngestApiService {
    partition_id: String,
    queues: Queues,
    memory_limit: usize,
    disk_limit: usize,
    memory_capacity: MemoryCapacity,
    notifications: Notifications,
}

impl fmt::Debug for IngestApiService {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
            let msg = format!("partition key ({partition_id_bytes:?}) is not utf8");
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
        let memory_capacity = MemoryCapacity::new(memory_limit);
        let notifications = Notifications::new();
        info!(ingest_partition_id=%partition_id, "Ingest API partition id");
        Ok(Self {
            partition_id,
            queues,
            memory_limit,
            disk_limit,
            memory_capacity,
            notifications,
        })
    }

    async fn ingest(
        &mut self,
        request: IngestRequest,
        reply: impl FnOnce(crate::Result<IngestResponse>) + Send + Sync + 'static,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let notification = self.ingest_inner(request, ctx).await;
        match notification {
            Ok((response, index_positions)) => {
                if index_positions.is_empty() {
                    reply(Ok(response));
                } else {
                    self.notifications
                        .register(index_positions, move || {
                            reply(Ok(response));
                        })
                        .await;
                }
                Ok(())
            }
            Err(err) => {
                reply(Err(err));
                Ok(())
            }
        }
    }

    async fn ingest_inner(
        &mut self,
        request: IngestRequest,
        ctx: &ActorContext<Self>,
    ) -> crate::Result<(IngestResponse, Vec<(String, u64)>)> {
        // Check all indexes exist assuming existing queues always have a corresponding index.
        let first_non_existing_queue_opt = request
            .doc_batches
            .iter()
            .map(|batch| batch.index_id.as_str())
            .find(|index_id| !self.queues.queue_exists(index_id));

        if let Some(index_id) = first_non_existing_queue_opt {
            error!(
                index_id,
                partition_id = self.partition_id,
                "could not find index"
            );
            return Err(IngestServiceError::IndexNotFound {
                index_id: index_id.to_string(),
            });
        }
        let disk_used = self.queues.resource_usage().disk_used_bytes;

        if disk_used > self.disk_limit {
            info!("ingestion rejected due to disk limit");
            return Err(IngestServiceError::RateLimited(RateLimitingCause::WalFull));
        }

        if self
            .memory_capacity
            .reserve_capacity(request.cost() as usize)
            .is_err()
        {
            info!("ingest request rejected due to memory limit");
            return Err(IngestServiceError::RateLimited(RateLimitingCause::WalFull));
        }
        let mut num_docs = 0usize;
        let mut notifications = Vec::new();
        let commit = request.commit();
        for doc_batch in request.doc_batches {
            // TODO better error handling.
            // If there is an error, we probably want a transactional behavior.

            let batch_num_docs = doc_batch.num_docs();
            let batch_num_bytes = doc_batch.num_bytes();
            let index_id = doc_batch.index_id.clone();
            let records_it = doc_batch.into_iter_raw();
            let max_position = self.queues.append_batch(&index_id, records_it, ctx).await?;
            if let Some(max_position) = max_position
                && commit != CommitType::Auto
            {
                if commit == CommitType::Force {
                    self.queues
                        .append_batch(
                            &index_id,
                            iter::once(DocCommand::Commit::<Bytes>.into_buf()),
                            ctx,
                        )
                        .await?;
                }
                notifications.push((index_id.clone(), max_position));
            }

            num_docs += batch_num_docs;
            INGEST_METRICS
                .ingested_docs_bytes_valid
                .inc_by(batch_num_bytes as u64);
            INGEST_METRICS
                .ingested_docs_valid
                .inc_by(batch_num_docs as u64);
        }
        // TODO we could fsync here and disable autosync to have better i/o perfs.
        Ok((
            IngestResponse {
                num_docs_for_processing: num_docs as u64,
            },
            notifications,
        ))
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
        self.notifications
            .notify(&request.index_id, request.up_to_position_included)
            .await;
        self.queues
            .suggest_truncate(&request.index_id, request.up_to_position_included, ctx)
            .await?;

        let memory_used = self.queues.resource_usage().memory_used_bytes;
        let new_capacity = self.memory_limit - memory_used;
        self.memory_capacity.reset_capacity(new_capacity);

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

#[derive(Debug)]
pub struct GetPartitionId;

#[async_trait]
impl Handler<GetPartitionId> for IngestApiService {
    type Reply = String;

    async fn handle(
        &mut self,
        _request: GetPartitionId,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.partition_id.clone())
    }
}

#[derive(Debug)]
pub struct GetMemoryCapacity;

#[async_trait]
impl Handler<GetMemoryCapacity> for IngestApiService {
    type Reply = MemoryCapacity;

    async fn handle(
        &mut self,
        _request: GetMemoryCapacity,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.memory_capacity.clone())
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
    type Reply = crate::Result<CreateQueueIfNotExistsResponse>;
    async fn handle(
        &mut self,
        create_queue_inf_req: CreateQueueIfNotExistsRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        if self.queues.queue_exists(&create_queue_inf_req.queue_id) {
            let response = CreateQueueIfNotExistsResponse {
                queue_id: create_queue_inf_req.queue_id,
                created: false,
            };
            return Ok(Ok(response));
        }
        Ok(self
            .queues
            .create_queue(&create_queue_inf_req.queue_id, ctx)
            .await
            .map(|_| CreateQueueIfNotExistsResponse {
                queue_id: create_queue_inf_req.queue_id,
                created: true,
            }))
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
impl DeferableReplyHandler<IngestRequest> for IngestApiService {
    type Reply = crate::Result<IngestResponse>;
    async fn handle_message(
        &mut self,
        ingest_req: IngestRequest,
        reply: impl FnOnce(Self::Reply) + Send + Sync + 'static,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.ingest(ingest_req, reply, ctx).await?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use quickwit_actors::Universe;
    use quickwit_config::IngestApiConfig;

    use super::*;
    use crate::{DocBatch, DocBatchBuilder, init_ingest_api};

    #[test]
    fn test_ingest_request_cost() {
        let ingest_request = IngestRequest {
            doc_batches: vec![
                DocBatch {
                    index_id: "index-1".to_string(),
                    doc_buffer: Bytes::from_static(&[0, 1, 2]),
                    doc_lengths: vec![1, 2],
                },
                DocBatch {
                    index_id: "index-2".to_string(),
                    doc_buffer: Bytes::from_static(&[3, 4, 5, 6, 7, 8]),
                    doc_lengths: vec![1, 3, 2],
                },
            ],
            commit: CommitType::Auto.into(),
        };
        assert_eq!(ingest_request.cost(), 9);
    }

    #[tokio::test]
    async fn test_ingest_api_service_with_commit() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir()?;
        let queues_dir_path = temp_dir.path();

        let ingest_api_service =
            init_ingest_api(&universe, queues_dir_path, &IngestApiConfig::default()).await?;

        // Ensure a queue for this index exists.
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: "index-1".to_string(),
        };

        ingest_api_service.ask_for_res(create_queue_req).await?;

        let mut batch = DocBatchBuilder::new("index-1".to_string());
        batch.ingest_doc(Bytes::from_static(b"Test1"));
        batch.ingest_doc(Bytes::from_static(b"Test2"));
        batch.ingest_doc(Bytes::from_static(b"Test3"));
        batch.ingest_doc(Bytes::from_static(b"Test4"));

        let ingest_request = IngestRequest {
            doc_batches: vec![batch.build()],
            commit: CommitType::Force.into(),
        };
        let ingest_response = ingest_api_service
            .send_message(ingest_request)
            .await
            .unwrap();
        universe.sleep(Duration::from_secs(2)).await;
        let fetch_request = FetchRequest {
            index_id: "index-1".to_string(),
            start_after: None,
            num_bytes_limit: None,
        };
        let fetch_response = ingest_api_service.ask_for_res(fetch_request).await.unwrap();
        let doc_batch = fetch_response.doc_batch.unwrap();
        let position = doc_batch.num_docs() as u64;
        assert_eq!(doc_batch.num_docs(), 5);
        assert!(matches!(
            doc_batch.into_iter().nth(4),
            Some(DocCommand::Commit::<Bytes>)
        ));
        ingest_api_service
            .send_message(SuggestTruncateRequest {
                index_id: "index-1".to_string(),
                up_to_position_included: position,
            })
            .await
            .unwrap();

        let ingest_response = ingest_response.await.unwrap().unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 4);

        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_ingest_api_service_with_wait() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir()?;
        let queues_dir_path = temp_dir.path();

        let ingest_api_service =
            init_ingest_api(&universe, queues_dir_path, &IngestApiConfig::default()).await?;

        // Ensure a queue for this index exists.
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: "index-1".to_string(),
        };

        ingest_api_service.ask_for_res(create_queue_req).await?;

        let mut batch = DocBatchBuilder::new("index-1".to_string());
        batch.ingest_doc(Bytes::from_static(b"Test1"));
        batch.ingest_doc(Bytes::from_static(b"Test2"));
        batch.ingest_doc(Bytes::from_static(b"Test3"));
        batch.ingest_doc(Bytes::from_static(b"Test4"));

        let ingest_request = IngestRequest {
            doc_batches: vec![batch.build()],
            commit: CommitType::WaitFor.into(),
        };
        let ingest_response = ingest_api_service
            .send_message(ingest_request)
            .await
            .unwrap();
        universe.sleep(Duration::from_secs(2)).await;
        let fetch_request = FetchRequest {
            index_id: "index-1".to_string(),
            start_after: None,
            num_bytes_limit: None,
        };
        let fetch_response = ingest_api_service.ask_for_res(fetch_request).await.unwrap();
        let doc_batch = fetch_response.doc_batch.unwrap();
        let position = doc_batch.num_docs() as u64;
        assert_eq!(doc_batch.num_docs(), 4);
        ingest_api_service
            .send_message(SuggestTruncateRequest {
                index_id: "index-1".to_string(),
                up_to_position_included: position,
            })
            .await
            .unwrap();

        let ingest_response = ingest_response.await.unwrap().unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 4);

        universe.assert_quit().await;
        Ok(())
    }
}
