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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::iter::once;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use mrecordlog::error::{DeleteQueueError, TruncateError};
use mrecordlog::MultiRecordLog;
use quickwit_common::tower::Pool;
use quickwit_common::ServiceStream;
use quickwit_proto::ingest::ingester::{
    AckReplicationMessage, FetchResponseV2, IngesterService, IngesterServiceClient,
    IngesterServiceStream, OpenFetchStreamRequest, OpenReplicationStreamRequest,
    OpenReplicationStreamResponse, PersistFailure, PersistFailureKind, PersistRequest,
    PersistResponse, PersistSuccess, PingRequest, PingResponse, ReplicateRequest,
    ReplicateSubrequest, SynReplicationMessage, TruncateRequest, TruncateResponse,
};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error, IngestV2Result, ShardState};
use quickwit_proto::metastore::{
    CloseShardsFailureKind, CloseShardsRequest, CloseShardsSubrequest,
};
use quickwit_proto::split_queue_id;
use quickwit_proto::types::{NodeId, QueueId};
use tokio::sync::{watch, RwLock};
use tracing::{error, info};

use super::fetch::FetchTask;
use super::gc::remove_shards_after;
use super::ingest_metastore::IngestMetastore;
use super::models::{Position, PrimaryShard, ReplicaShard, ShardStatus};
use super::replication::{
    ReplicationClient, ReplicationClientTask, ReplicationTask, ReplicationTaskHandle,
};
use super::IngesterPool;
use crate::ingest_v2::gc::REMOVAL_GRACE_PERIOD;
use crate::metrics::INGEST_METRICS;
use crate::DocCommand;

#[derive(Clone)]
pub struct Ingester {
    self_node_id: NodeId,
    metastore: Arc<dyn IngestMetastore>,
    ingester_pool: IngesterPool,
    state: Arc<RwLock<IngesterState>>,
    replication_factor: usize,
}

impl fmt::Debug for Ingester {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ingester")
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

pub(super) struct IngesterState {
    pub mrecordlog: MultiRecordLog,
    pub primary_shards: HashMap<QueueId, PrimaryShard>,
    pub replica_shards: HashMap<QueueId, ReplicaShard>,
    pub replication_clients: HashMap<NodeId, ReplicationClient>,
    pub replication_tasks: HashMap<NodeId, ReplicationTaskHandle>,
}

impl IngesterState {
    fn find_shard_status_rx(&self, queue_id: &QueueId) -> Option<watch::Receiver<ShardStatus>> {
        if let Some(shard) = self.primary_shards.get(queue_id) {
            return Some(shard.shard_status_rx.clone());
        }
        if let Some(shard) = self.replica_shards.get(queue_id) {
            return Some(shard.shard_status_rx.clone());
        }
        None
    }
}

impl Ingester {
    pub async fn try_new(
        self_node_id: NodeId,
        metastore: Arc<dyn IngestMetastore>,
        ingester_pool: Pool<NodeId, IngesterServiceClient>,
        wal_dir_path: &Path,
        replication_factor: usize,
    ) -> IngestV2Result<Self> {
        let mrecordlog = MultiRecordLog::open_with_prefs(
            wal_dir_path,
            mrecordlog::SyncPolicy::OnDelay(Duration::from_secs(5)),
        )
        .await
        .map_err(|error| IngestV2Error::Internal(error.to_string()))?;

        let inner = IngesterState {
            mrecordlog,
            primary_shards: HashMap::new(),
            replica_shards: HashMap::new(),
            replication_clients: HashMap::new(),
            replication_tasks: HashMap::new(),
        };
        let mut ingester = Self {
            self_node_id,
            metastore,
            ingester_pool,
            state: Arc::new(RwLock::new(inner)),
            replication_factor,
        };
        info!(
            replication_factor=%replication_factor,
            wal_dir=%wal_dir_path.display(),
            "spawning ingester"
        );
        ingester.init().await?;

        Ok(ingester)
    }

    async fn init(&mut self) -> IngestV2Result<()> {
        let mut per_queue_positions: HashMap<QueueId, Option<u64>> = HashMap::new();
        let state_guard = self.state.read().await;

        for queue_id in state_guard.mrecordlog.list_queues() {
            let current_position = state_guard
                .mrecordlog
                .current_position(queue_id)
                .expect("queue should exist");
            per_queue_positions.insert(queue_id.to_string(), current_position);
        }
        let mut subrequests = Vec::new();

        for (queue_id, current_position) in &per_queue_positions {
            let (index_uid, source_id, shard_id) =
                split_queue_id(queue_id).expect("queue ID should be well-formed");

            let subrequest = CloseShardsSubrequest {
                index_uid: index_uid.into(),
                source_id,
                shard_id,
                shard_state: ShardState::Closed as i32,
                replication_position_inclusive: *current_position,
            };
            subrequests.push(subrequest);
        }
        if subrequests.is_empty() {
            return Ok(());
        }
        drop(state_guard);

        let close_shards_request = CloseShardsRequest { subrequests };
        let close_shards_response = self.metastore.close_shards(close_shards_request).await?;
        info!("closed {} shard(s)", close_shards_response.successes.len());

        let mut state_guard = self.state.write().await;

        // Keep track of the queues that can be safely deleted.
        let mut queues_to_remove: Vec<QueueId> = Vec::new();

        for success in close_shards_response.successes {
            let queue_id = success.queue_id();
            let publish_position_inclusive: Position = success.publish_position_inclusive.into();

            if let Some(truncate_position) = publish_position_inclusive.offset() {
                state_guard
                    .mrecordlog
                    .truncate(&queue_id, truncate_position)
                    .await
                    .expect("queue should exist");
            }
            let current_position: Position = (*per_queue_positions
                .get(&queue_id)
                .expect("queue should exist"))
            .into();
            let shard_status = ShardStatus {
                shard_state: ShardState::Closed,
                publish_position_inclusive,
                replication_position_inclusive: current_position,
            };
            let (shard_status_tx, shard_status_rx) = watch::channel(shard_status);

            if publish_position_inclusive >= current_position {
                queues_to_remove.push(queue_id.clone())
            }
            if success.leader_id == self.self_node_id {
                let primary_shard = PrimaryShard {
                    follower_id_opt: success.follower_id.map(|follower_id| follower_id.into()),
                    shard_state: ShardState::Closed,
                    publish_position_inclusive,
                    primary_position_inclusive: current_position,
                    replica_position_inclusive_opt: None,
                    shard_status_tx,
                    shard_status_rx,
                };
                state_guard.primary_shards.insert(queue_id, primary_shard);
            } else {
                let replica_shard = ReplicaShard {
                    _leader_id: success.leader_id.into(),
                    shard_state: ShardState::Closed,
                    publish_position_inclusive,
                    replica_position_inclusive: current_position,
                    shard_status_tx,
                    shard_status_rx,
                };
                state_guard.replica_shards.insert(queue_id, replica_shard);
            }
        }
        for failure in close_shards_response.failures {
            if failure.failure_kind() == CloseShardsFailureKind::NotFound {
                let queue_id = failure.queue_id();

                if let Err(DeleteQueueError::IoError(error)) =
                    state_guard.mrecordlog.delete_queue(&queue_id).await
                {
                    error!("failed to delete mrecordlog queue: {}", error);
                }
            }
        }
        remove_shards_after(
            queues_to_remove,
            REMOVAL_GRACE_PERIOD,
            self.metastore.clone(),
            self.state.clone(),
        );
        Ok(())
    }

    async fn init_primary_shard<'a>(
        &self,
        state: &'a mut IngesterState,
        queue_id: &QueueId,
        leader_id: &NodeId,
        follower_id_opt: Option<&NodeId>,
    ) -> IngestV2Result<&'a PrimaryShard> {
        if !state.mrecordlog.queue_exists(queue_id) {
            state.mrecordlog.create_queue(queue_id).await.expect("TODO"); // IO error, what to do?
        } else {
            // TODO: Recover last position from mrecordlog and take it from there.
        }
        if let Some(follower_id) = follower_id_opt {
            self.init_replication_client(state, leader_id, follower_id)
                .await?;
        }
        let replica_position_inclusive_opt = follower_id_opt.map(|_| Position::default());
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());

        let primary_shard = PrimaryShard {
            follower_id_opt: follower_id_opt.cloned(),
            shard_state: ShardState::Open,
            publish_position_inclusive: Position::default(),
            primary_position_inclusive: Position::default(),
            replica_position_inclusive_opt,
            shard_status_tx,
            shard_status_rx,
        };
        let entry = state.primary_shards.entry(queue_id.clone());
        Ok(entry.or_insert(primary_shard))
    }

    async fn init_replication_client(
        &self,
        state: &mut IngesterState,
        leader_id: &NodeId,
        follower_id: &NodeId,
    ) -> IngestV2Result<()> {
        let Entry::Vacant(entry) = state.replication_clients.entry(follower_id.clone()) else {
            // The replication client is already initialized. Nothing to do!
            return Ok(());
        };
        let open_request = OpenReplicationStreamRequest {
            leader_id: leader_id.clone().into(),
            follower_id: follower_id.clone().into(),
        };
        let open_message = SynReplicationMessage::new_open_request(open_request);
        let (syn_replication_stream_tx, syn_replication_stream) = ServiceStream::new_bounded(5);
        syn_replication_stream_tx
            .try_send(open_message)
            .expect("The channel should be open and have capacity.");

        let mut ingester =
            self.ingester_pool
                .get(follower_id)
                .ok_or(IngestV2Error::IngesterUnavailable {
                    ingester_id: follower_id.clone(),
                })?;
        let mut ack_replication_stream = ingester
            .open_replication_stream(syn_replication_stream)
            .await?;
        ack_replication_stream
            .next()
            .await
            .expect("TODO")
            .expect("")
            .into_open_response()
            .expect("The first message should be an open response.");

        let replication_client =
            ReplicationClientTask::spawn(syn_replication_stream_tx, ack_replication_stream);
        entry.insert(replication_client);
        Ok(())
    }
}

#[async_trait]
impl IngesterService for Ingester {
    async fn persist(
        &mut self,
        persist_request: PersistRequest,
    ) -> IngestV2Result<PersistResponse> {
        if persist_request.leader_id != self.self_node_id {
            return Err(IngestV2Error::Internal(format!(
                "routing error: request was sent to ingester node `{}` instead of `{}`",
                self.self_node_id, persist_request.leader_id,
            )));
        }
        let mut state_guard = self.state.write().await;

        let mut persist_successes = Vec::with_capacity(persist_request.subrequests.len());
        let mut persist_failures = Vec::new();
        let mut replicate_subrequests: HashMap<NodeId, Vec<ReplicateSubrequest>> = HashMap::new();

        let commit_type = persist_request.commit_type();
        let force_commit = commit_type == CommitTypeV2::Force;
        let leader_id: NodeId = persist_request.leader_id.into();

        for subrequest in persist_request.subrequests {
            let queue_id = subrequest.queue_id();
            let follower_id: Option<NodeId> = subrequest.follower_id.map(Into::into);
            let primary_shard =
                if let Some(primary_shard) = state_guard.primary_shards.get(&queue_id) {
                    primary_shard
                } else {
                    self.init_primary_shard(
                        &mut state_guard,
                        &queue_id,
                        &leader_id,
                        follower_id.as_ref(),
                    )
                    .await?
                };
            if primary_shard.shard_state.is_closed() {
                let persist_failure = PersistFailure {
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    failure_kind: PersistFailureKind::ShardClosed as i32,
                };
                persist_failures.push(persist_failure);
                continue;
            }
            let from_position_inclusive = primary_shard.primary_position_inclusive;

            let Some(doc_batch) = subrequest.doc_batch else {
                let persist_success = PersistSuccess {
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    replication_position_inclusive: from_position_inclusive.offset(),
                };
                persist_successes.push(persist_success);
                continue;
            };
            let primary_position_inclusive = if force_commit {
                let docs = doc_batch.docs().chain(once(commit_doc()));
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, docs)
                    .await
                    .expect("TODO") // TODO: Io error, close shard?
            } else {
                let docs = doc_batch.docs();
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, docs)
                    .await
                    .expect("TODO") // TODO: Io error, close shard?
            };
            let batch_num_bytes = doc_batch.num_bytes() as u64;
            let batch_num_docs = doc_batch.num_docs() as u64;

            INGEST_METRICS.ingested_num_bytes.inc_by(batch_num_bytes);
            INGEST_METRICS.ingested_num_docs.inc_by(batch_num_docs);

            state_guard
                .primary_shards
                .get_mut(&queue_id)
                .expect("primary shard should exist")
                .set_primary_position_inclusive(primary_position_inclusive);

            if let Some(follower_id) = follower_id {
                let replicate_subrequest = ReplicateSubrequest {
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    from_position_exclusive: from_position_inclusive.offset(),
                    doc_batch: Some(doc_batch),
                };
                replicate_subrequests
                    .entry(follower_id)
                    .or_default()
                    .push(replicate_subrequest);
            } else {
                let persist_success = PersistSuccess {
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    replication_position_inclusive: primary_position_inclusive,
                };
                persist_successes.push(persist_success);
            }
        }
        if replicate_subrequests.is_empty() {
            let leader_id = self.self_node_id.to_string();
            let persist_response = PersistResponse {
                leader_id,
                successes: persist_successes,
                failures: persist_failures,
            };
            return Ok(persist_response);
        }
        let mut replicate_futures = FuturesUnordered::new();

        for (follower_id, subrequests) in replicate_subrequests {
            let replicate_request = ReplicateRequest {
                leader_id: self.self_node_id.clone().into(),
                follower_id: follower_id.clone().into(),
                subrequests,
                commit_type: persist_request.commit_type,
            };
            let replication_client = state_guard
                .replication_clients
                .get(&follower_id)
                .expect("the replication client should be initialized")
                .clone();
            replicate_futures
                .push(async move { replication_client.replicate(replicate_request).await });
        }
        // Drop the write lock AFTER pushing the replicate request into the replication client
        // channel to ensure that sequential writes in mrecordlog turn into sequential replicate
        // requests in the same order.
        drop(state_guard);

        while let Some(replicate_result) = replicate_futures.next().await {
            let replicate_response = replicate_result?;

            for replicate_success in replicate_response.successes {
                let persist_success = PersistSuccess {
                    index_uid: replicate_success.index_uid,
                    source_id: replicate_success.source_id,
                    shard_id: replicate_success.shard_id,
                    replication_position_inclusive: replicate_success.replica_position_inclusive,
                };
                persist_successes.push(persist_success);
            }
        }
        let mut state_guard = self.state.write().await;

        for persist_success in &persist_successes {
            let queue_id = persist_success.queue_id();
            state_guard
                .primary_shards
                .get_mut(&queue_id)
                .expect("TODO")
                .set_replica_position_inclusive(persist_success.replication_position_inclusive);
        }
        let leader_id = self.self_node_id.to_string();
        let persist_response = PersistResponse {
            leader_id,
            successes: persist_successes,
            failures: Vec::new(), // TODO
        };
        Ok(persist_response)
    }

    /// Opens a replication stream, which is a bi-directional gRPC stream. The client-side stream
    async fn open_replication_stream(
        &mut self,
        mut syn_replication_stream: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> IngestV2Result<IngesterServiceStream<AckReplicationMessage>> {
        let open_replication_stream_request = syn_replication_stream
            .next()
            .await
            .ok_or_else(|| IngestV2Error::Internal("syn replication stream aborted".to_string()))?
            .into_open_request()
            .expect("the first message should be an open replication stream request");

        if open_replication_stream_request.follower_id != self.self_node_id {
            return Err(IngestV2Error::Internal("routing error".to_string()));
        }
        let leader_id: NodeId = open_replication_stream_request.leader_id.into();
        let follower_id: NodeId = open_replication_stream_request.follower_id.into();

        let mut state_guard = self.state.write().await;

        let Entry::Vacant(entry) = state_guard.replication_tasks.entry(leader_id.clone()) else {
            return Err(IngestV2Error::Internal(format!(
                "a replication stream betwen {leader_id} and {follower_id} is already opened"
            )));
        };
        let (ack_replication_stream_tx, ack_replication_stream) = ServiceStream::new_bounded(5);
        let open_response = OpenReplicationStreamResponse {};
        let ack_replication_message = AckReplicationMessage::new_open_response(open_response);
        ack_replication_stream_tx
            .send(Ok(ack_replication_message))
            .await
            .expect("channel should be open and have enough capacity");

        let replication_task_handle = ReplicationTask::spawn(
            leader_id,
            follower_id,
            self.state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
        );
        entry.insert(replication_task_handle);
        Ok(ack_replication_stream)
    }

    async fn open_fetch_stream(
        &mut self,
        open_fetch_stream_request: OpenFetchStreamRequest,
    ) -> IngestV2Result<ServiceStream<IngestV2Result<FetchResponseV2>>> {
        let queue_id = open_fetch_stream_request.queue_id();
        let shard_status_rx = self
            .state
            .read()
            .await
            .find_shard_status_rx(&queue_id)
            .ok_or_else(|| IngestV2Error::Internal("shard not found".to_string()))?;
        let (service_stream, _fetch_task_handle) = FetchTask::spawn(
            open_fetch_stream_request,
            self.state.clone(),
            shard_status_rx,
            FetchTask::DEFAULT_BATCH_NUM_BYTES,
        );
        Ok(service_stream)
    }

    async fn ping(&mut self, ping_request: PingRequest) -> IngestV2Result<PingResponse> {
        if ping_request.leader_id != self.self_node_id {
            let ping_response = PingResponse {};
            return Ok(ping_response);
        };
        let Some(follower_id) = &ping_request.follower_id else {
            let ping_response = PingResponse {};
            return Ok(ping_response);
        };
        let follower_id: NodeId = follower_id.clone().into();
        let mut ingester = self.ingester_pool.get(&follower_id).ok_or({
            IngestV2Error::IngesterUnavailable {
                ingester_id: follower_id,
            }
        })?;
        ingester.ping(ping_request).await?;
        let ping_response = PingResponse {};
        Ok(ping_response)
    }

    async fn truncate(
        &mut self,
        truncate_request: TruncateRequest,
    ) -> IngestV2Result<TruncateResponse> {
        if truncate_request.ingester_id != self.self_node_id {
            return Err(IngestV2Error::Internal(format!(
                "routing error: expected ingester `{}`, got `{}`",
                self.self_node_id, truncate_request.ingester_id,
            )));
        }
        let mut queues_to_remove: Vec<QueueId> = Vec::new();
        let mut state_guard = self.state.write().await;

        for subrequest in truncate_request.subrequests {
            let queue_id = subrequest.queue_id();

            match state_guard
                .mrecordlog
                .truncate(&queue_id, subrequest.to_position_inclusive)
                .await
            {
                Ok(_) | Err(TruncateError::MissingQueue(_)) => {}
                Err(error) => {
                    error!("failed to truncate queue `{}`: {}", queue_id, error);
                    continue;
                }
            }
            if let Some(primary_shard) = state_guard.primary_shards.get_mut(&queue_id) {
                primary_shard.set_publish_position_inclusive(subrequest.to_position_inclusive);

                if primary_shard.is_removable() {
                    queues_to_remove.push(queue_id.clone());
                }
                continue;
            }
            if let Some(replica_shard) = state_guard.replica_shards.get_mut(&queue_id) {
                replica_shard.set_publish_position_inclusive(subrequest.to_position_inclusive);

                if replica_shard.is_removable() {
                    queues_to_remove.push(queue_id.clone());
                }
            }
        }
        drop(state_guard);

        remove_shards_after(
            queues_to_remove,
            REMOVAL_GRACE_PERIOD,
            self.metastore.clone(),
            self.state.clone(),
        );
        let truncate_response = TruncateResponse {};
        Ok(truncate_response)
    }
}

// TODO
pub(super) fn commit_doc() -> Bytes {
    let mut buffer = BytesMut::with_capacity(1);
    let command = DocCommand::<BytesMut>::Commit;
    command.write(&mut buffer);
    Bytes::from(buffer)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use quickwit_proto::ingest::ingester::{
        IngesterServiceGrpcServer, IngesterServiceGrpcServerAdapter, PersistSubrequest,
        TruncateSubrequest,
    };
    use quickwit_proto::ingest::DocBatchV2;
    use quickwit_proto::metastore::{
        CloseShardsFailure, CloseShardsFailureKind, CloseShardsResponse, CloseShardsSuccess,
        DeleteShardsResponse,
    };
    use quickwit_proto::types::queue_id;
    use tonic::transport::{Endpoint, Server};
    use tower::timeout::Timeout;

    use super::*;
    use crate::ingest_v2::ingest_metastore::MockIngestMetastore;
    use crate::ingest_v2::test_utils::{
        MultiRecordLogTestExt, PrimaryShardTestExt, ReplicaShardTestExt,
    };

    const NONE_REPLICA_POSITION: Option<Position> = None;

    #[tokio::test]
    async fn test_ingester_init() {
        let tempdir = tempfile::tempdir().unwrap();
        let self_node_id: NodeId = "test-ingester-0".into();
        let mut mock_metastore = MockIngestMetastore::default();
        mock_metastore
            .expect_close_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 4);
                let mut subrequests = request.subrequests;
                subrequests.sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

                for (i, subrequest) in subrequests.iter().enumerate() {
                    assert_eq!(subrequest.index_uid, "test-index:0");
                    assert_eq!(subrequest.source_id, "test-source");
                    assert_eq!(subrequest.shard_id, i as u64 + 1);
                    assert_eq!(subrequest.shard_state(), ShardState::Closed);
                }
                assert!(subrequests[0].replication_position_inclusive.is_none());
                assert_eq!(subrequests[1].replication_position_inclusive, Some(1));
                assert_eq!(subrequests[2].replication_position_inclusive, Some(1));
                assert_eq!(subrequests[3].replication_position_inclusive, Some(1));

                let response = CloseShardsResponse {
                    successes: vec![
                        CloseShardsSuccess {
                            index_uid: "test-index:0".to_string(),
                            source_id: "test-source".to_string(),
                            shard_id: 2,
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: Some("test-ingester-1".to_string()),
                            publish_position_inclusive: "1".to_string(),
                        },
                        CloseShardsSuccess {
                            index_uid: "test-index:0".to_string(),
                            source_id: "test-source".to_string(),
                            shard_id: 3,
                            leader_id: "test-ingester-1".to_string(),
                            follower_id: Some("test-ingester-0".to_string()),
                            publish_position_inclusive: "0".to_string(),
                        },
                        CloseShardsSuccess {
                            index_uid: "test-index:0".to_string(),
                            source_id: "test-source".to_string(),
                            shard_id: 4,
                            leader_id: "test-ingester-0".to_string(),
                            follower_id: Some("test-ingester-1".to_string()),
                            publish_position_inclusive: "".to_string(),
                        },
                    ],
                    failures: vec![CloseShardsFailure {
                        index_uid: "test-index:0".to_string(),
                        source_id: "test-source".to_string(),
                        shard_id: 1,
                        failure_kind: CloseShardsFailureKind::NotFound as i32,
                        failure_message: "shard not found".to_string(),
                    }],
                };
                Ok(response)
            });
        mock_metastore
            .expect_delete_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_ids, [2]);

                let response = DeleteShardsResponse {};
                Ok(response)
            });
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let wal_dir_path = tempdir.path();
        let replication_factor = 2;
        let mut ingester = Ingester::try_new(
            self_node_id.clone(),
            metastore,
            ingester_pool,
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        let mut state_guard = ingester.state.write().await;

        let queue_ids: Vec<QueueId> = (1..=4)
            .map(|shard_id| queue_id("test-index:0", "test-source", shard_id))
            .collect();

        for queue_id in &queue_ids {
            state_guard.mrecordlog.create_queue(queue_id).await.unwrap();
        }
        let records = [
            Bytes::from_static(b"test-doc-200"),
            Bytes::from_static(b"test-doc-201"),
        ];
        state_guard
            .mrecordlog
            .append_records(&queue_ids[1], None, records.into_iter())
            .await
            .unwrap();
        let records = [
            Bytes::from_static(b"test-doc-300"),
            Bytes::from_static(b"test-doc-301"),
        ];
        state_guard
            .mrecordlog
            .append_records(&queue_ids[2], None, records.into_iter())
            .await
            .unwrap();
        let records = [
            Bytes::from_static(b"test-doc-400"),
            Bytes::from_static(b"test-doc-401"),
        ];
        state_guard
            .mrecordlog
            .append_records(&queue_ids[3], None, records.into_iter())
            .await
            .unwrap();
        drop(state_guard);

        ingester.init().await.unwrap();

        let state_guard = ingester.state.read().await;
        assert!(!state_guard.mrecordlog.queue_exists(&queue_ids[0]));
        drop(state_guard);

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.primary_shards.len(), 2);
        assert_eq!(state_guard.replica_shards.len(), 1);

        let primary_shard_2 = state_guard.primary_shards.get(&queue_ids[1]).unwrap();
        assert_eq!(
            primary_shard_2.publish_position_inclusive,
            Position::Offset(1)
        );
        assert!(primary_shard_2.shard_state.is_closed());
        primary_shard_2.assert_positions(Some(1), NONE_REPLICA_POSITION);

        let primary_shard_4 = state_guard.primary_shards.get(&queue_ids[3]).unwrap();
        assert_eq!(
            primary_shard_4.publish_position_inclusive,
            Position::Beginning
        );
        assert!(primary_shard_4.shard_state.is_closed());
        primary_shard_2.assert_positions(Some(1), NONE_REPLICA_POSITION);

        let replica_shard_3 = state_guard.replica_shards.get(&queue_ids[2]).unwrap();
        assert_eq!(
            replica_shard_3.publish_position_inclusive,
            Position::Offset(0),
        );
        assert_eq!(
            replica_shard_3.replica_position_inclusive,
            Position::Offset(1),
        );
        assert!(replica_shard_3.shard_state.is_closed());

        drop(state_guard);

        // Wait for the removal task to run.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.primary_shards.len(), 1);
        assert!(!state_guard.primary_shards.contains_key(&queue_ids[1]));
        assert!(!state_guard.mrecordlog.queue_exists(&queue_ids[1]));
    }

    #[tokio::test]
    async fn test_ingester_persist() {
        let tempdir = tempfile::tempdir().unwrap();
        let self_node_id: NodeId = "test-ingester-0".into();
        let metastore = Arc::new(MockIngestMetastore::default());
        let ingester_pool = IngesterPool::default();
        let wal_dir_path = tempdir.path();
        let replication_factor = 1;
        let mut ingester = Ingester::try_new(
            self_node_id.clone(),
            metastore,
            ingester_pool,
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        let persist_request = PersistRequest {
            leader_id: self_node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 0,
                    follower_id: None,
                    doc_batch: None,
                },
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: None,
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-010"),
                        doc_lengths: vec![12],
                    }),
                },
                PersistSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 0,
                    follower_id: None,
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-100test-doc-101"),
                        doc_lengths: vec![12, 12],
                    }),
                },
            ],
        };
        ingester.persist(persist_request).await.unwrap();

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.primary_shards.len(), 3);

        let queue_id_00 = queue_id("test-index:0", "test-source", 0);
        let primary_shard_00 = state_guard.primary_shards.get(&queue_id_00).unwrap();
        primary_shard_00.assert_positions(None, NONE_REPLICA_POSITION);
        primary_shard_00.assert_is_open(None);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_00, .., &[]);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let primary_shard_01 = state_guard.primary_shards.get(&queue_id_01).unwrap();
        primary_shard_01.assert_positions(0, NONE_REPLICA_POSITION);
        primary_shard_01.assert_is_open(0);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, "test-doc-010")]);

        let queue_id_10 = queue_id("test-index:1", "test-source", 0);
        let primary_shard_10 = state_guard.primary_shards.get(&queue_id_10).unwrap();
        primary_shard_10.assert_positions(1, NONE_REPLICA_POSITION);
        primary_shard_10.assert_is_open(1);

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_10,
            ..,
            &[(0, "test-doc-100"), (1, "test-doc-101")],
        );
    }

    #[tokio::test]
    async fn test_ingester_open_replication_stream() {
        let tempdir = tempfile::tempdir().unwrap();
        let self_node_id: NodeId = "test-follower".into();
        let metastore = Arc::new(MockIngestMetastore::default());
        let ingester_pool = IngesterPool::default();
        let wal_dir_path = tempdir.path();
        let replication_factor = 1;
        let mut ingester = Ingester::try_new(
            self_node_id.clone(),
            metastore,
            ingester_pool,
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();
        let (syn_replication_stream_tx, syn_replication_stream) = ServiceStream::new_bounded(5);
        let open_stream_request = OpenReplicationStreamRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
        };
        let syn_replication_message = SynReplicationMessage::new_open_request(open_stream_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let mut ack_replication_stream = ingester
            .open_replication_stream(syn_replication_stream)
            .await
            .unwrap();
        ack_replication_stream
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_open_response()
            .unwrap();

        let state_guard = ingester.state.read().await;
        assert!(state_guard.replication_tasks.contains_key("test-leader"));
    }

    #[tokio::test]
    async fn test_ingester_persist_replicate() {
        let tempdir = tempfile::tempdir().unwrap();
        let leader_id: NodeId = "test-leader".into();
        let metastore = Arc::new(MockIngestMetastore::default());
        let ingester_pool = IngesterPool::default();
        let wal_dir_path = tempdir.path();
        let replication_factor = 2;
        let mut leader = Ingester::try_new(
            leader_id.clone(),
            metastore.clone(),
            ingester_pool.clone(),
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let follower_id: NodeId = "test-follower".into();
        let wal_dir_path = tempdir.path();
        let replication_factor = 2;
        let follower = Ingester::try_new(
            follower_id.clone(),
            metastore,
            ingester_pool.clone(),
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        ingester_pool.insert(
            follower_id.clone(),
            IngesterServiceClient::new(follower.clone()),
        );

        let persist_request = PersistRequest {
            leader_id: "test-leader".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 0,
                    follower_id: Some(follower_id.to_string()),
                    doc_batch: None,
                },
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: Some(follower_id.to_string()),
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-010"),
                        doc_lengths: vec![12],
                    }),
                },
                PersistSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 0,
                    follower_id: Some(follower_id.to_string()),
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-100test-doc-101"),
                        doc_lengths: vec![12, 12],
                    }),
                },
            ],
        };
        let persist_response = leader.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-leader");
        assert_eq!(persist_response.successes.len(), 3);
        assert_eq!(persist_response.failures.len(), 0);

        let leader_state_guard = leader.state.read().await;
        assert_eq!(leader_state_guard.primary_shards.len(), 3);

        let queue_id_00 = queue_id("test-index:0", "test-source", 0);
        let primary_shard_00 = leader_state_guard.primary_shards.get(&queue_id_00).unwrap();
        primary_shard_00.assert_positions(None, Some(None));
        primary_shard_00.assert_is_open(None);

        leader_state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_00, .., &[]);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let primary_shard_01 = leader_state_guard.primary_shards.get(&queue_id_01).unwrap();
        primary_shard_01.assert_positions(0, Some(0));
        primary_shard_01.assert_is_open(0);

        leader_state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, "test-doc-010")]);

        let queue_id_10 = queue_id("test-index:1", "test-source", 0);
        let primary_shard_10 = leader_state_guard.primary_shards.get(&queue_id_10).unwrap();
        primary_shard_10.assert_positions(1, Some(1));
        primary_shard_10.assert_is_open(1);

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_10,
            ..,
            &[(0, "test-doc-100"), (1, "test-doc-101")],
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_replicate_grpc() {
        let tempdir = tempfile::tempdir().unwrap();
        let leader_id: NodeId = "test-leader".into();
        let metastore = Arc::new(MockIngestMetastore::default());
        let ingester_pool = IngesterPool::default();
        let wal_dir_path = tempdir.path();
        let replication_factor = 2;
        let mut leader = Ingester::try_new(
            leader_id.clone(),
            metastore.clone(),
            ingester_pool.clone(),
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        let leader_grpc_server_adapter = IngesterServiceGrpcServerAdapter::new(leader.clone());
        let leader_grpc_server = IngesterServiceGrpcServer::new(leader_grpc_server_adapter);
        let leader_socket_addr: SocketAddr = "127.0.0.1:6666".parse().unwrap();

        tokio::spawn({
            async move {
                Server::builder()
                    .add_service(leader_grpc_server)
                    .serve(leader_socket_addr)
                    .await
                    .unwrap();
            }
        });

        let tempdir = tempfile::tempdir().unwrap();
        let follower_id: NodeId = "test-follower".into();
        let wal_dir_path = tempdir.path();
        let replication_factor = 2;
        let follower = Ingester::try_new(
            follower_id.clone(),
            metastore,
            ingester_pool.clone(),
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        let follower_grpc_server_adapter = IngesterServiceGrpcServerAdapter::new(follower.clone());
        let follower_grpc_server = IngesterServiceGrpcServer::new(follower_grpc_server_adapter);
        let follower_socket_addr: SocketAddr = "127.0.0.1:7777".parse().unwrap();

        tokio::spawn({
            async move {
                Server::builder()
                    .add_service(follower_grpc_server)
                    .serve(follower_socket_addr)
                    .await
                    .unwrap();
            }
        });
        let follower_channel = Timeout::new(
            Endpoint::from_static("http://127.0.0.1:7777").connect_lazy(),
            Duration::from_secs(1),
        );
        let follower_grpc_client = IngesterServiceClient::from_channel(follower_channel);

        ingester_pool.insert(follower_id.clone(), follower_grpc_client);

        let persist_request = PersistRequest {
            leader_id: "test-leader".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 0,
                    follower_id: Some(follower_id.to_string()),
                    doc_batch: None,
                },
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: Some(follower_id.to_string()),
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-010"),
                        doc_lengths: vec![12],
                    }),
                },
                PersistSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 0,
                    follower_id: Some(follower_id.to_string()),
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-100test-doc-101"),
                        doc_lengths: vec![12, 12],
                    }),
                },
            ],
        };
        let persist_response = leader.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-leader");
        assert_eq!(persist_response.successes.len(), 3);
        assert_eq!(persist_response.failures.len(), 0);

        let queue_id_00 = queue_id("test-index:0", "test-source", 0);

        let leader_state_guard = leader.state.read().await;
        leader_state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_00, .., &[]);

        let primary_shard = leader_state_guard.primary_shards.get(&queue_id_00).unwrap();
        primary_shard.assert_positions(None, Some(None));
        primary_shard.assert_is_open(None);

        let follower_state_guard = follower.state.read().await;
        assert!(!follower_state_guard.mrecordlog.queue_exists(&queue_id_00));

        assert!(!follower_state_guard
            .replica_shards
            .contains_key(&queue_id_00));

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);

        let leader_state_guard = leader.state.read().await;
        leader_state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, "test-doc-010")]);

        let primary_shard = leader_state_guard.primary_shards.get(&queue_id_01).unwrap();
        primary_shard.assert_positions(0, Some(0));
        primary_shard.assert_is_open(0);

        follower_state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, "test-doc-010")]);

        let replica_shard = follower_state_guard
            .replica_shards
            .get(&queue_id_01)
            .unwrap();
        replica_shard.assert_position(0);
        replica_shard.assert_is_open(0);

        let queue_id_10 = queue_id("test-index:1", "test-source", 0);

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_10,
            ..,
            &[(0, "test-doc-100"), (1, "test-doc-101")],
        );

        let primary_shard = leader_state_guard.primary_shards.get(&queue_id_10).unwrap();
        primary_shard.assert_positions(1, Some(1));
        primary_shard.assert_is_open(1);

        follower_state_guard.mrecordlog.assert_records_eq(
            &queue_id_10,
            ..,
            &[(0, "test-doc-100"), (1, "test-doc-101")],
        );

        let replica_shard = follower_state_guard
            .replica_shards
            .get(&queue_id_10)
            .unwrap();
        replica_shard.assert_position(1);
        replica_shard.assert_is_open(1);
    }

    #[tokio::test]
    async fn test_ingester_open_fetch_stream() {
        let tempdir = tempfile::tempdir().unwrap();
        let self_node_id: NodeId = "test-ingester-0".into();
        let metastore = Arc::new(MockIngestMetastore::default());
        let ingester_pool = IngesterPool::default();
        let wal_dir_path = tempdir.path();
        let replication_factor = 1;
        let mut ingester = Ingester::try_new(
            self_node_id.clone(),
            metastore,
            ingester_pool,
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        let persist_request = PersistRequest {
            leader_id: self_node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 0,
                    follower_id: None,
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-000"),
                        doc_lengths: vec![12],
                    }),
                },
                PersistSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: None,
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-010"),
                        doc_lengths: vec![12],
                    }),
                },
            ],
        };
        ingester.persist(persist_request).await.unwrap();

        let client_id = "test-client".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: None,
        };
        let mut fetch_stream = ingester
            .open_fetch_stream(open_fetch_stream_request)
            .await
            .unwrap();
        let fetch_response = fetch_stream.next().await.unwrap().unwrap();
        let doc_batch = fetch_response.doc_batch.unwrap();
        assert_eq!(doc_batch.doc_buffer, Bytes::from_static(b"test-doc-000"));
        assert_eq!(doc_batch.doc_lengths, [12]);
        assert_eq!(fetch_response.from_position_inclusive, 0);

        let persist_request = PersistRequest {
            leader_id: self_node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 0,
                follower_id: None,
                doc_batch: Some(DocBatchV2 {
                    doc_buffer: Bytes::from_static(b"test-doc-001test-doc-002"),
                    doc_lengths: vec![12, 12],
                }),
            }],
        };
        ingester.persist(persist_request).await.unwrap();

        let fetch_response = fetch_stream.next().await.unwrap().unwrap();
        let doc_batch = fetch_response.doc_batch.unwrap();
        assert_eq!(
            doc_batch.doc_buffer,
            Bytes::from_static(b"test-doc-001test-doc-002")
        );
        assert_eq!(doc_batch.doc_lengths, [12, 12]);
        assert_eq!(fetch_response.from_position_inclusive, 1);
    }

    #[tokio::test]
    async fn test_ingester_truncate() {
        let tempdir = tempfile::tempdir().unwrap();
        let self_node_id: NodeId = "test-ingester-0".into();
        let mut mock_metastore = MockIngestMetastore::default();
        mock_metastore
            .expect_delete_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);

                let subrequest = &request.subrequests[0];
                assert_eq!(subrequest.index_uid, "test-index:0");
                assert_eq!(subrequest.source_id, "test-source");
                assert_eq!(subrequest.shard_ids, [2]);

                let response = DeleteShardsResponse {};
                Ok(response)
            });
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let wal_dir_path = tempdir.path();
        let replication_factor = 1;
        let mut ingester = Ingester::try_new(
            self_node_id.clone(),
            metastore,
            ingester_pool,
            wal_dir_path,
            replication_factor,
        )
        .await
        .unwrap();

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let queue_id_02 = queue_id("test-index:0", "test-source", 2);

        let mut state_guard = ingester.state.write().await;
        ingester
            .init_primary_shard(&mut state_guard, &queue_id_01, &self_node_id, None)
            .await
            .unwrap();
        ingester
            .init_primary_shard(&mut state_guard, &queue_id_02, &self_node_id, None)
            .await
            .unwrap();

        state_guard
            .primary_shards
            .get_mut(&queue_id_02)
            .unwrap()
            .shard_state = ShardState::Closed;

        let records = [
            Bytes::from_static(b"test-doc-000"),
            Bytes::from_static(b"test-doc-001"),
        ]
        .into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_01, None, records)
            .await
            .unwrap();

        let records = [
            Bytes::from_static(b"test-doc-010"),
            Bytes::from_static(b"test-doc-011"),
        ]
        .into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_02, None, records)
            .await
            .unwrap();

        drop(state_guard);

        let truncate_request = TruncateRequest {
            ingester_id: self_node_id.to_string(),
            subrequests: vec![
                TruncateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    to_position_inclusive: 0,
                },
                TruncateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 2,
                    to_position_inclusive: 1,
                },
                TruncateSubrequest {
                    index_uid: "test-index:1337".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    to_position_inclusive: 1337,
                },
            ],
        };
        ingester.truncate(truncate_request).await.unwrap();

        let state_guard = ingester.state.read().await;
        state_guard
            .primary_shards
            .get(&queue_id_01)
            .unwrap()
            .assert_publish_position(0);
        state_guard
            .primary_shards
            .get(&queue_id_02)
            .unwrap()
            .assert_publish_position(1);

        let (position, record) = state_guard
            .mrecordlog
            .range(&queue_id_01, 0..)
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(position, 1);
        assert_eq!(&*record, b"test-doc-001");

        let record_opt = state_guard
            .mrecordlog
            .range(&queue_id_02, 0..)
            .unwrap()
            .next();
        assert!(record_opt.is_none());
        drop(state_guard);

        // Wait for the removal task to run.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.primary_shards.len(), 1);
        assert!(!state_guard.primary_shards.contains_key(&queue_id_02));

        assert!(!state_guard.mrecordlog.queue_exists(&queue_id_02));
    }
}
