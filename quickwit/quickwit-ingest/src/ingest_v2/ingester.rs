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
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytesize::ByteSize;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use mrecordlog::error::{CreateQueueError, DeleteQueueError, TruncateError};
use mrecordlog::MultiRecordLog;
use quickwit_cluster::Cluster;
use quickwit_common::pubsub::{EventBroker, EventSubscriber};
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::tower::Pool;
use quickwit_common::ServiceStream;
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::ingest::ingester::{
    AckReplicationMessage, CloseShardsRequest, CloseShardsResponse, DecommissionRequest,
    DecommissionResponse, FetchMessage, IngesterService, IngesterServiceClient,
    IngesterServiceStream, IngesterStatus, InitShardsRequest, InitShardsResponse,
    ObservationMessage, OpenFetchStreamRequest, OpenObservationStreamRequest,
    OpenReplicationStreamRequest, OpenReplicationStreamResponse, PersistFailure,
    PersistFailureReason, PersistRequest, PersistResponse, PersistSuccess, PingRequest,
    PingResponse, ReplicateFailureReason, ReplicateSubrequest, SynReplicationMessage,
    TruncateShardsRequest, TruncateShardsResponse,
};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error, IngestV2Result, Shard, ShardState};
use quickwit_proto::types::{queue_id, NodeId, Position, QueueId};
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, warn};

use super::fetch::FetchStreamTask;
use super::models::IngesterShard;
use super::mrecord::MRecord;
use super::mrecordlog_utils::{check_enough_capacity, force_delete_queue};
use super::rate_meter::RateMeter;
use super::replication::{
    ReplicationClient, ReplicationStreamTask, ReplicationStreamTaskHandle, ReplicationTask,
    ReplicationTaskHandle, SYN_REPLICATION_STREAM_CAPACITY,
};
use super::IngesterPool;
use crate::ingest_v2::broadcast::BroadcastLocalShardsTask;
use crate::ingest_v2::mrecordlog_utils::queue_position_range;
use crate::metrics::INGEST_METRICS;
use crate::{estimate_size, FollowerId, LeaderId};

/// Duration after which persist requests time out with
/// [`quickwit_proto::ingest::IngestV2Error::Timeout`].
pub(super) const PERSIST_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(6)
};

#[derive(Clone)]
pub struct Ingester {
    self_node_id: NodeId,
    ingester_pool: IngesterPool,
    state: Arc<RwLock<IngesterState>>,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    rate_limiter_settings: RateLimiterSettings,
    replication_factor: usize,
    observation_rx: watch::Receiver<IngestV2Result<ObservationMessage>>,
}

impl fmt::Debug for Ingester {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Ingester")
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

pub(super) struct IngesterState {
    pub mrecordlog: MultiRecordLog,
    pub shards: HashMap<QueueId, IngesterShard>,
    pub rate_trackers: HashMap<QueueId, (RateLimiter, RateMeter)>,
    // Replication stream opened with followers.
    pub replication_streams: HashMap<FollowerId, ReplicationStreamTaskHandle>,
    // Replication tasks running for each replication stream opened with leaders.
    pub replication_tasks: HashMap<LeaderId, ReplicationTaskHandle>,
    pub status: IngesterStatus,
    pub observation_tx: watch::Sender<IngestV2Result<ObservationMessage>>,
}

impl Ingester {
    pub async fn try_new(
        cluster: Cluster,
        ingester_pool: Pool<NodeId, IngesterServiceClient>,
        wal_dir_path: &Path,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
        replication_factor: usize,
    ) -> IngestV2Result<Self> {
        let self_node_id: NodeId = cluster.self_node_id().clone().into();
        let mrecordlog = MultiRecordLog::open_with_prefs(
            wal_dir_path,
            mrecordlog::SyncPolicy::OnDelay(Duration::from_secs(5)),
        )
        .await
        .map_err(|error| {
            let message = format!(
                "failed to create or open write-ahead log located at `{}`: {error}",
                wal_dir_path.display()
            );
            IngestV2Error::Internal(message)
        })?;
        let observe_message = ObservationMessage {
            node_id: self_node_id.clone().into(),
            status: IngesterStatus::Ready as i32,
        };
        let (observation_tx, observation_rx) = watch::channel(Ok(observe_message));

        let inner = IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            rate_trackers: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
            status: IngesterStatus::Ready,
            observation_tx,
        };
        let ingester = Self {
            self_node_id,
            ingester_pool,
            state: Arc::new(RwLock::new(inner)),
            disk_capacity,
            memory_capacity,
            rate_limiter_settings,
            replication_factor,
            observation_rx,
        };
        info!(
            replication_factor=%replication_factor,
            wal_dir=%wal_dir_path.display(),
            "spawning ingester"
        );
        ingester.init().await?;

        let weak_state = Arc::downgrade(&ingester.state);
        BroadcastLocalShardsTask::spawn(cluster, weak_state);

        Ok(ingester)
    }

    /// Checks whether the ingester is fully decommissioned and updates its status accordingly.
    fn check_decommissioning_status(&self, state: &mut IngesterState) {
        if state.status != IngesterStatus::Decommissioning {
            return;
        }
        if state.shards.values().all(|shard| shard.is_indexed()) {
            info!("ingester fully decommissioned");
            state.status = IngesterStatus::Decommissioned;

            state.observation_tx.send_if_modified(|observation_result| {
                if let Ok(observation) = observation_result {
                    observation.status = IngesterStatus::Decommissioned as i32;
                    return true;
                }
                false
            });
        }
    }

    /// During the initialization of the ingester, we list all the queues contained in
    /// the write-ahead log. Empty queues are deleted, while non-empty queues are recovered.
    /// However, the corresponding shards are closed and become read-only.
    async fn init(&self) -> IngestV2Result<()> {
        let mut state_guard = self.state.write().await;

        let queue_ids: Vec<QueueId> = state_guard
            .mrecordlog
            .list_queues()
            .map(|queue_id| queue_id.to_string())
            .collect();

        if !queue_ids.is_empty() {
            info!("recovering {} shard(s)", queue_ids.len());
        }
        let mut num_closed_shards = 0;
        let mut num_deleted_shards = 0;

        for queue_id in queue_ids {
            if let Some(position_range) = queue_position_range(&state_guard.mrecordlog, &queue_id) {
                // The queue is not empty: recover it.
                let replication_position_inclusive = Position::offset(*position_range.end());
                let truncation_position_inclusive = if *position_range.start() == 0 {
                    Position::Beginning
                } else {
                    Position::offset(*position_range.start() - 1)
                };
                let solo_shard = IngesterShard::new_solo(
                    ShardState::Closed,
                    replication_position_inclusive,
                    truncation_position_inclusive,
                );
                state_guard.shards.insert(queue_id.clone(), solo_shard);

                let rate_limiter = RateLimiter::from_settings(self.rate_limiter_settings);
                let rate_meter = RateMeter::default();
                state_guard
                    .rate_trackers
                    .insert(queue_id, (rate_limiter, rate_meter));

                num_closed_shards += 1;
            } else {
                // The queue is empty: delete it.
                force_delete_queue(&mut state_guard.mrecordlog, &queue_id)
                    .await
                    .expect("TODO: handle IO error");

                num_deleted_shards += 1;
            }
        }
        if num_closed_shards > 0 {
            info!("recovered and closed {num_closed_shards} shard(s)");
        }
        if num_deleted_shards > 0 {
            info!("deleted {num_deleted_shards} empty shard(s)");
        }
        Ok(())
    }

    /// Initializes a primary shard by creating a queue in the write-ahead log and inserting a new
    /// [`IngesterShard`] into the ingester state. If replication is enabled, this method will
    /// also:
    /// - open a replication stream between the leader and the follower if one does not already
    ///   exist.
    /// - initialize the replica shard.
    async fn init_primary_shard(
        &self,
        state: &mut IngesterState,
        shard: Shard,
    ) -> IngestV2Result<()> {
        let queue_id = shard.queue_id();

        let Entry::Vacant(entry) = state.shards.entry(queue_id.clone()) else {
            return Ok(());
        };
        match state.mrecordlog.create_queue(&queue_id).await {
            Ok(_) => {}
            Err(CreateQueueError::AlreadyExists) => panic!("queue should not exist"),
            Err(CreateQueueError::IoError(io_error)) => {
                // TODO: Close all shards and set readiness to false.
                error!(
                    "failed to create mrecordlog queue `{}`: {}",
                    queue_id, io_error
                );
                return Err(IngestV2Error::IngesterUnavailable {
                    ingester_id: shard.leader_id.into(),
                });
            }
        };
        let rate_limiter = RateLimiter::from_settings(self.rate_limiter_settings);
        let rate_meter = RateMeter::default();
        state
            .rate_trackers
            .insert(queue_id, (rate_limiter, rate_meter));

        let primary_shard = if let Some(follower_id) = &shard.follower_id {
            let leader_id: NodeId = shard.leader_id.clone().into();
            let follower_id: NodeId = follower_id.clone().into();

            let replication_client = self
                .init_replication_stream(
                    &mut state.replication_streams,
                    leader_id,
                    follower_id.clone(),
                )
                .await?;

            if let Err(error) = replication_client.init_replica(shard).await {
                error!("failed to initialize replica shard: {error}",);
                return Err(IngestV2Error::Internal(format!(
                    "failed to initialize replica shard: {error}"
                )));
            }
            IngesterShard::new_primary(
                follower_id,
                ShardState::Open,
                Position::Beginning,
                Position::Beginning,
            )
        } else {
            IngesterShard::new_solo(ShardState::Open, Position::Beginning, Position::Beginning)
        };
        entry.insert(primary_shard);
        Ok(())
    }

    async fn init_replication_stream(
        &self,
        replication_streams: &mut HashMap<FollowerId, ReplicationStreamTaskHandle>,
        leader_id: NodeId,
        follower_id: NodeId,
    ) -> IngestV2Result<ReplicationClient> {
        let entry = match replication_streams.entry(follower_id.clone()) {
            Entry::Occupied(entry) => {
                // A replication stream with this follower is already opened.
                return Ok(entry.get().replication_client());
            }
            Entry::Vacant(entry) => entry,
        };
        let open_request = OpenReplicationStreamRequest {
            leader_id: leader_id.clone().into(),
            follower_id: follower_id.clone().into(),
            replication_seqno: 0,
        };
        let open_message = SynReplicationMessage::new_open_request(open_request);
        let (syn_replication_stream_tx, syn_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        syn_replication_stream_tx
            .try_send(open_message)
            .expect("channel should be open and have capacity");

        let mut ingester =
            self.ingester_pool
                .get(&follower_id)
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
            .expect("TODO")
            .into_open_response()
            .expect("first message should be an open response");

        let replication_stream_task_handle = ReplicationStreamTask::spawn(
            leader_id.clone(),
            follower_id.clone(),
            syn_replication_stream_tx,
            ack_replication_stream,
        );
        let replication_client = replication_stream_task_handle.replication_client();
        entry.insert(replication_stream_task_handle);
        Ok(replication_client)
    }

    pub fn subscribe(&self, event_broker: &EventBroker) {
        let weak_ingester_state = WeakIngesterState(Arc::downgrade(&self.state));
        event_broker
            .subscribe::<ShardPositionsUpdate>(weak_ingester_state)
            .forever();
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
                "routing error: expected leader ID `{}`, got `{}`",
                self.self_node_id, persist_request.leader_id,
            )));
        }
        let mut persist_successes = Vec::with_capacity(persist_request.subrequests.len());
        let mut persist_failures = Vec::new();
        let mut replicate_subrequests: HashMap<NodeId, Vec<ReplicateSubrequest>> = HashMap::new();

        let commit_type = persist_request.commit_type();
        let force_commit = commit_type == CommitTypeV2::Force;
        let leader_id: NodeId = persist_request.leader_id.into();

        let mut state_guard = self.state.write().await;

        if state_guard.status != IngesterStatus::Ready {
            persist_failures.reserve_exact(persist_request.subrequests.len());

            for subrequest in persist_request.subrequests {
                let persist_failure = PersistFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: PersistFailureReason::ShardClosed as i32,
                };
                persist_failures.push(persist_failure);
            }
            let persist_response = PersistResponse {
                leader_id: leader_id.into(),
                successes: Vec::new(),
                failures: persist_failures,
            };
            return Ok(persist_response);
        }
        for subrequest in persist_request.subrequests {
            let queue_id = subrequest.queue_id();

            let Some(shard) = state_guard.shards.get_mut(&queue_id) else {
                let persist_failure = PersistFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: PersistFailureReason::ShardNotFound as i32,
                };
                persist_failures.push(persist_failure);
                continue;
            };
            if shard.shard_state.is_closed() {
                let persist_failure = PersistFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: PersistFailureReason::ShardClosed as i32,
                };
                persist_failures.push(persist_failure);
                continue;
            }
            let follower_id_opt = shard.follower_id_opt().cloned();
            let from_position_exclusive = shard.replication_position_inclusive.clone();

            let doc_batch = match subrequest.doc_batch {
                Some(doc_batch) if !doc_batch.is_empty() => doc_batch,
                _ => {
                    warn!("received empty persist request");

                    let persist_success = PersistSuccess {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        shard_id: subrequest.shard_id,
                        replication_position_inclusive: Some(
                            shard.replication_position_inclusive.clone(),
                        ),
                    };
                    persist_successes.push(persist_success);
                    continue;
                }
            };
            let requested_capacity = estimate_size(&doc_batch);

            if let Err(error) = check_enough_capacity(
                &state_guard.mrecordlog,
                self.disk_capacity,
                self.memory_capacity,
                requested_capacity,
            ) {
                warn!(
                    "failed to persist records to ingester `{}`: {error}",
                    self.self_node_id
                );
                let persist_failure = PersistFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: PersistFailureReason::ResourceExhausted as i32,
                };
                persist_failures.push(persist_failure);
                continue;
            }
            let (rate_limiter, rate_meter) = state_guard
                .rate_trackers
                .get_mut(&queue_id)
                .expect("rate limiter should be initialized");

            if !rate_limiter.acquire_bytes(requested_capacity) {
                debug!("failed to persist records to shard `{queue_id}`: rate limited");

                let persist_failure = PersistFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: PersistFailureReason::RateLimited as i32,
                };
                persist_failures.push(persist_failure);
                continue;
            }
            let batch_num_bytes = doc_batch.num_bytes() as u64;
            let batch_num_docs = doc_batch.num_docs() as u64;

            rate_meter.update(batch_num_bytes);

            let current_position_inclusive: Position = if force_commit {
                let encoded_mrecords = doc_batch
                    .docs()
                    .map(|doc| MRecord::Doc(doc).encode())
                    .chain(once(MRecord::Commit.encode()));
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, encoded_mrecords)
                    .await
                    .expect("TODO") // TODO: Io error, close shard?
            } else {
                let encoded_mrecords = doc_batch.docs().map(|doc| MRecord::Doc(doc).encode());
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, encoded_mrecords)
                    .await
                    .expect("TODO") // TODO: Io error, close shard?
            }
            .map(Position::offset)
            .expect("records should not be empty");

            INGEST_METRICS.ingested_num_bytes.inc_by(batch_num_bytes);
            INGEST_METRICS.ingested_num_docs.inc_by(batch_num_docs);

            state_guard
                .shards
                .get_mut(&queue_id)
                .expect("primary shard should exist")
                .set_replication_position_inclusive(current_position_inclusive.clone());

            if let Some(follower_id) = follower_id_opt {
                let replicate_subrequest = ReplicateSubrequest {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    from_position_exclusive: Some(from_position_exclusive),
                    to_position_inclusive: Some(current_position_inclusive),
                    doc_batch: Some(doc_batch),
                };
                replicate_subrequests
                    .entry(follower_id)
                    .or_default()
                    .push(replicate_subrequest);
            } else {
                let persist_success = PersistSuccess {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    replication_position_inclusive: Some(current_position_inclusive),
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
            let replication_client = state_guard
                .replication_streams
                .get(&follower_id)
                .expect("replication stream should be initialized")
                .replication_client();
            let leader_id = self.self_node_id.clone();
            let replicate_future =
                replication_client.replicate(leader_id, follower_id, subrequests, commit_type);
            replicate_futures.push(replicate_future);
        }
        // Drop the write lock AFTER pushing the replicate request into the replication client
        // channel to ensure that sequential writes in mrecordlog turn into sequential replicate
        // requests in the same order.
        drop(state_guard);

        while let Some(replication_result) = replicate_futures.next().await {
            let replicate_response = match replication_result {
                Ok(replicate_response) => replicate_response,
                Err(_) => {
                    // TODO: Handle replication error:
                    // 1. Close and evict all the shards hosted by the follower.
                    // 2. Close and evict the replication client.
                    // 3. Return `PersistFailureReason::ShardClosed` to router.
                    continue;
                }
            };
            for replicate_success in replicate_response.successes {
                let persist_success = PersistSuccess {
                    subrequest_id: replicate_success.subrequest_id,
                    index_uid: replicate_success.index_uid,
                    source_id: replicate_success.source_id,
                    shard_id: replicate_success.shard_id,
                    replication_position_inclusive: replicate_success
                        .replication_position_inclusive,
                };
                persist_successes.push(persist_success);
            }
            for replicate_failure in replicate_response.failures {
                // TODO: If the replica shard is closed, close the primary shard if it is not
                // already.
                let persist_failure_reason = match replicate_failure.reason() {
                    ReplicateFailureReason::Unspecified => PersistFailureReason::Unspecified,
                    ReplicateFailureReason::ShardNotFound => PersistFailureReason::ShardNotFound,
                    ReplicateFailureReason::ShardClosed => PersistFailureReason::ShardClosed,
                    ReplicateFailureReason::ResourceExhausted => {
                        PersistFailureReason::ResourceExhausted
                    }
                };
                let persist_failure = PersistFailure {
                    subrequest_id: replicate_failure.subrequest_id,
                    index_uid: replicate_failure.index_uid,
                    source_id: replicate_failure.source_id,
                    shard_id: replicate_failure.shard_id,
                    reason: persist_failure_reason as i32,
                };
                persist_failures.push(persist_failure);
            }
        }
        let leader_id = self.self_node_id.to_string();
        let persist_response = PersistResponse {
            leader_id,
            successes: persist_successes,
            failures: persist_failures,
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
            .expect("first message should be an open replication stream request");

        if open_replication_stream_request.follower_id != self.self_node_id {
            return Err(IngestV2Error::Internal("routing error".to_string()));
        }
        let leader_id: NodeId = open_replication_stream_request.leader_id.into();
        let follower_id: NodeId = open_replication_stream_request.follower_id.into();

        let mut state_guard = self.state.write().await;

        if state_guard.status != IngesterStatus::Ready {
            return Err(IngestV2Error::Internal("node decommissioned".to_string()));
        }
        let Entry::Vacant(entry) = state_guard.replication_tasks.entry(leader_id.clone()) else {
            return Err(IngestV2Error::Internal(format!(
                "a replication stream betwen {leader_id} and {follower_id} is already opened"
            )));
        };
        // Channel capacity: there is no need to bound the capacity of the channel here because it
        // is already virtually bounded by the capacity of the SYN replication stream.
        let (ack_replication_stream_tx, ack_replication_stream) = ServiceStream::new_unbounded();
        let open_response = OpenReplicationStreamResponse {
            replication_seqno: 0,
        };
        let ack_replication_message = AckReplicationMessage::new_open_response(open_response);
        ack_replication_stream_tx
            .send(Ok(ack_replication_message))
            .expect("channel should be open");

        let replication_task_handle = ReplicationTask::spawn(
            leader_id,
            follower_id,
            self.state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
            self.disk_capacity,
            self.memory_capacity,
        );
        entry.insert(replication_task_handle);
        Ok(ack_replication_stream)
    }

    async fn open_fetch_stream(
        &mut self,
        open_fetch_stream_request: OpenFetchStreamRequest,
    ) -> IngestV2Result<ServiceStream<IngestV2Result<FetchMessage>>> {
        let queue_id = open_fetch_stream_request.queue_id();
        let shard_status_rx = self
            .state
            .read()
            .await
            .shards
            .get(&queue_id)
            .ok_or_else(|| IngestV2Error::Internal("shard not found".to_string()))?
            .shard_status_rx
            .clone();
        let (service_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            self.state.clone(),
            shard_status_rx,
            FetchStreamTask::DEFAULT_BATCH_NUM_BYTES,
        );
        Ok(service_stream)
    }

    async fn ping(&mut self, ping_request: PingRequest) -> IngestV2Result<PingResponse> {
        let state_guard = self.state.read().await;

        if state_guard.status != IngesterStatus::Ready {
            return Err(IngestV2Error::Internal("node decommissioned".to_string()));
        }
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

    async fn init_shards(
        &mut self,
        init_shards_request: InitShardsRequest,
    ) -> IngestV2Result<InitShardsResponse> {
        let mut state_guard = self.state.write().await;

        if state_guard.status != IngesterStatus::Ready {
            return Err(IngestV2Error::Internal("node decommissioned".to_string()));
        }

        for shard in init_shards_request.shards {
            self.init_primary_shard(&mut state_guard, shard).await?;
        }
        Ok(InitShardsResponse {})
    }

    async fn truncate_shards(
        &mut self,
        truncate_shards_request: TruncateShardsRequest,
    ) -> IngestV2Result<TruncateShardsResponse> {
        if truncate_shards_request.ingester_id != self.self_node_id {
            return Err(IngestV2Error::Internal(format!(
                "routing error: expected ingester `{}`, got `{}`",
                self.self_node_id, truncate_shards_request.ingester_id,
            )));
        }
        let mut state_guard = self.state.write().await;

        for subrequest in truncate_shards_request.subrequests {
            let queue_id = subrequest.queue_id();
            let truncate_up_to_position_inclusive = subrequest.truncate_up_to_position_inclusive();

            if truncate_up_to_position_inclusive.is_eof() {
                state_guard.delete_shard(&queue_id).await;
            } else {
                state_guard
                    .truncate_shard(&queue_id, truncate_up_to_position_inclusive)
                    .await;
            }
        }
        self.check_decommissioning_status(&mut state_guard);
        let truncate_response = TruncateShardsResponse {};
        Ok(truncate_response)
    }

    async fn close_shards(
        &mut self,
        close_shards_request: CloseShardsRequest,
    ) -> IngestV2Result<CloseShardsResponse> {
        let mut state_guard = self.state.write().await;

        for shard_ids in close_shards_request.shards {
            for queue_id in shard_ids.queue_ids() {
                if let Some(shard) = state_guard.shards.get_mut(&queue_id) {
                    shard.shard_state = ShardState::Closed;
                    shard.notify_shard_status();
                }
            }
        }
        Ok(CloseShardsResponse {})
    }

    async fn decommission(
        &mut self,
        _decommission_request: DecommissionRequest,
    ) -> IngestV2Result<DecommissionResponse> {
        info!("decommissioning ingester");
        let mut state_guard = self.state.write().await;

        for shard in state_guard.shards.values_mut() {
            shard.shard_state = ShardState::Closed;
            shard.notify_shard_status();
        }
        state_guard.status = IngesterStatus::Decommissioning;
        self.check_decommissioning_status(&mut state_guard);

        Ok(DecommissionResponse {})
    }

    async fn open_observation_stream(
        &mut self,
        _open_observation_stream_request: OpenObservationStreamRequest,
    ) -> IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        let observation_stream = self.observation_rx.clone().into();
        Ok(observation_stream)
    }
}

impl IngesterState {
    /// Truncates the shard identified by `queue_id` up to `truncate_up_to_position_inclusive` only
    /// if the current truncation position of the shard is smaller.
    async fn truncate_shard(
        &mut self,
        queue_id: &QueueId,
        truncate_up_to_position_inclusive: Position,
    ) {
        // TODO: Replace with if-let-chains when stabilized.
        let Some(truncate_up_to_offset_inclusive) = truncate_up_to_position_inclusive.as_u64()
        else {
            return;
        };
        let Some(shard) = self.shards.get_mut(queue_id) else {
            return;
        };
        if shard.truncation_position_inclusive >= truncate_up_to_position_inclusive {
            return;
        }
        match self
            .mrecordlog
            .truncate(queue_id, truncate_up_to_offset_inclusive)
            .await
        {
            Ok(_) => {
                shard.truncation_position_inclusive = truncate_up_to_position_inclusive;
            }
            Err(TruncateError::MissingQueue(_)) => {
                warn!("failed to truncate WAL queue `{queue_id}`: queue does not exist");
            }
            Err(error) => {
                error!(%error, "failed to truncate WAL queue `{queue_id}`");
            }
        };
    }

    /// Deletes the shard identified by `queue_id` from the ingester state. It removes the
    /// mrecordlog queue first and then, if the operation is successful, removes the shard.
    async fn delete_shard(&mut self, queue_id: &QueueId) {
        match self.mrecordlog.delete_queue(queue_id).await {
            Ok(_) => {
                self.shards.remove(queue_id);
                self.rate_trackers.remove(queue_id);
            }
            Err(DeleteQueueError::MissingQueue(_)) => {
                // The shard has already been deleted.
            }
            Err(DeleteQueueError::IoError(_)) => {
                panic!("TODO: handle IO error")
            }
        };
    }
}

struct WeakIngesterState(Weak<RwLock<IngesterState>>);

#[async_trait]
impl EventSubscriber<ShardPositionsUpdate> for WeakIngesterState {
    async fn handle_event(&mut self, shard_positions_update: ShardPositionsUpdate) {
        let Some(state) = self.0.upgrade() else {
            return;
        };
        let mut state_guard = state.write().await;

        let index_uid = shard_positions_update.source_uid.index_uid;
        let source_id = shard_positions_update.source_uid.source_id;

        for (shard_id, shard_position) in shard_positions_update.shard_positions {
            let queue_id = queue_id(index_uid.as_str(), &source_id, shard_id);

            if shard_position.is_eof() {
                state_guard.delete_shard(&queue_id).await;
            } else {
                state_guard.truncate_shard(&queue_id, shard_position).await;
            }
        }
    }
}

pub async fn wait_for_ingester_decommission(ingester_opt: Option<IngesterServiceClient>) {
    let Some(mut ingester) = ingester_opt else {
        return;
    };
    if let Err(error) = ingester.decommission(DecommissionRequest {}).await {
        error!("failed to initiate ingester decommission: {error}");
        return;
    }
    let mut observation_stream = match ingester
        .open_observation_stream(OpenObservationStreamRequest {})
        .await
    {
        Ok(observation_stream) => observation_stream,
        Err(error) => {
            error!("failed to open observation stream: {error}");
            return;
        }
    };
    while let Some(observation_message_result) = observation_stream.next().await {
        let observation_message = match observation_message_result {
            Ok(observation_message) => observation_message,
            Err(error) => {
                error!("observation stream ended unexpectedly: {error}");
                return;
            }
        };
        if observation_message.status() == IngesterStatus::Decommissioned {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, Ordering};

    use bytes::Bytes;
    use quickwit_cluster::{create_cluster_for_test_with_id, ChannelTransport};
    use quickwit_common::shared_consts::INGESTER_PRIMARY_SHARDS_PREFIX;
    use quickwit_common::tower::ConstantRate;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::ingest::ingester::{
        IngesterServiceGrpcServer, IngesterServiceGrpcServerAdapter, PersistSubrequest,
        TruncateShardsSubrequest,
    };
    use quickwit_proto::ingest::{DocBatchV2, ShardIds};
    use quickwit_proto::types::{queue_id, SourceUid};
    use tokio::task::yield_now;
    use tonic::transport::{Endpoint, Server};

    use super::*;
    use crate::ingest_v2::broadcast::ShardInfos;
    use crate::ingest_v2::fetch::tests::{into_fetch_eof, into_fetch_payload};
    use crate::ingest_v2::test_utils::MultiRecordLogTestExt;

    pub(super) struct IngesterForTest {
        node_id: NodeId,
        ingester_pool: IngesterPool,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
        replication_factor: usize,
    }

    impl Default for IngesterForTest {
        fn default() -> Self {
            Self {
                node_id: "test-ingester".into(),
                ingester_pool: IngesterPool::default(),
                disk_capacity: ByteSize::mb(256),
                memory_capacity: ByteSize::mb(1),
                rate_limiter_settings: RateLimiterSettings::default(),
                replication_factor: 1,
            }
        }
    }

    impl IngesterForTest {
        pub fn with_node_id(mut self, node_id: &str) -> Self {
            self.node_id = node_id.into();
            self
        }

        pub fn with_ingester_pool(mut self, ingester_pool: &IngesterPool) -> Self {
            self.ingester_pool = ingester_pool.clone();
            self
        }

        pub fn with_disk_capacity(mut self, disk_capacity: ByteSize) -> Self {
            self.disk_capacity = disk_capacity;
            self
        }

        pub fn with_rate_limiter_settings(
            mut self,
            rate_limiter_settings: RateLimiterSettings,
        ) -> Self {
            self.rate_limiter_settings = rate_limiter_settings;
            self
        }

        pub fn with_replication(mut self) -> Self {
            self.replication_factor = 2;
            self
        }

        pub async fn build(self) -> (IngesterContext, Ingester) {
            static GOSSIP_ADVERTISE_PORT_SEQUENCE: AtomicU16 = AtomicU16::new(1u16);

            let tempdir = tempfile::tempdir().unwrap();
            let wal_dir_path = tempdir.path();
            let transport = ChannelTransport::default();

            let gossip_advertise_port =
                GOSSIP_ADVERTISE_PORT_SEQUENCE.fetch_add(1, Ordering::Relaxed);

            let cluster = create_cluster_for_test_with_id(
                self.node_id.clone(),
                gossip_advertise_port,
                "test-cluster".to_string(),
                Vec::new(),
                &HashSet::from_iter([QuickwitService::Indexer]),
                &transport,
                true,
            )
            .await
            .unwrap();

            let ingester = Ingester::try_new(
                cluster.clone(),
                self.ingester_pool.clone(),
                wal_dir_path,
                self.disk_capacity,
                self.memory_capacity,
                self.rate_limiter_settings,
                self.replication_factor,
            )
            .await
            .unwrap();

            let ingester_env = IngesterContext {
                _tempdir: tempdir,
                _transport: transport,
                node_id: self.node_id,
                cluster,
                ingester_pool: self.ingester_pool,
            };
            (ingester_env, ingester)
        }
    }

    pub struct IngesterContext {
        _tempdir: tempfile::TempDir,
        _transport: ChannelTransport,
        node_id: NodeId,
        cluster: Cluster,
        ingester_pool: IngesterPool,
    }

    #[tokio::test]
    async fn test_ingester_init() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let mut state_guard = ingester.state.write().await;

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        state_guard
            .mrecordlog
            .create_queue(&queue_id_01)
            .await
            .unwrap();

        let records = [MRecord::new_doc("test-doc-foo").encode()].into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_01, None, records)
            .await
            .unwrap();

        state_guard
            .mrecordlog
            .truncate(&queue_id_01, 0)
            .await
            .unwrap();

        let queue_id_02 = queue_id("test-index:0", "test-source", 2);
        state_guard
            .mrecordlog
            .create_queue(&queue_id_02)
            .await
            .unwrap();

        let records = [
            MRecord::new_doc("test-doc-foo").encode(),
            MRecord::new_doc("test-doc-bar").encode(),
        ]
        .into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_02, None, records)
            .await
            .unwrap();

        state_guard
            .mrecordlog
            .truncate(&queue_id_02, 0)
            .await
            .unwrap();

        let queue_id_03 = queue_id("test-index:0", "test-source", 3);
        state_guard
            .mrecordlog
            .create_queue(&queue_id_03)
            .await
            .unwrap();

        drop(state_guard);

        ingester.init().await.unwrap();

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.shards.len(), 1);

        let solo_shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        solo_shard_02.assert_is_solo();
        solo_shard_02.assert_is_closed();
        solo_shard_02.assert_replication_position(Position::offset(1u64));
        solo_shard_02.assert_truncation_position(Position::offset(0u64));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_02, .., &[(1, "\0\0test-doc-bar")]);

        state_guard.rate_trackers.contains_key(&queue_id_02);
    }

    #[tokio::test]
    async fn test_ingester_broadcasts_local_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.write().await;

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let shard =
            IngesterShard::new_solo(ShardState::Open, Position::Beginning, Position::Beginning);
        state_guard.shards.insert(queue_id_01.clone(), shard);

        let rate_limiter = RateLimiter::from_settings(RateLimiterSettings::default());
        let rate_meter = RateMeter::default();
        state_guard
            .rate_trackers
            .insert(queue_id_01.clone(), (rate_limiter, rate_meter));

        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let key = format!(
            "{INGESTER_PRIMARY_SHARDS_PREFIX}{}:{}",
            "test-index:0", "test-source"
        );
        let value = ingester_ctx.cluster.get_self_key_value(&key).await.unwrap();

        let shard_infos: ShardInfos = serde_json::from_str(&value).unwrap();
        assert_eq!(shard_infos.len(), 1);

        let shard_info = shard_infos.iter().next().unwrap();
        assert_eq!(shard_info.shard_id, 1);
        assert_eq!(shard_info.shard_state, ShardState::Open);
        assert_eq!(shard_info.ingestion_rate, 0);

        let mut state_guard = ingester.state.write().await;
        state_guard
            .shards
            .get_mut(&queue_id_01)
            .unwrap()
            .shard_state = ShardState::Closed;
        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let value = ingester_ctx.cluster.get_self_key_value(&key).await.unwrap();

        let shard_infos: ShardInfos = serde_json::from_str(&value).unwrap();
        assert_eq!(shard_infos.len(), 1);

        let shard_info = shard_infos.iter().next().unwrap();
        assert_eq!(shard_info.shard_state, ShardState::Closed);

        let mut state_guard = ingester.state.write().await;
        state_guard.shards.remove(&queue_id_01).unwrap();
        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let value_opt = ingester_ctx.cluster.get_self_key_value(&key).await;
        assert!(value_opt.is_none());
    }

    #[tokio::test]
    async fn test_ingester_persist() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let init_shards_request = InitShardsRequest {
            shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    ..Default::default()
                },
            ],
        };
        ingester.init_shards(init_shards_request).await.unwrap();

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![
                PersistSubrequest {
                    subrequest_id: 0,
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: None,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: None,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-110", "test-doc-111"])),
                },
            ],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 2);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success_0 = &persist_response.successes[0];
        assert_eq!(persist_success_0.subrequest_id, 0);
        assert_eq!(persist_success_0.index_uid, "test-index:0");
        assert_eq!(persist_success_0.source_id, "test-source");
        assert_eq!(persist_success_0.shard_id, 1);
        assert_eq!(
            persist_success_0.replication_position_inclusive,
            Some(Position::offset(1u64))
        );

        let persist_success_1 = &persist_response.successes[1];
        assert_eq!(persist_success_1.subrequest_id, 1);
        assert_eq!(persist_success_1.index_uid, "test-index:1");
        assert_eq!(persist_success_1.source_id, "test-source");
        assert_eq!(persist_success_1.shard_id, 1);
        assert_eq!(
            persist_success_1.replication_position_inclusive,
            Some(Position::offset(2u64))
        );

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.shards.len(), 2);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let solo_shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        solo_shard_01.assert_is_solo();
        solo_shard_01.assert_is_open();
        solo_shard_01.assert_replication_position(Position::offset(1u64));

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010"), (1, "\0\x01")],
        );

        let queue_id_11 = queue_id("test-index:1", "test-source", 1);
        let solo_shard_11 = state_guard.shards.get(&queue_id_11).unwrap();
        solo_shard_11.assert_is_solo();
        solo_shard_11.assert_is_open();
        solo_shard_11.assert_replication_position(Position::offset(2u64));

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[
                (0, "\0\0test-doc-110"),
                (1, "\0\0test-doc-111"),
                (2, "\0\x01"),
            ],
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_replicate() {
        let (leader_ctx, mut leader) = IngesterForTest::default()
            .with_node_id("test-leader")
            .with_replication()
            .build()
            .await;

        let (follower_ctx, follower) = IngesterForTest::default()
            .with_node_id("test-follower")
            .with_ingester_pool(&leader_ctx.ingester_pool)
            .with_replication()
            .build()
            .await;

        leader_ctx.ingester_pool.insert(
            follower_ctx.node_id.clone(),
            IngesterServiceClient::new(follower.clone()),
        );

        let init_shards_request = InitShardsRequest {
            shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    shard_state: ShardState::Open as i32,
                    leader_id: leader_ctx.node_id.to_string(),
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    shard_state: ShardState::Open as i32,
                    leader_id: leader_ctx.node_id.to_string(),
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    ..Default::default()
                },
            ],
        };
        leader.init_shards(init_shards_request).await.unwrap();

        let persist_request = PersistRequest {
            leader_id: "test-leader".to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![
                PersistSubrequest {
                    subrequest_id: 0,
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-110", "test-doc-111"])),
                },
            ],
        };
        let persist_response = leader.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-leader");
        assert_eq!(persist_response.successes.len(), 2);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success_0 = &persist_response.successes[0];
        assert_eq!(persist_success_0.subrequest_id, 0);
        assert_eq!(persist_success_0.index_uid, "test-index:0");
        assert_eq!(persist_success_0.source_id, "test-source");
        assert_eq!(persist_success_0.shard_id, 1);
        assert_eq!(
            persist_success_0.replication_position_inclusive,
            Some(Position::offset(1u64))
        );

        let persist_success_1 = &persist_response.successes[1];
        assert_eq!(persist_success_1.subrequest_id, 1);
        assert_eq!(persist_success_1.index_uid, "test-index:1");
        assert_eq!(persist_success_1.source_id, "test-source");
        assert_eq!(persist_success_1.shard_id, 1);
        assert_eq!(
            persist_success_1.replication_position_inclusive,
            Some(Position::offset(2u64))
        );

        let leader_state_guard = leader.state.read().await;
        assert_eq!(leader_state_guard.shards.len(), 2);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let primary_shard_01 = leader_state_guard.shards.get(&queue_id_01).unwrap();
        primary_shard_01.assert_is_primary();
        primary_shard_01.assert_is_open();
        primary_shard_01.assert_replication_position(Position::offset(1u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010"), (1, "\0\x01")],
        );

        let queue_id_11 = queue_id("test-index:1", "test-source", 1);
        let primary_shard_11 = leader_state_guard.shards.get(&queue_id_11).unwrap();
        primary_shard_11.assert_is_primary();
        primary_shard_11.assert_is_open();
        primary_shard_11.assert_replication_position(Position::offset(2u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[
                (0, "\0\0test-doc-110"),
                (1, "\0\0test-doc-111"),
                (2, "\0\x01"),
            ],
        );

        let follower_state_guard = follower.state.read().await;
        assert_eq!(follower_state_guard.shards.len(), 2);

        let replica_shard_01 = follower_state_guard.shards.get(&queue_id_01).unwrap();
        replica_shard_01.assert_is_replica();
        replica_shard_01.assert_is_open();
        replica_shard_01.assert_replication_position(Position::offset(1u64));

        follower_state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010"), (1, "\0\x01")],
        );

        let replica_shard_11 = follower_state_guard.shards.get(&queue_id_11).unwrap();
        replica_shard_11.assert_is_replica();
        replica_shard_11.assert_is_open();
        replica_shard_11.assert_replication_position(Position::offset(2u64));

        follower_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[
                (0, "\0\0test-doc-110"),
                (1, "\0\0test-doc-111"),
                (2, "\0\x01"),
            ],
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_replicate_grpc() {
        let (leader_ctx, mut leader) = IngesterForTest::default()
            .with_node_id("test-leader")
            .with_replication()
            .build()
            .await;

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

        let (follower_ctx, follower) = IngesterForTest::default()
            .with_node_id("test-follower")
            .with_ingester_pool(&leader_ctx.ingester_pool)
            .with_replication()
            .build()
            .await;

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
        let follower_channel = Endpoint::from_static("http://127.0.0.1:7777").connect_lazy();
        let follower_grpc_client = IngesterServiceClient::from_channel(
            "127.0.0.1:7777".parse().unwrap(),
            follower_channel,
        );

        leader_ctx
            .ingester_pool
            .insert(follower_ctx.node_id.clone(), follower_grpc_client);

        let init_shards_request = InitShardsRequest {
            shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    shard_state: ShardState::Open as i32,
                    leader_id: leader_ctx.node_id.to_string(),
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    shard_state: ShardState::Open as i32,
                    leader_id: leader_ctx.node_id.to_string(),
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    ..Default::default()
                },
            ],
        };
        leader.init_shards(init_shards_request).await.unwrap();

        let persist_request = PersistRequest {
            leader_id: "test-leader".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                PersistSubrequest {
                    subrequest_id: 0,
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-110", "test-doc-111"])),
                },
            ],
        };
        let persist_response = leader.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-leader");
        assert_eq!(persist_response.successes.len(), 2);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success_0 = &persist_response.successes[0];
        assert_eq!(persist_success_0.subrequest_id, 0);
        assert_eq!(persist_success_0.index_uid, "test-index:0");
        assert_eq!(persist_success_0.source_id, "test-source");
        assert_eq!(persist_success_0.shard_id, 1);
        assert_eq!(
            persist_success_0.replication_position_inclusive,
            Some(Position::offset(0u64))
        );

        let persist_success_1 = &persist_response.successes[1];
        assert_eq!(persist_success_1.subrequest_id, 1);
        assert_eq!(persist_success_1.index_uid, "test-index:1");
        assert_eq!(persist_success_1.source_id, "test-source");
        assert_eq!(persist_success_1.shard_id, 1);
        assert_eq!(
            persist_success_1.replication_position_inclusive,
            Some(Position::offset(1u64))
        );

        let leader_state_guard = leader.state.read().await;
        assert_eq!(leader_state_guard.shards.len(), 2);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let primary_shard_01 = leader_state_guard.shards.get(&queue_id_01).unwrap();
        primary_shard_01.assert_is_primary();
        primary_shard_01.assert_is_open();
        primary_shard_01.assert_replication_position(Position::offset(0u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010")],
        );

        let queue_id_11 = queue_id("test-index:1", "test-source", 1);
        let primary_shard_11 = leader_state_guard.shards.get(&queue_id_11).unwrap();
        primary_shard_11.assert_is_primary();
        primary_shard_11.assert_is_open();
        primary_shard_11.assert_replication_position(Position::offset(1u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[(0, "\0\0test-doc-110"), (1, "\0\0test-doc-111")],
        );

        let follower_state_guard = follower.state.read().await;
        assert_eq!(follower_state_guard.shards.len(), 2);

        let replica_shard_01 = follower_state_guard.shards.get(&queue_id_01).unwrap();
        replica_shard_01.assert_is_replica();
        replica_shard_01.assert_is_open();
        replica_shard_01.assert_replication_position(Position::offset(0u64));

        follower_state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010")],
        );

        let replica_shard_11 = follower_state_guard.shards.get(&queue_id_11).unwrap();
        replica_shard_11.assert_is_replica();
        replica_shard_11.assert_is_open();
        replica_shard_11.assert_replication_position(Position::offset(1u64));

        follower_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[(0, "\0\0test-doc-110"), (1, "\0\0test-doc-111")],
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_shard_closed() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;
        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let solo_shard =
            IngesterShard::new_solo(ShardState::Closed, Position::Beginning, Position::Beginning);
        ingester
            .state
            .write()
            .await
            .shards
            .insert(queue_id_01.clone(), solo_shard);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                follower_id: None,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid, "test-index:0");
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id, 1);
        assert_eq!(persist_failure.reason(), PersistFailureReason::ShardClosed);

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.shards.len(), 1);

        let solo_shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        solo_shard_01.assert_is_solo();
        solo_shard_01.assert_is_closed();
        solo_shard_01.assert_replication_position(Position::Beginning);
    }

    #[tokio::test]
    async fn test_ingester_persist_rate_limited() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default()
            .with_rate_limiter_settings(RateLimiterSettings {
                burst_limit: 0,
                rate_limit: ConstantRate::bytes_per_sec(ByteSize(0)),
                refill_period: Duration::from_millis(100),
            })
            .build()
            .await;

        let mut state_guard = ingester.state.write().await;

        let primary_shard = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            ..Default::default()
        };
        ingester
            .init_primary_shard(&mut state_guard, primary_shard)
            .await
            .unwrap();

        drop(state_guard);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                follower_id: None,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid, "test-index:0");
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id, 1);
        assert_eq!(persist_failure.reason(), PersistFailureReason::RateLimited);

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.shards.len(), 1);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);

        let solo_shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        solo_shard_01.assert_is_solo();
        solo_shard_01.assert_is_open();
        solo_shard_01.assert_replication_position(Position::Beginning);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[]);
    }

    #[tokio::test]
    async fn test_ingester_persist_resource_exhausted() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default()
            .with_disk_capacity(ByteSize(0))
            .build()
            .await;

        let mut state_guard = ingester.state.write().await;

        let primary_shard = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            ..Default::default()
        };
        ingester
            .init_primary_shard(&mut state_guard, primary_shard)
            .await
            .unwrap();

        drop(state_guard);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                follower_id: None,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid, "test-index:0");
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id, 1);
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::ResourceExhausted
        );

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.shards.len(), 1);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let solo_shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        solo_shard_01.assert_is_solo();
        solo_shard_01.assert_is_open();
        solo_shard_01.assert_replication_position(Position::Beginning);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[]);
    }
    #[tokio::test]

    async fn test_ingester_open_replication_stream() {
        let (_ingester_ctx, mut ingester) = IngesterForTest::default()
            .with_node_id("test-follower")
            .build()
            .await;

        let (syn_replication_stream_tx, syn_replication_stream) = ServiceStream::new_bounded(5);
        let open_stream_request = OpenReplicationStreamRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            replication_seqno: 0,
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
    async fn test_ingester_open_fetch_stream() {
        let (_ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let shard = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id = queue_id("test-index:0", "test-source", 1);

        let mut state_guard = ingester.state.write().await;

        ingester
            .init_primary_shard(&mut state_guard, shard)
            .await
            .unwrap();

        let records = [MRecord::new_doc("test-doc-foo").encode()].into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id, None, records)
            .await
            .unwrap();

        drop(state_guard);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: "test-client".to_string(),
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            from_position_exclusive: None,
        };
        let mut fetch_stream = ingester
            .open_fetch_stream(open_fetch_stream_request)
            .await
            .unwrap();

        let fetch_response = fetch_stream.next().await.unwrap().unwrap();
        let fetch_payload = into_fetch_payload(fetch_response);

        assert_eq!(fetch_payload.from_position_exclusive(), Position::Beginning);
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(0u64)
        );

        let mrecord_batch = fetch_payload.mrecord_batch.unwrap();
        assert_eq!(
            mrecord_batch.mrecord_buffer,
            Bytes::from_static(b"\0\0test-doc-foo")
        );
        assert_eq!(mrecord_batch.mrecord_lengths, [14]);

        let mut state_guard = ingester.state.write().await;

        let records = [MRecord::new_doc("test-doc-bar").encode()].into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id, None, records)
            .await
            .unwrap();

        state_guard
            .shards
            .get(&queue_id)
            .unwrap()
            .notify_shard_status();

        drop(state_guard);

        let fetch_response = fetch_stream.next().await.unwrap().unwrap();
        let fetch_payload = into_fetch_payload(fetch_response);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );

        let mrecord_batch = fetch_payload.mrecord_batch.unwrap();
        assert_eq!(
            mrecord_batch.mrecord_buffer,
            Bytes::from_static(b"\0\0test-doc-bar")
        );
        assert_eq!(mrecord_batch.mrecord_lengths, [14]);
    }

    #[tokio::test]
    async fn test_ingester_truncate() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let shard_01 = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id_01 = queue_id("test-index:0", "test-source", 1);

        let shard_02 = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 2,
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let queue_id_02 = queue_id("test-index:0", "test-source", 2);

        let mut state_guard = ingester.state.write().await;

        ingester
            .init_primary_shard(&mut state_guard, shard_01)
            .await
            .unwrap();
        ingester
            .init_primary_shard(&mut state_guard, shard_02)
            .await
            .unwrap();

        let records = [
            MRecord::new_doc("test-doc-foo").encode(),
            MRecord::new_doc("test-doc-bar").encode(),
        ]
        .into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_01, None, records)
            .await
            .unwrap();

        let records = [MRecord::new_doc("test-doc-baz").encode()].into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_02, None, records)
            .await
            .unwrap();

        drop(state_guard);

        let truncate_shards_request = TruncateShardsRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            subrequests: vec![
                TruncateShardsSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    truncate_up_to_position_inclusive: Some(Position::offset(0u64)),
                },
                TruncateShardsSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 2,
                    truncate_up_to_position_inclusive: Some(Position::eof(0u64)),
                },
                TruncateShardsSubrequest {
                    index_uid: "test-index:1337".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1337,
                    truncate_up_to_position_inclusive: Some(Position::offset(1337u64)),
                },
            ],
        };
        ingester
            .truncate_shards(truncate_shards_request.clone())
            .await
            .unwrap();

        // Verify idempotency.
        ingester
            .truncate_shards(truncate_shards_request.clone())
            .await
            .unwrap();

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.shards.len(), 1);

        assert!(state_guard.shards.contains_key(&queue_id_01));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(1, "\0\0test-doc-bar")]);

        assert!(!state_guard.shards.contains_key(&queue_id_02));
        assert!(!state_guard.mrecordlog.queue_exists(&queue_id_02));
    }

    #[tokio::test]
    async fn test_ingester_close_shards() {
        let (_ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let shard = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id = queue_id("test-index:0", "test-source", 1);

        let mut state_guard = ingester.state.write().await;
        ingester
            .init_primary_shard(&mut state_guard, shard)
            .await
            .unwrap();
        drop(state_guard);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: "test-client".to_string(),
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            from_position_exclusive: None,
        };
        let mut fetch_stream = ingester
            .open_fetch_stream(open_fetch_stream_request)
            .await
            .unwrap();

        let close_shards_request = CloseShardsRequest {
            shards: vec![ShardIds {
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_ids: vec![1, 1337],
            }],
        };
        ingester
            .close_shards(close_shards_request.clone())
            .await
            .unwrap();

        // Verify idempotency.
        ingester
            .close_shards(close_shards_request.clone())
            .await
            .unwrap();

        let state_guard = ingester.state.read().await;
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_closed();

        let fetch_response =
            tokio::time::timeout(std::time::Duration::from_millis(50), fetch_stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        let fetch_eof = into_fetch_eof(fetch_response);

        assert_eq!(fetch_eof.eof_position(), Position::Beginning.as_eof());
    }

    #[tokio::test]
    async fn test_ingester_open_observation_stream() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let mut observation_stream = ingester
            .open_observation_stream(OpenObservationStreamRequest {})
            .await
            .unwrap();
        let observation = observation_stream.next().await.unwrap().unwrap();
        assert_eq!(observation.node_id, ingester_ctx.node_id);
        assert_eq!(observation.status(), IngesterStatus::Ready);

        let state_guard = ingester.state.read().await;
        let observe_message = ObservationMessage {
            node_id: ingester_ctx.node_id.to_string(),
            status: IngesterStatus::Decommissioning as i32,
        };
        state_guard
            .observation_tx
            .send(Ok(observe_message))
            .unwrap();
        drop(state_guard);

        let observation = observation_stream.next().await.unwrap().unwrap();
        assert_eq!(observation.node_id, ingester_ctx.node_id);
        assert_eq!(observation.status(), IngesterStatus::Decommissioning);

        drop(ingester);

        let observation_opt = observation_stream.next().await;
        assert!(observation_opt.is_none());
    }

    #[tokio::test]
    async fn test_check_decommissioning_status() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let mut state_guard = ingester.state.write().await;

        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status, IngesterStatus::Ready);
        assert_eq!(
            ingester.observation_rx.borrow().as_ref().unwrap().status(),
            IngesterStatus::Ready
        );

        state_guard.status = IngesterStatus::Decommissioning;
        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status, IngesterStatus::Decommissioned);

        state_guard.status = IngesterStatus::Decommissioning;

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);

        state_guard.shards.insert(
            queue_id_01.clone(),
            IngesterShard::new_solo(
                ShardState::Closed,
                Position::offset(12u64),
                Position::Beginning,
            ),
        );
        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status, IngesterStatus::Decommissioning);

        let shard = state_guard.shards.get_mut(&queue_id_01).unwrap();
        shard.truncation_position_inclusive = Position::Beginning.as_eof();

        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status, IngesterStatus::Decommissioned);
        assert_eq!(
            ingester.observation_rx.borrow().as_ref().unwrap().status(),
            IngesterStatus::Decommissioned
        );
    }

    #[tokio::test]
    async fn test_ingester_truncate_on_shard_positions_update() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let event_broker = EventBroker::default();
        ingester.subscribe(&event_broker);

        let shard_01 = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id_01 = queue_id("test-index:0", "test-source", 1);

        let shard_02 = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 2,
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let queue_id_02 = queue_id("test-index:0", "test-source", 2);

        let mut state_guard = ingester.state.write().await;

        ingester
            .init_primary_shard(&mut state_guard, shard_01)
            .await
            .unwrap();
        ingester
            .init_primary_shard(&mut state_guard, shard_02)
            .await
            .unwrap();

        let records = [
            MRecord::new_doc("test-doc-foo").encode(),
            MRecord::new_doc("test-doc-bar").encode(),
        ]
        .into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_01, None, records)
            .await
            .unwrap();

        let records = [MRecord::new_doc("test-doc-baz").encode()].into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id_02, None, records)
            .await
            .unwrap();

        drop(state_guard);

        let shard_position_update = ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid: "test-index:0".into(),
                source_id: "test-source".to_string(),
            },
            shard_positions: vec![
                (1, Position::offset(0u64)),
                (2, Position::eof(0u64)),
                (1337, Position::offset(1337u64)),
            ],
        };
        event_broker.publish(shard_position_update.clone());

        // Verify idempotency.
        event_broker.publish(shard_position_update);

        // Yield so that the event is processed.
        yield_now().await;

        let state_guard = ingester.state.read().await;
        assert_eq!(state_guard.shards.len(), 1);

        assert!(state_guard.shards.contains_key(&queue_id_01));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(1, "\0\0test-doc-bar")]);

        assert!(!state_guard.shards.contains_key(&queue_id_02));
        assert!(!state_guard.mrecordlog.queue_exists(&queue_id_02));
    }
}
