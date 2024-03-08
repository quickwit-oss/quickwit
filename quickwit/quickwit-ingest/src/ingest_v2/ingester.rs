// Copyright (C) 2024 Quickwit, Inc.
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
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use bytesize::ByteSize;
use fnv::FnvHashMap;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use mrecordlog::error::CreateQueueError;
use quickwit_cluster::Cluster;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::pubsub::{EventBroker, EventSubscriber};
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::tower::Pool;
use quickwit_common::{rate_limited_warn, ServiceStream};
use quickwit_proto::control_plane::{
    AdviseResetShardsRequest, ControlPlaneService, ControlPlaneServiceClient,
};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::ingest::ingester::{
    AckReplicationMessage, CloseShardsRequest, CloseShardsResponse, DecommissionRequest,
    DecommissionResponse, FetchMessage, IngesterService, IngesterServiceClient,
    IngesterServiceStream, IngesterStatus, InitShardsRequest, InitShardsResponse,
    ObservationMessage, OpenFetchStreamRequest, OpenObservationStreamRequest,
    OpenReplicationStreamRequest, OpenReplicationStreamResponse, PersistFailure,
    PersistFailureReason, PersistRequest, PersistResponse, PersistSuccess, PingRequest,
    PingResponse, ReplicateFailureReason, ReplicateSubrequest, RetainShardsForSource,
    RetainShardsRequest, RetainShardsResponse, SynReplicationMessage, TruncateShardsRequest,
    TruncateShardsResponse,
};
use quickwit_proto::ingest::{
    CommitTypeV2, IngestV2Error, IngestV2Result, Shard, ShardIds, ShardState,
};
use quickwit_proto::types::{
    queue_id, split_queue_id, IndexUid, NodeId, Position, QueueId, ShardId, SourceId,
};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use super::broadcast::BroadcastLocalShardsTask;
use super::fetch::FetchStreamTask;
use super::idle::CloseIdleShardsTask;
use super::metrics::INGEST_V2_METRICS;
use super::models::IngesterShard;
use super::mrecordlog_utils::{
    append_non_empty_doc_batch, check_enough_capacity, AppendDocBatchError,
};
use super::rate_meter::RateMeter;
use super::replication::{
    ReplicationClient, ReplicationStreamTask, ReplicationStreamTaskHandle, ReplicationTask,
    SYN_REPLICATION_STREAM_CAPACITY,
};
use super::state::{IngesterState, InnerIngesterState, WeakIngesterState};
use super::IngesterPool;
use crate::metrics::INGEST_METRICS;
use crate::mrecordlog_async::MultiRecordLogAsync;
use crate::{estimate_size, with_lock_metrics, FollowerId};

/// Minimum interval between two reset shards operations.
const MIN_RESET_SHARDS_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::ZERO
} else {
    Duration::from_secs(60)
};

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
    control_plane: ControlPlaneServiceClient,
    ingester_pool: IngesterPool,
    state: IngesterState,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    rate_limiter_settings: RateLimiterSettings,
    replication_factor: usize,
    // This semaphore ensures that the ingester that not run two reset shards operations
    // concurrently.
    reset_shards_permits: Arc<Semaphore>,
}

impl fmt::Debug for Ingester {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Ingester")
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

impl Ingester {
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        cluster: Cluster,
        control_plane: ControlPlaneServiceClient,
        ingester_pool: Pool<NodeId, IngesterServiceClient>,
        wal_dir_path: &Path,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
        replication_factor: usize,
        idle_shard_timeout: Duration,
    ) -> IngestV2Result<Self> {
        let self_node_id: NodeId = cluster.self_node_id().into();
        let state = IngesterState::load(wal_dir_path, rate_limiter_settings);

        let weak_state = state.weak();
        BroadcastLocalShardsTask::spawn(cluster, weak_state.clone());
        CloseIdleShardsTask::spawn(weak_state, idle_shard_timeout);

        let ingester = Self {
            self_node_id,
            control_plane,
            ingester_pool,
            state,
            disk_capacity,
            memory_capacity,
            rate_limiter_settings,
            replication_factor,
            reset_shards_permits: Arc::new(Semaphore::new(1)),
        };
        ingester.background_reset_shards();

        Ok(ingester)
    }

    /// Checks whether the ingester is fully decommissioned and updates its status accordingly.
    fn check_decommissioning_status(&self, state: &mut InnerIngesterState) {
        if state.status() != IngesterStatus::Decommissioning {
            return;
        }
        if state.shards.values().all(|shard| shard.is_indexed()) {
            state.set_status(IngesterStatus::Decommissioned);
        }
    }

    /// Initializes a primary shard by creating a queue in the write-ahead log and inserting a new
    /// [`IngesterShard`] into the ingester state. If replication is enabled, this method will
    /// also:
    /// - open a replication stream between the leader and the follower if one does not already
    ///   exist.
    /// - initialize the replica shard.
    async fn init_primary_shard(
        &self,
        state: &mut InnerIngesterState,
        mrecordlog: &mut MultiRecordLogAsync,
        shard: Shard,
        now: Instant,
    ) -> IngestV2Result<()> {
        let queue_id = shard.queue_id();
        info!(
            index_uid=%shard.index_uid(),
            source_id=shard.source_id,
            shard_id=%shard.shard_id(),
            "init primary shard"
        );
        let Entry::Vacant(entry) = state.shards.entry(queue_id.clone()) else {
            return Ok(());
        };
        match mrecordlog.create_queue(&queue_id).await {
            Ok(_) => {}
            Err(CreateQueueError::AlreadyExists) => panic!("queue should not exist"),
            Err(CreateQueueError::IoError(io_error)) => {
                // TODO: Close all shards and set readiness to false.
                error!("failed to create mrecordlog queue `{queue_id}`: {io_error}");
                return Err(IngestV2Error::Internal(format!("Io Error: {io_error}")));
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
                // TODO: Remove dangling queue from the WAL.
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
                now,
            )
        } else {
            IngesterShard::new_solo(
                ShardState::Open,
                Position::Beginning,
                Position::Beginning,
                now,
            )
        };
        entry.insert(primary_shard);
        Ok(())
    }

    /// Resets the local shards in a separate background task.
    fn background_reset_shards(&self) {
        let mut ingester = self.clone();

        let future = async move {
            ingester.reset_shards().await;
        };
        tokio::spawn(future);
    }

    /// Resets the local shards at most once by minute by querying the control plane for the shards
    /// that should be deleted or truncated and then performing the requested operations.
    ///
    /// This operation should be triggered very rarely when the ingester has not been able to delete
    /// or truncate its shards by other means (RPCs from indexers, gossip, etc.).
    async fn reset_shards(&mut self) {
        let Ok(_permit) = self.reset_shards_permits.try_acquire() else {
            return;
        };
        self.state.wait_for_ready().await;

        info!("resetting shards");
        let now = Instant::now();

        let mut per_source_shard_ids: HashMap<(IndexUid, SourceId), Vec<ShardId>> = HashMap::new();

        let state_guard = with_lock_metrics!(self.state.lock_fully().await, "reset_shards", "read")
            .expect("ingester should be ready");

        for queue_id in state_guard.mrecordlog.list_queues() {
            let Some((index_uid, source_id, shard_id)) = split_queue_id(queue_id) else {
                warn!("failed to parse queue ID `{queue_id}`");
                continue;
            };
            per_source_shard_ids
                .entry((index_uid, source_id))
                .or_default()
                .push(shard_id);
        }
        drop(state_guard);

        let shard_ids = per_source_shard_ids
            .into_iter()
            .map(|((index_uid, source_id), shard_ids)| ShardIds {
                index_uid: Some(index_uid),
                source_id,
                shard_ids,
            })
            .collect();

        let advise_reset_shards_request = AdviseResetShardsRequest { shard_ids };
        let advise_reset_shards_future = self
            .control_plane
            .advise_reset_shards(advise_reset_shards_request);
        let advise_reset_shards_result =
            timeout(Duration::from_secs(30), advise_reset_shards_future).await;

        match advise_reset_shards_result {
            Ok(Ok(advise_reset_shards_response)) => {
                let mut state_guard =
                    with_lock_metrics!(self.state.lock_fully().await, "reset_shards", "write")
                        .expect("ingester should be ready");

                state_guard
                    .reset_shards(&advise_reset_shards_response)
                    .await;

                info!(
                    "deleted {} and truncated {} shard(s) in {}",
                    advise_reset_shards_response.shards_to_delete.len(),
                    advise_reset_shards_response.shards_to_truncate.len(),
                    now.elapsed().pretty_display()
                );
                INGEST_V2_METRICS
                    .reset_shards_operations_total
                    .with_label_values(["success"])
                    .inc();
                INGEST_V2_METRICS
                    .wal_disk_usage_bytes
                    .set(state_guard.mrecordlog.disk_usage() as i64);
                INGEST_V2_METRICS
                    .wal_memory_usage_bytes
                    .set(state_guard.mrecordlog.memory_usage() as i64);
            }
            Ok(Err(error)) => {
                warn!("advise reset shards request failed: {error}");

                INGEST_V2_METRICS
                    .reset_shards_operations_total
                    .with_label_values(["error"])
                    .inc();
            }
            Err(_) => {
                warn!("advise reset shards request timed out");

                INGEST_V2_METRICS
                    .reset_shards_operations_total
                    .with_label_values(["timeout"])
                    .inc();
            }
        };
        // We still hold the permit while sleeping so we effectively rate limit the reset shards
        // operation to once per [`MIN_RESET_SHARDS_INTERVAL`].
        if let Some(sleep_for) = MIN_RESET_SHARDS_INTERVAL.checked_sub(now.elapsed()) {
            sleep(sleep_for).await;
        }
    }

    async fn init_replication_stream(
        &self,
        replication_streams: &mut FnvHashMap<FollowerId, ReplicationStreamTaskHandle>,
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
        let weak_ingester_state = self.state.weak();
        // This subscription is the one in charge of truncating the mrecordlog.
        info!("subscribing ingester to shard positions updates");
        event_broker
            .subscribe_without_timeout::<ShardPositionsUpdate>(weak_ingester_state)
            .forever();
    }

    async fn persist_inner(
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
        let mut replicate_subrequests: HashMap<NodeId, Vec<(ReplicateSubrequest, QueueId)>> =
            HashMap::new();
        let mut local_persist_subrequests: Vec<LocalPersistSubrequest> =
            Vec::with_capacity(persist_request.subrequests.len());

        // Keep track of the shards that need to be closed following an IO error.
        let mut shards_to_close: HashSet<QueueId> = HashSet::new();

        // Keep track of dangling shards, i.e., shards for which there is no longer a corresponding
        // queue in the WAL and should be deleted.
        let mut shards_to_delete: HashSet<QueueId> = HashSet::new();

        let commit_type = persist_request.commit_type();
        let force_commit = commit_type == CommitTypeV2::Force;
        let leader_id: NodeId = persist_request.leader_id.into();

        let mut state_guard =
            with_lock_metrics!(self.state.lock_fully().await, "persist", "write")?;

        if state_guard.status() != IngesterStatus::Ready {
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

        // first verify if we would locally accept each subrequest
        {
            let mut sum_of_requested_capacity = bytesize::ByteSize::b(0);
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
                if shard.is_closed() {
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

                let index_uid = subrequest.index_uid().clone();
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

                match check_enough_capacity(
                    &state_guard.mrecordlog,
                    self.disk_capacity,
                    self.memory_capacity,
                    requested_capacity + sum_of_requested_capacity,
                ) {
                    Ok(_usage) => (),
                    Err(error) => {
                        rate_limited_warn!(
                            limit_per_min = 10,
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
                };

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
                rate_meter.update(batch_num_bytes);
                sum_of_requested_capacity += requested_capacity;

                if let Some(follower_id) = follower_id_opt {
                    let replicate_subrequest = ReplicateSubrequest {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        shard_id: subrequest.shard_id,
                        from_position_exclusive: Some(from_position_exclusive),
                        doc_batch: Some(doc_batch),
                    };
                    replicate_subrequests
                        .entry(follower_id)
                        .or_default()
                        .push((replicate_subrequest, queue_id));
                } else {
                    local_persist_subrequests.push(LocalPersistSubrequest {
                        queue_id,
                        subrequest_id: subrequest.subrequest_id,
                        index_uid,
                        source_id: subrequest.source_id,
                        shard_id: subrequest.shard_id,
                        doc_batch,
                        expected_position_inclusive: None,
                    })
                }
            }
        }

        // replicate to the follower
        {
            let mut replicate_futures = FuturesUnordered::new();
            let mut doc_batch_map = HashMap::new();

            for (follower_id, subrequests_with_queue_id) in replicate_subrequests {
                let replication_client = state_guard
                    .replication_streams
                    .get(&follower_id)
                    .expect("replication stream should be initialized")
                    .replication_client();
                let leader_id = self.self_node_id.clone();
                let mut subrequests = Vec::with_capacity(subrequests_with_queue_id.len());
                for (subrequest, queue_id) in subrequests_with_queue_id {
                    let doc_batch = subrequest
                        .doc_batch
                        .clone()
                        .expect("we already verified doc is present and not empty");
                    doc_batch_map.insert(subrequest.subrequest_id, (doc_batch, queue_id));
                    subrequests.push(subrequest);
                }
                let replicate_future =
                    replication_client.replicate(leader_id, follower_id, subrequests, commit_type);
                replicate_futures.push(replicate_future);
            }

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
                    let (doc_batch, queue_id) = doc_batch_map
                        .remove(&replicate_success.subrequest_id)
                        .expect("expected known subrequest id");
                    let local_persist_subrequest = LocalPersistSubrequest {
                        queue_id,
                        subrequest_id: replicate_success.subrequest_id,
                        index_uid: replicate_success.index_uid().clone(),
                        source_id: replicate_success.source_id,
                        shard_id: replicate_success.shard_id,
                        doc_batch,
                        expected_position_inclusive: replicate_success
                            .replication_position_inclusive,
                    };
                    local_persist_subrequests.push(local_persist_subrequest);
                }
                for replicate_failure in replicate_response.failures {
                    // TODO: If the replica shard is closed, close the primary shard if it is not
                    // already.
                    let persist_failure_reason = match replicate_failure.reason() {
                        ReplicateFailureReason::Unspecified => PersistFailureReason::Unspecified,
                        ReplicateFailureReason::ShardNotFound => {
                            PersistFailureReason::ShardNotFound
                        }
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
        }

        // finally write locally
        {
            let now = Instant::now();
            for subrequest in local_persist_subrequests {
                let queue_id = subrequest.queue_id;

                let batch_num_bytes = subrequest.doc_batch.num_bytes() as u64;
                let batch_num_docs = subrequest.doc_batch.num_docs() as u64;

                let append_result = append_non_empty_doc_batch(
                    &mut state_guard.mrecordlog,
                    &queue_id,
                    subrequest.doc_batch,
                    force_commit,
                )
                .await;

                let current_position_inclusive = match append_result {
                    Ok(current_position_inclusive) => current_position_inclusive,
                    Err(append_error) => {
                        let reason = match &append_error {
                            AppendDocBatchError::Io(io_error) => {
                                error!(
                                    "failed to persist records to shard `{queue_id}`: {io_error}"
                                );
                                shards_to_close.insert(queue_id);
                                PersistFailureReason::ShardClosed
                            }
                            AppendDocBatchError::QueueNotFound(_) => {
                                error!(
                                    "failed to persist records to shard `{queue_id}`: WAL queue \
                                     not found"
                                );
                                shards_to_delete.insert(queue_id);
                                PersistFailureReason::ShardNotFound
                            }
                        };
                        let persist_failure = PersistFailure {
                            subrequest_id: subrequest.subrequest_id,
                            index_uid: Some(subrequest.index_uid),
                            source_id: subrequest.source_id,
                            shard_id: subrequest.shard_id,
                            reason: reason as i32,
                        };
                        persist_failures.push(persist_failure);
                        continue;
                    }
                };

                if let Some(expected_position_inclusive) = subrequest.expected_position_inclusive {
                    if expected_position_inclusive != current_position_inclusive {
                        return Err(IngestV2Error::Internal(format!(
                            "bad replica position: expected {expected_position_inclusive:?}, got \
                             {current_position_inclusive:?}"
                        )));
                    }
                }
                state_guard
                    .shards
                    .get_mut(&queue_id)
                    .expect("primary shard should exist")
                    .set_replication_position_inclusive(current_position_inclusive.clone(), now);

                INGEST_METRICS.ingested_num_bytes.inc_by(batch_num_bytes);
                INGEST_METRICS.ingested_num_docs.inc_by(batch_num_docs);

                let persist_success = PersistSuccess {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: Some(subrequest.index_uid),
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    replication_position_inclusive: Some(current_position_inclusive),
                };
                persist_successes.push(persist_success);
            }
        }

        if !shards_to_close.is_empty() {
            for queue_id in &shards_to_close {
                let shard = state_guard
                    .shards
                    .get_mut(queue_id)
                    .expect("shard should exist");

                shard.shard_state = ShardState::Closed;
                shard.notify_shard_status();
            }
            info!(
                "closed {} shard(s) following IO error(s)",
                shards_to_close.len()
            );
        }
        if !shards_to_delete.is_empty() {
            for queue_id in &shards_to_delete {
                state_guard.shards.remove(queue_id);
                state_guard.rate_trackers.remove(queue_id);
            }
            info!("deleted {} dangling shard(s)", shards_to_delete.len());
        }
        let disk_usage = state_guard.mrecordlog.disk_usage() as u64;

        if disk_usage >= self.disk_capacity.as_u64() * 90 / 100 {
            self.background_reset_shards();
        }

        INGEST_V2_METRICS
            .wal_disk_usage_bytes
            .set(disk_usage as i64);
        INGEST_V2_METRICS
            .wal_memory_usage_bytes
            .set(state_guard.mrecordlog.memory_usage() as i64);

        drop(state_guard);

        let leader_id = self.self_node_id.to_string();
        let persist_response = PersistResponse {
            leader_id,
            successes: persist_successes,
            failures: persist_failures,
        };
        Ok(persist_response)
    }

    /// Opens a replication stream, which is a bi-directional gRPC stream. The client-side stream
    async fn open_replication_stream_inner(
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

        let mut state_guard = self.state.lock_partially().await?;

        if state_guard.status() != IngesterStatus::Ready {
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

    async fn open_fetch_stream_inner(
        &mut self,
        open_fetch_stream_request: OpenFetchStreamRequest,
    ) -> IngestV2Result<ServiceStream<IngestV2Result<FetchMessage>>> {
        let queue_id = open_fetch_stream_request.queue_id();
        let shard_status_rx = self
            .state
            .lock_partially()
            .await?
            .shards
            .get(&queue_id)
            .ok_or(IngestV2Error::ShardNotFound {
                shard_id: open_fetch_stream_request.shard_id().clone(),
            })?
            .shard_status_rx
            .clone();
        let mrecordlog = self.state.mrecordlog();
        let (service_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog,
            shard_status_rx,
            FetchStreamTask::DEFAULT_BATCH_NUM_BYTES,
        );
        Ok(service_stream)
    }

    async fn open_observation_stream_inner(
        &mut self,
        _open_observation_stream_request: OpenObservationStreamRequest,
    ) -> IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        let status_stream = ServiceStream::from(self.state.status_rx.clone());
        let self_node_id = self.self_node_id.clone();
        let observation_stream = status_stream.map(move |status| {
            let observation_message = ObservationMessage {
                node_id: self_node_id.clone().into(),
                status: status as i32,
            };
            Ok(observation_message)
        });
        Ok(observation_stream)
    }

    async fn init_shards_inner(
        &mut self,
        init_shards_request: InitShardsRequest,
    ) -> IngestV2Result<InitShardsResponse> {
        let mut state_guard =
            with_lock_metrics!(self.state.lock_fully().await, "init_shards", "write")?;

        if state_guard.status() != IngesterStatus::Ready {
            return Err(IngestV2Error::Internal("node decommissioned".to_string()));
        }
        let now = Instant::now();

        for shard in init_shards_request.shards {
            self.init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard,
                now,
            )
            .await?;
        }
        Ok(InitShardsResponse {})
    }

    async fn truncate_shards_inner(
        &mut self,
        truncate_shards_request: TruncateShardsRequest,
    ) -> IngestV2Result<TruncateShardsResponse> {
        if truncate_shards_request.ingester_id != self.self_node_id {
            return Err(IngestV2Error::Internal(format!(
                "routing error: expected ingester `{}`, got `{}`",
                self.self_node_id, truncate_shards_request.ingester_id,
            )));
        }
        let mut state_guard =
            with_lock_metrics!(self.state.lock_fully().await, "truncate_shards", "write")?;

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
        let current_disk_usage = state_guard.mrecordlog.disk_usage();
        let current_memory_usage = state_guard.mrecordlog.memory_usage();

        INGEST_V2_METRICS
            .wal_disk_usage_bytes
            .set(current_disk_usage as i64);
        INGEST_V2_METRICS
            .wal_memory_usage_bytes
            .set(current_memory_usage as i64);

        self.check_decommissioning_status(&mut state_guard);
        let truncate_response = TruncateShardsResponse {};
        Ok(truncate_response)
    }

    async fn close_shards_inner(
        &mut self,
        close_shards_request: CloseShardsRequest,
    ) -> IngestV2Result<CloseShardsResponse> {
        let mut state_guard =
            with_lock_metrics!(self.state.lock_partially().await, "close_shards", "write")?;

        for shard_ids in close_shards_request.shards {
            for queue_id in shard_ids.queue_ids() {
                if let Some(shard) = state_guard.shards.get_mut(&queue_id) {
                    shard.close();
                }
            }
        }
        Ok(CloseShardsResponse {})
    }

    async fn ping_inner(&mut self, ping_request: PingRequest) -> IngestV2Result<PingResponse> {
        let state_guard = self.state.lock_partially().await?;

        if state_guard.status() != IngesterStatus::Ready {
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

    async fn decommission_inner(
        &mut self,
        _decommission_request: DecommissionRequest,
    ) -> IngestV2Result<DecommissionResponse> {
        info!("decommissioning ingester");
        let mut state_guard = self.state.lock_partially().await?;

        for shard in state_guard.shards.values_mut() {
            shard.shard_state = ShardState::Closed;
            shard.notify_shard_status();
        }
        state_guard.set_status(IngesterStatus::Decommissioning);
        self.check_decommissioning_status(&mut state_guard);

        Ok(DecommissionResponse {})
    }
}

#[async_trait]
impl IngesterService for Ingester {
    async fn persist(
        &mut self,
        persist_request: PersistRequest,
    ) -> IngestV2Result<PersistResponse> {
        self.persist_inner(persist_request).await
    }

    async fn open_replication_stream(
        &mut self,
        syn_replication_stream: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> IngestV2Result<IngesterServiceStream<AckReplicationMessage>> {
        self.open_replication_stream_inner(syn_replication_stream)
            .await
    }

    async fn open_fetch_stream(
        &mut self,
        open_fetch_stream_request: OpenFetchStreamRequest,
    ) -> IngestV2Result<ServiceStream<IngestV2Result<FetchMessage>>> {
        self.open_fetch_stream_inner(open_fetch_stream_request)
            .await
    }

    async fn open_observation_stream(
        &mut self,
        open_observation_stream_request: OpenObservationStreamRequest,
    ) -> IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        self.open_observation_stream_inner(open_observation_stream_request)
            .await
    }

    async fn init_shards(
        &mut self,
        init_shards_request: InitShardsRequest,
    ) -> IngestV2Result<InitShardsResponse> {
        self.init_shards_inner(init_shards_request).await
    }

    async fn retain_shards(
        &mut self,
        request: RetainShardsRequest,
    ) -> IngestV2Result<RetainShardsResponse> {
        let retain_queue_ids: HashSet<QueueId> = request
            .retain_shards_for_sources
            .into_iter()
            .flat_map(|retain_shards_for_source: RetainShardsForSource| {
                let index_uid = retain_shards_for_source.index_uid().clone();
                retain_shards_for_source
                    .shard_ids
                    .into_iter()
                    .map(move |shard_id| {
                        queue_id(&index_uid, &retain_shards_for_source.source_id, &shard_id)
                    })
            })
            .collect();
        let mut state_guard =
            with_lock_metrics!(self.state.lock_fully(), "retain_shards", "write").await?;
        let remove_queue_ids: HashSet<QueueId> = state_guard
            .shards
            .keys()
            .filter(move |shard_id| !retain_queue_ids.contains(*shard_id))
            .map(ToString::to_string)
            .collect();
        info!(queues=?remove_queue_ids, "removing queues");
        for queue_id in remove_queue_ids {
            state_guard.delete_shard(&queue_id).await;
        }
        self.check_decommissioning_status(&mut state_guard);
        Ok(RetainShardsResponse {})
    }

    async fn truncate_shards(
        &mut self,
        truncate_shards_request: TruncateShardsRequest,
    ) -> IngestV2Result<TruncateShardsResponse> {
        self.truncate_shards_inner(truncate_shards_request).await
    }

    async fn close_shards(
        &mut self,
        close_shards_request: CloseShardsRequest,
    ) -> IngestV2Result<CloseShardsResponse> {
        self.close_shards_inner(close_shards_request).await
    }

    async fn ping(&mut self, ping_request: PingRequest) -> IngestV2Result<PingResponse> {
        self.ping_inner(ping_request).await
    }

    async fn decommission(
        &mut self,
        decommission_request: DecommissionRequest,
    ) -> IngestV2Result<DecommissionResponse> {
        self.decommission_inner(decommission_request).await
    }
}

#[async_trait]
impl EventSubscriber<ShardPositionsUpdate> for WeakIngesterState {
    async fn handle_event(&mut self, shard_positions_update: ShardPositionsUpdate) {
        let Some(state) = self.upgrade() else {
            warn!("ingester state update failed");
            return;
        };
        let Ok(mut state_guard) =
            with_lock_metrics!(state.lock_fully().await, "gc_shards", "write")
        else {
            error!("failed to lock the ingester state");
            return;
        };
        let index_uid = shard_positions_update.source_uid.index_uid;
        let source_id = shard_positions_update.source_uid.source_id;

        for (shard_id, shard_position) in shard_positions_update.updated_shard_positions {
            let queue_id = queue_id(&index_uid, &source_id, &shard_id);
            if shard_position.is_eof() {
                info!(shard = queue_id, "deleting shard");
                state_guard.delete_shard(&queue_id).await;
            } else {
                info!(shard=queue_id, shard_position=%shard_position, "truncating shard");
                state_guard.truncate_shard(&queue_id, &shard_position).await;
            }
        }
    }
}

pub async fn wait_for_ingester_status(
    mut ingester: IngesterServiceClient,
    status: IngesterStatus,
) -> anyhow::Result<()> {
    let mut observation_stream = ingester
        .open_observation_stream(OpenObservationStreamRequest {})
        .await
        .context("failed to open observation stream")?;

    while let Some(observation_message_result) = observation_stream.next().await {
        let observation_message =
            observation_message_result.context("observation stream ended unexpectedly")?;

        if observation_message.status() == status {
            break;
        }
    }
    Ok(())
}

pub async fn wait_for_ingester_decommission(
    mut ingester: IngesterServiceClient,
) -> anyhow::Result<()> {
    let now = Instant::now();

    ingester
        .decommission(DecommissionRequest {})
        .await
        .context("failed to initiate ingester decommission")?;

    wait_for_ingester_status(ingester, IngesterStatus::Decommissioned).await?;

    info!(
        "successfully decommissioned ingester in {}",
        now.elapsed().pretty_display()
    );
    Ok(())
}

struct LocalPersistSubrequest {
    queue_id: QueueId,
    subrequest_id: u32,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: Option<quickwit_proto::types::ShardId>,
    doc_batch: quickwit_proto::ingest::DocBatchV2,
    expected_position_inclusive: Option<Position>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::mutable_key_type)]

    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, Ordering};

    use bytes::Bytes;
    use quickwit_cluster::{create_cluster_for_test_with_id, ChannelTransport};
    use quickwit_common::shared_consts::INGESTER_PRIMARY_SHARDS_PREFIX;
    use quickwit_common::tower::ConstantRate;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::control_plane::{AdviseResetShardsResponse, MockControlPlaneService};
    use quickwit_proto::ingest::ingester::{
        IngesterServiceGrpcServer, IngesterServiceGrpcServerAdapter, PersistSubrequest,
        TruncateShardsSubrequest,
    };
    use quickwit_proto::ingest::{DocBatchV2, ShardIdPosition, ShardIdPositions, ShardIds};
    use quickwit_proto::types::{queue_id, ShardId, SourceUid};
    use tokio::task::yield_now;
    use tokio::time::timeout;
    use tonic::transport::{Endpoint, Server};

    use super::*;
    use crate::ingest_v2::broadcast::ShardInfos;
    use crate::ingest_v2::fetch::tests::{into_fetch_eof, into_fetch_payload};
    use crate::ingest_v2::DEFAULT_IDLE_SHARD_TIMEOUT;
    use crate::MRecord;

    const MAX_GRPC_MESSAGE_SIZE: ByteSize = ByteSize::mib(1);

    pub(super) struct IngesterForTest {
        node_id: NodeId,
        control_plane: ControlPlaneServiceClient,
        ingester_pool: IngesterPool,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
        replication_factor: usize,
        idle_shard_timeout: Duration,
    }

    impl Default for IngesterForTest {
        fn default() -> Self {
            let mut mock_control_plane = MockControlPlaneService::new();
            mock_control_plane
                .expect_advise_reset_shards()
                .returning(|_| Ok(AdviseResetShardsResponse::default()));
            let control_plane = ControlPlaneServiceClient::from(mock_control_plane);

            Self {
                node_id: "test-ingester".into(),
                control_plane,
                ingester_pool: IngesterPool::default(),
                disk_capacity: ByteSize::mb(256),
                memory_capacity: ByteSize::mb(1),
                rate_limiter_settings: RateLimiterSettings::default(),
                replication_factor: 1,
                idle_shard_timeout: DEFAULT_IDLE_SHARD_TIMEOUT,
            }
        }
    }

    impl IngesterForTest {
        pub fn with_node_id(mut self, node_id: &str) -> Self {
            self.node_id = node_id.into();
            self
        }

        pub fn with_control_plane(mut self, control_plane: ControlPlaneServiceClient) -> Self {
            self.control_plane = control_plane;
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

        pub fn with_idle_shard_timeout(mut self, idle_shard_timeout: Duration) -> Self {
            self.idle_shard_timeout = idle_shard_timeout;
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
                self.control_plane.clone(),
                self.ingester_pool.clone(),
                wal_dir_path,
                self.disk_capacity,
                self.memory_capacity,
                self.rate_limiter_settings,
                self.replication_factor,
                self.idle_shard_timeout,
            )
            .await
            .unwrap();

            wait_for_ingester_status(
                IngesterServiceClient::new(ingester.clone()),
                IngesterStatus::Ready,
            )
            .await
            .unwrap();

            let ingester_env = IngesterContext {
                tempdir,
                _transport: transport,
                node_id: self.node_id,
                cluster,
                ingester_pool: self.ingester_pool,
            };
            (ingester_env, ingester)
        }
    }

    pub struct IngesterContext {
        tempdir: tempfile::TempDir,
        _transport: ChannelTransport,
        node_id: NodeId,
        cluster: Cluster,
        ingester_pool: IngesterPool,
    }

    #[tokio::test]
    async fn test_ingester_init() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));
        let queue_id_03 = queue_id(&index_uid, "test-source", &ShardId::from(3));

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

        state_guard
            .mrecordlog
            .create_queue(&queue_id_03)
            .await
            .unwrap();

        state_guard.set_status(IngesterStatus::Initializing);

        drop(state_guard);

        ingester
            .state
            .init(ingester_ctx.tempdir.path(), RateLimiterSettings::default())
            .await;

        let state_guard = ingester.state.lock_fully().await.unwrap();
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

        assert_eq!(state_guard.status(), IngesterStatus::Ready);
    }

    #[tokio::test]
    async fn test_ingester_broadcasts_local_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let shard = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
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
            index_uid, "test-source"
        );
        let value = ingester_ctx.cluster.get_self_key_value(&key).await.unwrap();

        let shard_infos: ShardInfos = serde_json::from_str(&value).unwrap();
        assert_eq!(shard_infos.len(), 1);

        let shard_info = shard_infos.iter().next().unwrap();
        assert_eq!(shard_info.shard_id, ShardId::from(1));
        assert_eq!(shard_info.shard_state, ShardState::Open);
        assert_eq!(shard_info.ingestion_rate, 0);

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
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

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        state_guard.shards.remove(&queue_id_01).unwrap();
        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let value_opt = ingester_ctx.cluster.get_self_key_value(&key).await;
        assert!(value_opt.is_none());
    }

    #[tokio::test]
    async fn test_ingester_persist() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index", 1);
        let init_shards_request = InitShardsRequest {
            shards: vec![
                Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    ..Default::default()
                },
                Shard {
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
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
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
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
        assert_eq!(persist_success_0.index_uid(), &index_uid);
        assert_eq!(persist_success_0.source_id, "test-source");
        assert_eq!(persist_success_0.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_success_0.replication_position_inclusive,
            Some(Position::offset(1u64))
        );

        let persist_success_1 = &persist_response.successes[1];
        assert_eq!(persist_success_1.subrequest_id, 1);
        assert_eq!(persist_success_1.index_uid(), &index_uid2);
        assert_eq!(persist_success_1.source_id, "test-source");
        assert_eq!(persist_success_1.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_success_1.replication_position_inclusive,
            Some(Position::offset(2u64))
        );

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert_eq!(state_guard.shards.len(), 2);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let solo_shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        solo_shard_01.assert_is_solo();
        solo_shard_01.assert_is_open();
        solo_shard_01.assert_replication_position(Position::offset(1u64));

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010"), (1, "\0\x01")],
        );

        let queue_id_11 = queue_id(&index_uid2, "test-source", &ShardId::from(1));
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
    async fn test_ingester_persist_empty() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: Vec::new(),
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_request = PersistRequest {
            leader_id: "test-ingester".to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: None,
            }],
        };

        let init_shards_request = InitShardsRequest {
            shards: vec![Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: ingester_ctx.node_id.to_string(),
                ..Default::default()
            }],
        };
        ingester.init_shards(init_shards_request).await.unwrap();

        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 1);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success = &persist_response.successes[0];
        assert_eq!(persist_success.subrequest_id, 0);
        assert_eq!(persist_success.index_uid(), &index_uid);
        assert_eq!(persist_success.source_id, "test-source");
        assert_eq!(persist_success.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_success.replication_position_inclusive,
            Some(Position::Beginning)
        );
    }

    // This test should be run manually and independently of other tests with the `fail/failpoints`
    // feature enabled.
    #[tokio::test]
    #[ignore]
    async fn test_ingester_persist_closes_shard_on_io_error() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("ingester:append_records", "return").unwrap();

        let (_ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let solo_shard = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        state_guard.shards.insert(queue_id.clone(), solo_shard);

        state_guard
            .mrecordlog
            .create_queue(&queue_id)
            .await
            .unwrap();

        let rate_limiter = RateLimiter::from_settings(RateLimiterSettings::default());
        let rate_meter = RateMeter::default();
        state_guard
            .rate_trackers
            .insert(queue_id.clone(), (rate_limiter, rate_meter));

        drop(state_guard);

        let persist_request = PersistRequest {
            leader_id: "test-ingester".to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id(), ShardId::from(1));
        assert_eq!(persist_failure.reason(), PersistFailureReason::ShardClosed,);

        let state_guard = ingester.state.lock_fully().await.unwrap();
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_closed();

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_ingester_persist_deletes_dangling_shard() {
        let (_ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let solo_shard = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        state_guard.shards.insert(queue_id.clone(), solo_shard);

        let rate_limiter = RateLimiter::from_settings(RateLimiterSettings::default());
        let rate_meter = RateMeter::default();
        state_guard
            .rate_trackers
            .insert(queue_id.clone(), (rate_limiter, rate_meter));

        drop(state_guard);

        let persist_request = PersistRequest {
            leader_id: "test-ingester".to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::ShardNotFound
        );

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert_eq!(state_guard.shards.len(), 0);
        assert_eq!(state_guard.rate_trackers.len(), 0);
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

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index", 1);

        let init_shards_request = InitShardsRequest {
            shards: vec![
                Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    leader_id: leader_ctx.node_id.to_string(),
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    ..Default::default()
                },
                Shard {
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
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
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
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
        assert_eq!(persist_success_0.index_uid(), &index_uid);
        assert_eq!(persist_success_0.source_id, "test-source");
        assert_eq!(persist_success_0.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_success_0.replication_position_inclusive,
            Some(Position::offset(1u64))
        );

        let persist_success_1 = &persist_response.successes[1];
        assert_eq!(persist_success_1.subrequest_id, 1);
        assert_eq!(persist_success_1.index_uid(), &index_uid2);
        assert_eq!(persist_success_1.source_id, "test-source");
        assert_eq!(persist_success_1.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_success_1.replication_position_inclusive,
            Some(Position::offset(2u64))
        );

        let leader_state_guard = leader.state.lock_fully().await.unwrap();
        assert_eq!(leader_state_guard.shards.len(), 2);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let primary_shard_01 = leader_state_guard.shards.get(&queue_id_01).unwrap();
        primary_shard_01.assert_is_primary();
        primary_shard_01.assert_is_open();
        primary_shard_01.assert_replication_position(Position::offset(1u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010"), (1, "\0\x01")],
        );

        let queue_id_11 = queue_id(&index_uid2, "test-source", &ShardId::from(1));
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

        let follower_state_guard = follower.state.lock_fully().await.unwrap();
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
            MAX_GRPC_MESSAGE_SIZE,
        );

        leader_ctx
            .ingester_pool
            .insert(follower_ctx.node_id.clone(), follower_grpc_client);

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index", 1);

        let init_shards_request = InitShardsRequest {
            shards: vec![
                Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    shard_state: ShardState::Open as i32,
                    leader_id: leader_ctx.node_id.to_string(),
                    follower_id: Some(follower_ctx.node_id.to_string()),
                    ..Default::default()
                },
                Shard {
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
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
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
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
        assert_eq!(persist_success_0.index_uid(), &index_uid);
        assert_eq!(persist_success_0.source_id, "test-source");
        assert_eq!(persist_success_0.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_success_0.replication_position_inclusive,
            Some(Position::offset(0u64))
        );

        let persist_success_1 = &persist_response.successes[1];
        assert_eq!(persist_success_1.subrequest_id, 1);
        assert_eq!(persist_success_1.index_uid(), &index_uid2);
        assert_eq!(persist_success_1.source_id, "test-source");
        assert_eq!(persist_success_1.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_success_1.replication_position_inclusive,
            Some(Position::offset(1u64))
        );

        let leader_state_guard = leader.state.lock_fully().await.unwrap();
        assert_eq!(leader_state_guard.shards.len(), 2);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let primary_shard_01 = leader_state_guard.shards.get(&queue_id_01).unwrap();
        primary_shard_01.assert_is_primary();
        primary_shard_01.assert_is_open();
        primary_shard_01.assert_replication_position(Position::offset(0u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-010")],
        );

        let queue_id_11 = queue_id(&index_uid2, "test-source", &ShardId::from(1));
        let primary_shard_11 = leader_state_guard.shards.get(&queue_id_11).unwrap();
        primary_shard_11.assert_is_primary();
        primary_shard_11.assert_is_open();
        primary_shard_11.assert_replication_position(Position::offset(1u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[(0, "\0\0test-doc-110"), (1, "\0\0test-doc-111")],
        );

        let follower_state_guard = follower.state.lock_fully().await.unwrap();
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
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let solo_shard = IngesterShard::new_solo(
            ShardState::Closed,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        ingester
            .state
            .lock_fully()
            .await
            .unwrap()
            .shards
            .insert(queue_id_01.clone(), solo_shard);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id(), ShardId::from(1));
        assert_eq!(persist_failure.reason(), PersistFailureReason::ShardClosed);

        let state_guard = ingester.state.lock_fully().await.unwrap();
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

        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let primary_shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            ..Default::default()
        };
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                primary_shard,
                Instant::now(),
            )
            .await
            .unwrap();

        drop(state_guard);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id(), ShardId::from(1));
        assert_eq!(persist_failure.reason(), PersistFailureReason::RateLimited);

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

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

        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let primary_shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            ..Default::default()
        };
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                primary_shard,
                Instant::now(),
            )
            .await
            .unwrap();

        drop(state_guard);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-010"])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.shard_id(), ShardId::from(1));
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::ResourceExhausted
        );

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
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

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert!(state_guard.replication_tasks.contains_key("test-leader"));
    }

    #[tokio::test]
    async fn test_ingester_open_fetch_stream() {
        let (_ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: "test-client".to_string(),
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1337)),
            from_position_exclusive: Some(Position::Beginning),
        };
        let error = ingester
            .open_fetch_stream(open_fetch_stream_request)
            .await
            .unwrap_err();
        assert!(
            matches!(error, IngestV2Error::ShardNotFound { shard_id } if shard_id == ShardId::from(1337))
        );

        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard,
                Instant::now(),
            )
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
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            from_position_exclusive: Some(Position::Beginning),
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

        let mut state_guard = ingester.state.lock_fully().await.unwrap();

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
    async fn test_ingester_truncate_shards() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                now,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                now,
            )
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
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    truncate_up_to_position_inclusive: Some(Position::offset(0u64)),
                },
                TruncateShardsSubrequest {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(2)),
                    truncate_up_to_position_inclusive: Some(Position::eof(0u64)),
                },
                TruncateShardsSubrequest {
                    index_uid: Some(IndexUid::for_test("test-index", 1337)),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1337)),
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

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        assert!(state_guard.shards.contains_key(&queue_id_01));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(1, "\0\0test-doc-bar")]);

        assert!(!state_guard.shards.contains_key(&queue_id_02));
        assert!(!state_guard.mrecordlog.queue_exists(&queue_id_02));
    }

    #[tokio::test]
    async fn test_ingester_truncate_shards_deletes_dangling_shards() {
        let (ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let solo_shard = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        state_guard.shards.insert(queue_id.clone(), solo_shard);

        let rate_limiter = RateLimiter::from_settings(RateLimiterSettings::default());
        let rate_meter = RateMeter::default();
        state_guard
            .rate_trackers
            .insert(queue_id.clone(), (rate_limiter, rate_meter));

        drop(state_guard);

        let truncate_shards_request = TruncateShardsRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            subrequests: vec![TruncateShardsSubrequest {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                truncate_up_to_position_inclusive: Some(Position::offset(0u64)),
            }],
        };
        ingester
            .truncate_shards(truncate_shards_request.clone())
            .await
            .unwrap();

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert_eq!(state_guard.shards.len(), 0);
        assert_eq!(state_guard.rate_trackers.len(), 0);
    }

    #[tokio::test]
    async fn test_ingester_reset_shards() {
        let mut mock_control_plane = MockControlPlaneService::new();
        mock_control_plane
            .expect_advise_reset_shards()
            .once()
            .returning(|_| Ok(AdviseResetShardsResponse::default()));

        mock_control_plane
            .expect_advise_reset_shards()
            .once()
            .returning(|mut request| {
                assert_eq!(request.shard_ids.len(), 1);
                assert_eq!(request.shard_ids[0].index_uid(), &("test-index", 0));
                assert_eq!(request.shard_ids[0].source_id, "test-source");
                request.shard_ids[0].shard_ids.sort_unstable();
                assert_eq!(
                    request.shard_ids[0].shard_ids,
                    [ShardId::from(1), ShardId::from(2)]
                );
                let response = AdviseResetShardsResponse {
                    shards_to_delete: vec![ShardIds {
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        shard_ids: vec![ShardId::from(1)],
                    }],
                    shards_to_truncate: vec![ShardIdPositions {
                        index_uid: Some(IndexUid::for_test("test-index", 0)),
                        source_id: "test-source".to_string(),
                        shard_positions: vec![ShardIdPosition {
                            shard_id: Some(ShardId::from(2)),
                            publish_position_inclusive: Some(Position::offset(1u64)),
                        }],
                    }],
                };
                Ok(response)
            });
        let control_plane = ControlPlaneServiceClient::from(mock_control_plane);

        let (_ingester_ctx, mut ingester) = IngesterForTest::default()
            .with_control_plane(control_plane)
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                now,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                now,
            )
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

        drop(state_guard);

        ingester.reset_shards().await;

        let state_guard = ingester.state.lock_partially().await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        shard_02.assert_truncation_position(Position::offset(1u64));
    }

    #[tokio::test]
    async fn test_ingester_retain_shards() {
        let (_ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let shard_17 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(17)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };

        let shard_18 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(18)),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let queue_id_17 = queue_id(
            shard_17.index_uid(),
            &shard_17.source_id,
            shard_17.shard_id(),
        );

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_17,
                now,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_18,
                now,
            )
            .await
            .unwrap();

        drop(state_guard);

        {
            let state_guard = ingester.state.lock_fully().await.unwrap();
            assert_eq!(state_guard.shards.len(), 2);
        }

        let retain_shards_request = RetainShardsRequest {
            retain_shards_for_sources: vec![RetainShardsForSource {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_ids: vec![ShardId::from(17u64)],
            }],
        };
        ingester.retain_shards(retain_shards_request).await.unwrap();

        {
            let state_guard = ingester.state.lock_fully().await.unwrap();
            assert_eq!(state_guard.shards.len(), 1);
            assert!(state_guard.shards.contains_key(&queue_id_17));
        }
    }

    #[tokio::test]
    async fn test_ingester_close_shards() {
        let (_ingester_ctx, mut ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        };
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard,
                Instant::now(),
            )
            .await
            .unwrap();
        drop(state_guard);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: "test-client".to_string(),
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            from_position_exclusive: Some(Position::Beginning),
        };
        let mut fetch_stream = ingester
            .open_fetch_stream(open_fetch_stream_request)
            .await
            .unwrap();

        let close_shards_request = CloseShardsRequest {
            shards: vec![ShardIds {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_ids: vec![ShardId::from(1), ShardId::from(1337)],
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

        let state_guard = ingester.state.lock_partially().await.unwrap();
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_closed();

        let fetch_response = timeout(Duration::from_millis(100), fetch_stream.next())
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

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        state_guard.set_status(IngesterStatus::Decommissioning);
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
        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status(), IngesterStatus::Ready);

        state_guard.set_status(IngesterStatus::Decommissioning);
        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status(), IngesterStatus::Decommissioned);

        state_guard.set_status(IngesterStatus::Decommissioning);

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

        state_guard.shards.insert(
            queue_id_01.clone(),
            IngesterShard::new_solo(
                ShardState::Closed,
                Position::offset(12u64),
                Position::Beginning,
                Instant::now(),
            ),
        );
        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status(), IngesterStatus::Decommissioning);

        let shard = state_guard.shards.get_mut(&queue_id_01).unwrap();
        shard.truncation_position_inclusive = Position::Beginning.as_eof();

        ingester.check_decommissioning_status(&mut state_guard);
        assert_eq!(state_guard.status(), IngesterStatus::Decommissioned);
    }

    #[tokio::test]
    async fn test_ingester_truncate_on_shard_positions_update() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let event_broker = EventBroker::default();
        ingester.subscribe(&event_broker);

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                now,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                now,
            )
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
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
            },
            updated_shard_positions: vec![
                (ShardId::from(1), Position::offset(0u64)),
                (ShardId::from(2), Position::eof(0u64)),
                (ShardId::from(1337), Position::offset(1337u64)),
            ],
        };
        event_broker.publish(shard_position_update.clone());

        // Verify idempotency.
        event_broker.publish(shard_position_update);

        // Yield so that the event is processed.
        yield_now().await;

        let state_guard = ingester.state.lock_fully().await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        assert!(state_guard.shards.contains_key(&queue_id_01));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(1, "\0\0test-doc-bar")]);

        assert!(!state_guard.shards.contains_key(&queue_id_02));
        assert!(!state_guard.mrecordlog.queue_exists(&queue_id_02));
    }

    #[tokio::test]
    async fn test_ingester_closes_idle_shards() {
        let idle_shard_timeout = Duration::from_millis(200);
        let (_ingester_ctx, ingester) = IngesterForTest::default()
            .with_idle_shard_timeout(idle_shard_timeout)
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                now - idle_shard_timeout,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                now,
            )
            .await
            .unwrap();
        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(100)).await; // 2 times the run interval period of the close idle shards task

        let state_guard = ingester.state.lock_partially().await.unwrap();
        state_guard
            .shards
            .get(&queue_id_01)
            .unwrap()
            .assert_is_closed();
        state_guard
            .shards
            .get(&queue_id_02)
            .unwrap()
            .assert_is_open();
        drop(state_guard);
    }
}
