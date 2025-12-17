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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use bytesize::ByteSize;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use mrecordlog::error::CreateQueueError;
use once_cell::sync::OnceCell;
use quickwit_cluster::Cluster;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::pubsub::{EventBroker, EventSubscriber};
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::tower::Pool;
use quickwit_common::{ServiceStream, rate_limited_error, rate_limited_warn};
use quickwit_proto::control_plane::{
    AdviseResetShardsRequest, ControlPlaneService, ControlPlaneServiceClient,
};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::ingest::ingester::{
    AckReplicationMessage, CloseShardsRequest, CloseShardsResponse, DecommissionRequest,
    DecommissionResponse, FetchMessage, IngesterService, IngesterServiceClient,
    IngesterServiceStream, IngesterStatus, InitShardFailure, InitShardSuccess, InitShardsRequest,
    InitShardsResponse, ObservationMessage, OpenFetchStreamRequest, OpenObservationStreamRequest,
    OpenReplicationStreamRequest, OpenReplicationStreamResponse, PersistFailure,
    PersistFailureReason, PersistRequest, PersistResponse, PersistSuccess, ReplicateFailureReason,
    ReplicateSubrequest, RetainShardsForSource, RetainShardsRequest, RetainShardsResponse,
    SynReplicationMessage, TruncateShardsRequest, TruncateShardsResponse,
};
use quickwit_proto::ingest::{
    CommitTypeV2, DocBatchV2, IngestV2Error, IngestV2Result, ParseFailure, Shard, ShardIds,
    ShardState,
};
use quickwit_proto::types::{
    IndexUid, NodeId, Position, QueueId, ShardId, SourceId, SubrequestId, queue_id, split_queue_id,
};
use serde_json::{Value as JsonValue, json};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{debug, error, info, warn};

use super::IngesterPool;
use super::broadcast::BroadcastLocalShardsTask;
use super::doc_mapper::validate_doc_batch;
use super::fetch::FetchStreamTask;
use super::idle::CloseIdleShardsTask;
use super::metrics::INGEST_V2_METRICS;
use super::models::IngesterShard;
use super::mrecordlog_utils::{
    AppendDocBatchError, append_non_empty_doc_batch, check_enough_capacity,
};
use super::rate_meter::RateMeter;
use super::replication::{
    ReplicationClient, ReplicationStreamTask, ReplicationStreamTaskHandle, ReplicationTask,
    SYN_REPLICATION_STREAM_CAPACITY,
};
use super::state::{IngesterState, InnerIngesterState, WeakIngesterState};
use crate::ingest_v2::doc_mapper::get_or_try_build_doc_mapper;
use crate::ingest_v2::metrics::report_wal_usage;
use crate::ingest_v2::models::IngesterShardType;
use crate::mrecordlog_async::MultiRecordLogAsync;
use crate::{FollowerId, estimate_size, with_lock_metrics};

/// Minimum interval between two reset shards operations.
const MIN_RESET_SHARDS_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::ZERO
} else {
    Duration::from_secs(60)
};

/// Duration after which persist requests time out with
/// [`quickwit_proto::ingest::IngestV2Error::Timeout`].
pub(super) const PERSIST_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(6)
};

const DEFAULT_BATCH_NUM_BYTES: usize = 1024 * 1024; // 1 MiB

fn get_batch_num_bytes() -> usize {
    static BATCH_NUM_BYTES_CELL: OnceCell<usize> = OnceCell::new();
    *BATCH_NUM_BYTES_CELL.get_or_init(|| {
        quickwit_common::get_from_env("QW_INGEST_BATCH_NUM_BYTES", DEFAULT_BATCH_NUM_BYTES, false)
    })
}

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
        doc_mapping_json: &str,
        now: Instant,
        validate: bool,
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
        let doc_mapper = get_or_try_build_doc_mapper(
            &mut state.doc_mappers,
            shard.doc_mapping_uid(),
            doc_mapping_json,
        )?;
        match mrecordlog.create_queue(&queue_id).await {
            Ok(_) => {}
            Err(CreateQueueError::AlreadyExists) => {
                error!("WAL queue `{queue_id}` already exists");
                let message = format!("WAL queue `{queue_id}` already exists");
                return Err(IngestV2Error::Internal(message));
            }
            Err(CreateQueueError::IoError(io_error)) => {
                error!("failed to create WAL queue `{queue_id}`: {io_error}",);
                let message = format!("failed to create WAL queue `{queue_id}`: {io_error}");
                return Err(IngestV2Error::Internal(message));
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
                error!("failed to initialize replica shard: {error}");
                let message = format!("failed to initialize replica shard: {error}");
                return Err(IngestV2Error::Internal(message));
            }
            IngesterShard::new_primary(
                follower_id,
                ShardState::Open,
                Position::Beginning,
                Position::Beginning,
                doc_mapper,
                now,
                validate,
            )
        } else {
            IngesterShard::new_solo(
                ShardState::Open,
                Position::Beginning,
                Position::Beginning,
                Some(doc_mapper),
                now,
                validate,
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

        let advise_reset_shards_request = AdviseResetShardsRequest {
            ingester_id: self.self_node_id.to_string(),
            shard_ids,
        };
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

                let wal_usage = state_guard.mrecordlog.resource_usage();
                report_wal_usage(wal_usage);
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

        let ingester = self.ingester_pool.get(&follower_id).ok_or_else(|| {
            let message = format!("ingester `{follower_id}` is unavailable");
            IngestV2Error::Unavailable(message)
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
        &self,
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
        let mut per_follower_replicate_subrequests: HashMap<NodeId, Vec<ReplicateSubrequest>> =
            HashMap::new();
        let mut pending_persist_subrequests: HashMap<SubrequestId, PendingPersistSubrequest> =
            HashMap::with_capacity(persist_request.subrequests.len());

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
            let mut total_requested_capacity = ByteSize::b(0);

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
                // A router can only know about a newly opened shard if it has been informed by the
                // control plane, which confirms that the shard was correctly opened in the
                // metastore.
                shard.is_advertisable = true;

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
                let doc_mapper = shard.doc_mapper_opt.clone().expect("shard should be open");
                let validate_docs = shard.validate_docs;
                let follower_id_opt = shard.follower_id_opt().cloned();
                let from_position_exclusive = shard.replication_position_inclusive.clone();

                let doc_batch = match subrequest.doc_batch {
                    Some(doc_batch) if !doc_batch.is_empty() => doc_batch,
                    _ => {
                        warn!("received empty persist request");
                        DocBatchV2::default()
                    }
                };
                let requested_capacity = estimate_size(&doc_batch);

                if let Err(error) = check_enough_capacity(
                    &state_guard.mrecordlog,
                    self.disk_capacity,
                    self.memory_capacity,
                    requested_capacity + total_requested_capacity,
                ) {
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
                        reason: PersistFailureReason::WalFull as i32,
                    };
                    persist_failures.push(persist_failure);
                    continue;
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
                        reason: PersistFailureReason::ShardRateLimited as i32,
                    };
                    persist_failures.push(persist_failure);
                    continue;
                }

                // Total number of bytes (valid and invalid documents)
                let original_batch_num_bytes = doc_batch.num_bytes() as u64;

                let (valid_doc_batch, parse_failures) = if validate_docs {
                    validate_doc_batch(doc_batch, doc_mapper).await?
                } else {
                    (doc_batch, Vec::new())
                };

                if valid_doc_batch.is_empty() {
                    crate::metrics::INGEST_METRICS
                        .ingested_docs_invalid
                        .inc_by(parse_failures.len() as u64);
                    crate::metrics::INGEST_METRICS
                        .ingested_docs_bytes_invalid
                        .inc_by(original_batch_num_bytes);
                    let persist_success = PersistSuccess {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        shard_id: subrequest.shard_id,
                        replication_position_inclusive: Some(from_position_exclusive),
                        num_persisted_docs: 0,
                        parse_failures,
                    };
                    persist_successes.push(persist_success);
                    continue;
                };

                crate::metrics::INGEST_METRICS
                    .ingested_docs_valid
                    .inc_by(valid_doc_batch.num_docs() as u64);
                crate::metrics::INGEST_METRICS
                    .ingested_docs_bytes_valid
                    .inc_by(valid_doc_batch.num_bytes() as u64);
                if !parse_failures.is_empty() {
                    crate::metrics::INGEST_METRICS
                        .ingested_docs_invalid
                        .inc_by(parse_failures.len() as u64);
                    crate::metrics::INGEST_METRICS
                        .ingested_docs_bytes_invalid
                        .inc_by(original_batch_num_bytes - valid_doc_batch.num_bytes() as u64);
                }
                let valid_batch_num_bytes = valid_doc_batch.num_bytes() as u64;
                rate_meter.update(valid_batch_num_bytes);
                total_requested_capacity += requested_capacity;

                let mut successfully_replicated = true;

                if let Some(follower_id) = follower_id_opt {
                    successfully_replicated = false;

                    let replicate_subrequest = ReplicateSubrequest {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid.clone(),
                        source_id: subrequest.source_id.clone(),
                        shard_id: subrequest.shard_id.clone(),
                        from_position_exclusive: Some(from_position_exclusive),
                        doc_batch: Some(valid_doc_batch.clone()),
                    };
                    per_follower_replicate_subrequests
                        .entry(follower_id)
                        .or_default()
                        .push(replicate_subrequest);
                }
                let pending_persist_subrequest = PendingPersistSubrequest {
                    queue_id,
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    doc_batch: valid_doc_batch,
                    parse_failures,
                    expected_position_inclusive: None,
                    successfully_replicated,
                };
                pending_persist_subrequests.insert(
                    pending_persist_subrequest.subrequest_id,
                    pending_persist_subrequest,
                );
            }
        }
        // replicate to the follower
        {
            let mut replicate_futures = FuturesUnordered::new();

            for (follower_id, replicate_subrequests) in per_follower_replicate_subrequests {
                let replication_client = state_guard
                    .replication_streams
                    .get(&follower_id)
                    .expect("replication stream should be initialized")
                    .replication_client();
                let leader_id = self.self_node_id.clone();

                let replicate_future = replication_client.replicate(
                    leader_id,
                    follower_id,
                    replicate_subrequests,
                    commit_type,
                );
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
                    let pending_persist_subrequest = pending_persist_subrequests
                        .get_mut(&replicate_success.subrequest_id)
                        .expect("persist subrequest should exist");

                    pending_persist_subrequest.successfully_replicated = true;
                    pending_persist_subrequest.expected_position_inclusive =
                        replicate_success.replication_position_inclusive;
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
                        ReplicateFailureReason::WalFull => PersistFailureReason::WalFull,
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
            for subrequest in pending_persist_subrequests.into_values() {
                if !subrequest.successfully_replicated {
                    continue;
                }
                let queue_id = subrequest.queue_id;

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
                            index_uid: subrequest.index_uid,
                            source_id: subrequest.source_id,
                            shard_id: subrequest.shard_id,
                            reason: reason as i32,
                        };
                        persist_failures.push(persist_failure);
                        continue;
                    }
                };

                if let Some(expected_position_inclusive) = subrequest.expected_position_inclusive
                    && expected_position_inclusive != current_position_inclusive
                {
                    return Err(IngestV2Error::Internal(format!(
                        "bad replica position: expected {expected_position_inclusive:?}, got \
                         {current_position_inclusive:?}"
                    )));
                }
                state_guard
                    .shards
                    .get_mut(&queue_id)
                    .expect("primary shard should exist")
                    .set_replication_position_inclusive(current_position_inclusive.clone(), now);

                let persist_success = PersistSuccess {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    replication_position_inclusive: Some(current_position_inclusive),
                    num_persisted_docs: batch_num_docs as u32,
                    parse_failures: subrequest.parse_failures,
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

                shard.close();
                warn!("closed shard `{queue_id}` following IO error");
            }
        }
        if !shards_to_delete.is_empty() {
            for queue_id in &shards_to_delete {
                state_guard.shards.remove(queue_id);
                state_guard.rate_trackers.remove(queue_id);
                warn!("deleted dangling shard `{queue_id}`");
            }
        }
        let wal_usage = state_guard.mrecordlog.resource_usage();
        drop(state_guard);

        let disk_used = wal_usage.disk_used_bytes as u64;

        if disk_used >= self.disk_capacity.as_u64() * 90 / 100 {
            self.background_reset_shards();
        }
        report_wal_usage(wal_usage);

        #[cfg(test)]
        {
            persist_successes.sort_by_key(|success| success.subrequest_id);
            persist_failures.sort_by_key(|failure| failure.subrequest_id);
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
    async fn open_replication_stream_inner(
        &self,
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
                "a replication stream between {leader_id} and {follower_id} is already opened"
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
        &self,
        open_fetch_stream_request: OpenFetchStreamRequest,
    ) -> IngestV2Result<ServiceStream<IngestV2Result<FetchMessage>>> {
        let queue_id = open_fetch_stream_request.queue_id();

        let mut state_guard = self.state.lock_partially().await?;

        let shard = state_guard.shards.get_mut(&queue_id).ok_or_else(|| {
            rate_limited_error!(limit_per_min=6, queue_id=%queue_id, "shard not found");
            IngestV2Error::ShardNotFound {
                shard_id: open_fetch_stream_request.shard_id().clone(),
            }
        })?;
        // An indexer can only know about a newly opened shard if it has been scheduled by the
        // control plane, which confirms that the shard was correctly opened in the
        // metastore.
        shard.is_advertisable = true;

        let shard_status_rx = shard.shard_status_rx.clone();
        let mrecordlog = self.state.mrecordlog();
        let (service_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog,
            shard_status_rx,
            get_batch_num_bytes(),
        );
        Ok(service_stream)
    }

    async fn open_observation_stream_inner(
        &self,
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
        &self,
        init_shards_request: InitShardsRequest,
    ) -> IngestV2Result<InitShardsResponse> {
        let mut state_guard =
            with_lock_metrics!(self.state.lock_fully().await, "init_shards", "write")?;

        if state_guard.status() != IngesterStatus::Ready {
            return Err(IngestV2Error::Internal("node decommissioned".to_string()));
        }
        let mut successes = Vec::with_capacity(init_shards_request.subrequests.len());
        let mut failures = Vec::new();
        let now = Instant::now();

        for subrequest in init_shards_request.subrequests {
            let init_primary_shard_result = self
                .init_primary_shard(
                    &mut state_guard.inner,
                    &mut state_guard.mrecordlog,
                    subrequest.shard().clone(),
                    &subrequest.doc_mapping_json,
                    now,
                    subrequest.validate_docs,
                )
                .await;
            if init_primary_shard_result.is_ok() {
                let success = InitShardSuccess {
                    subrequest_id: subrequest.subrequest_id,
                    shard: subrequest.shard,
                };
                successes.push(success);
            } else {
                let shard = subrequest.shard();
                let failure = InitShardFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: shard.index_uid.clone(),
                    source_id: shard.source_id.clone(),
                    shard_id: shard.shard_id.clone(),
                };
                failures.push(failure);
            }
        }
        let response = InitShardsResponse {
            successes,
            failures,
        };
        Ok(response)
    }

    async fn truncate_shards_inner(
        &self,
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
                state_guard.delete_shard(&queue_id, "indexer-rpc").await;
            } else {
                state_guard
                    .truncate_shard(&queue_id, truncate_up_to_position_inclusive, "indexer-rpc")
                    .await;
            }
        }
        let wal_usage = state_guard.mrecordlog.resource_usage();
        report_wal_usage(wal_usage);

        self.check_decommissioning_status(&mut state_guard);
        let truncate_response = TruncateShardsResponse {};
        Ok(truncate_response)
    }

    async fn close_shards_inner(
        &self,
        close_shards_request: CloseShardsRequest,
    ) -> IngestV2Result<CloseShardsResponse> {
        let mut state_guard =
            with_lock_metrics!(self.state.lock_partially().await, "close_shards", "write")?;

        let mut successes = Vec::with_capacity(close_shards_request.shard_pkeys.len());

        for shard_pkey in close_shards_request.shard_pkeys {
            let queue_id = shard_pkey.queue_id();

            if let Some(shard) = state_guard.shards.get_mut(&queue_id) {
                shard.close();
                successes.push(shard_pkey);
            }
        }
        info!("closed {} shards", successes.len());
        let response = CloseShardsResponse { successes };
        Ok(response)
    }

    async fn decommission_inner(
        &self,
        _decommission_request: DecommissionRequest,
    ) -> IngestV2Result<DecommissionResponse> {
        info!("decommissioning ingester");
        let mut state_guard = self.state.lock_partially().await?;

        for shard in state_guard.shards.values_mut() {
            shard.close();
        }
        state_guard.set_status(IngesterStatus::Decommissioning);
        self.check_decommissioning_status(&mut state_guard);

        Ok(DecommissionResponse {})
    }

    pub async fn debug_info(&self) -> JsonValue {
        let state_guard = match self.state.lock_fully().await {
            Ok(state_guard) => state_guard,
            Err(_) => {
                return json!({
                    "status": "initializing",
                    "shards": {},
                    "mrecordlog": {},
                });
            }
        };
        let mut per_index_shards_json: BTreeMap<IndexUid, Vec<JsonValue>> = BTreeMap::new();

        for (queue_id, shard) in &state_guard.shards {
            let Some((index_uid, source_id, shard_id)) = split_queue_id(queue_id) else {
                continue;
            };
            let mut shard_json = json!({
                "index_uid": index_uid,
                "source_id": source_id,
                "shard_id": shard_id,
                "state": shard.shard_state.as_json_str_name(),
                "replication_position_inclusive": shard.replication_position_inclusive,
                "truncation_position_inclusive": shard.truncation_position_inclusive,
            });
            match &shard.shard_type {
                IngesterShardType::Primary { follower_id, .. } => {
                    shard_json["type"] = json!("primary");
                    shard_json["leader_id"] = json!(self.self_node_id.to_string());
                    shard_json["follower_id"] = json!(follower_id.to_string());
                }
                IngesterShardType::Replica { leader_id } => {
                    shard_json["type"] = json!("replica");
                    shard_json["leader_id"] = json!(leader_id.to_string());
                    shard_json["follower_id"] = json!(self.self_node_id.to_string());
                }
                IngesterShardType::Solo => {
                    shard_json["type"] = json!("solo");
                    shard_json["leader_id"] = json!(self.self_node_id.to_string());
                }
            };
            per_index_shards_json
                .entry(index_uid.clone())
                .or_default()
                .push(shard_json);
        }
        json!({
            "status": state_guard.status().as_json_str_name(),
            "shards": per_index_shards_json,
            "mrecordlog":  state_guard.mrecordlog.summary(),
        })
    }
}

#[async_trait]
impl IngesterService for Ingester {
    async fn persist(&self, persist_request: PersistRequest) -> IngestV2Result<PersistResponse> {
        // If the request is local, the amount of memory it occupies is already
        // accounted for in the router.
        let request_size_bytes = persist_request
            .subrequests
            .iter()
            .flat_map(|subrequest| match &subrequest.doc_batch {
                Some(doc_batch) if doc_batch.doc_buffer.is_unique() => Some(doc_batch.num_bytes()),
                _ => None,
            })
            .sum::<usize>();
        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.ingester_persist);
        gauge_guard.add(request_size_bytes as i64);

        self.persist_inner(persist_request).await
    }

    async fn open_replication_stream(
        &self,
        syn_replication_stream: quickwit_common::ServiceStream<SynReplicationMessage>,
    ) -> IngestV2Result<IngesterServiceStream<AckReplicationMessage>> {
        self.open_replication_stream_inner(syn_replication_stream)
            .await
    }

    async fn open_fetch_stream(
        &self,
        open_fetch_stream_request: OpenFetchStreamRequest,
    ) -> IngestV2Result<ServiceStream<IngestV2Result<FetchMessage>>> {
        self.open_fetch_stream_inner(open_fetch_stream_request)
            .await
    }

    async fn open_observation_stream(
        &self,
        open_observation_stream_request: OpenObservationStreamRequest,
    ) -> IngestV2Result<IngesterServiceStream<ObservationMessage>> {
        self.open_observation_stream_inner(open_observation_stream_request)
            .await
    }

    async fn init_shards(
        &self,
        init_shards_request: InitShardsRequest,
    ) -> IngestV2Result<InitShardsResponse> {
        self.init_shards_inner(init_shards_request).await
    }

    async fn retain_shards(
        &self,
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
            state_guard
                .delete_shard(&queue_id, "control-plane-retain-shards-rpc")
                .await;
        }
        self.check_decommissioning_status(&mut state_guard);
        Ok(RetainShardsResponse {})
    }

    async fn truncate_shards(
        &self,
        truncate_shards_request: TruncateShardsRequest,
    ) -> IngestV2Result<TruncateShardsResponse> {
        self.truncate_shards_inner(truncate_shards_request).await
    }

    async fn close_shards(
        &self,
        close_shards_request: CloseShardsRequest,
    ) -> IngestV2Result<CloseShardsResponse> {
        self.close_shards_inner(close_shards_request).await
    }

    async fn decommission(
        &self,
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
                state_guard.delete_shard(&queue_id, "indexer-gossip").await;
            } else if !shard_position.is_beginning() {
                state_guard
                    .truncate_shard(&queue_id, shard_position, "indexer-gossip")
                    .await;
            }
        }
    }
}

pub async fn wait_for_ingester_status(
    ingester: impl IngesterService,
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

pub async fn wait_for_ingester_decommission(ingester: Ingester) -> anyhow::Result<()> {
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

struct PendingPersistSubrequest {
    queue_id: QueueId,
    subrequest_id: u32,
    index_uid: Option<IndexUid>,
    source_id: SourceId,
    shard_id: Option<ShardId>,
    doc_batch: DocBatchV2,
    parse_failures: Vec<ParseFailure>,
    expected_position_inclusive: Option<Position>,
    successfully_replicated: bool,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::mutable_key_type)]

    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, Ordering};

    use bytes::Bytes;
    use quickwit_cluster::{ChannelTransport, create_cluster_for_test_with_id};
    use quickwit_common::shared_consts::INGESTER_PRIMARY_SHARDS_PREFIX;
    use quickwit_common::tower::ConstantRate;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::control_plane::{AdviseResetShardsResponse, MockControlPlaneService};
    use quickwit_proto::ingest::ingester::{
        IngesterServiceGrpcServer, IngesterServiceGrpcServerAdapter, InitShardSubrequest,
        PersistSubrequest, TruncateShardsSubrequest,
    };
    use quickwit_proto::ingest::{
        DocBatchV2, ParseFailureReason, ShardIdPosition, ShardIdPositions, ShardIds, ShardPKey,
    };
    use quickwit_proto::types::{DocMappingUid, DocUid, ShardId, SourceUid, queue_id};
    use tokio::task::yield_now;
    use tokio::time::timeout;
    use tonic::transport::{Endpoint, Server};

    use super::*;
    use crate::MRecord;
    use crate::ingest_v2::DEFAULT_IDLE_SHARD_TIMEOUT;
    use crate::ingest_v2::broadcast::ShardInfos;
    use crate::ingest_v2::doc_mapper::try_build_doc_mapper;
    use crate::ingest_v2::fetch::tests::{into_fetch_eof, into_fetch_payload};

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
            let control_plane = ControlPlaneServiceClient::from_mock(mock_control_plane);

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

        pub fn with_memory_capacity(mut self, memory_capacity: ByteSize) -> Self {
            self.memory_capacity = memory_capacity;
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

            wait_for_ingester_status(ingester.clone(), IngesterStatus::Ready)
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
        assert!(solo_shard_02.is_advertisable);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_02, .., &[(1, [0, 0], "test-doc-bar")]);

        state_guard.rate_trackers.contains_key(&queue_id_02);

        assert_eq!(state_guard.status(), IngesterStatus::Ready);
    }

    #[tokio::test]
    async fn test_ingester_broadcasts_local_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let queue_id_00 = queue_id(&index_uid, "test-source", &ShardId::from(0));
        let shard_00 = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            Instant::now(),
            false,
        );
        state_guard.shards.insert(queue_id_00.clone(), shard_00);

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let mut shard_01 = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            Instant::now(),
            false,
        );
        shard_01.is_advertisable = true;
        state_guard.shards.insert(queue_id_01.clone(), shard_01);

        for queue_id in [&queue_id_00, &queue_id_01] {
            let rate_limiter = RateLimiter::from_settings(RateLimiterSettings::default());
            let rate_meter = RateMeter::default();
            state_guard
                .rate_trackers
                .insert(queue_id.clone(), (rate_limiter, rate_meter));
        }
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
        assert_eq!(shard_info.short_term_ingestion_rate, 0);

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
    async fn test_ingester_init_primary_shard() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}",
                "field_mappings": [{{
                        "name": "message",
                        "type": "text"
                }}]
            }}"#
        );
        let primary_shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                primary_shard,
                &doc_mapping_json,
                Instant::now(),
                true,
            )
            .await
            .unwrap();

        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_solo();
        shard.assert_is_open();
        shard.assert_replication_position(Position::Beginning);
        shard.assert_truncation_position(Position::Beginning);
        assert!(shard.doc_mapper_opt.is_some());
    }

    #[tokio::test]
    async fn test_ingester_init_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            follower_id: None,
            doc_mapping_uid: Some(doc_mapping_uid),
            publish_position_inclusive: None,
            publish_token: None,
            update_timestamp: 1724158996,
        };
        let init_shards_request = InitShardsRequest {
            subrequests: vec![InitShardSubrequest {
                subrequest_id: 0,
                shard: Some(shard.clone()),
                doc_mapping_json,
                validate_docs: true,
            }],
        };
        let response = ingester.init_shards(init_shards_request).await.unwrap();
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.failures.len(), 0);

        let init_shard_success = &response.successes[0];
        assert_eq!(init_shard_success.subrequest_id, 0);
        assert_eq!(init_shard_success.shard, Some(shard));

        let state_guard = ingester.state.lock_fully().await.unwrap();

        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_solo();
        shard.assert_is_open();
        shard.assert_replication_position(Position::Beginning);
        shard.assert_truncation_position(Position::Beginning);

        assert!(state_guard.rate_trackers.contains_key(&queue_id));
        assert!(state_guard.mrecordlog.queue_exists(&queue_id));
    }

    #[tokio::test]
    async fn test_ingester_persist() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index", 1);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![
                InitShardSubrequest {
                    subrequest_id: 0,
                    shard: Some(Shard {
                        index_uid: Some(index_uid.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        leader_id: ingester_ctx.node_id.to_string(),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json: doc_mapping_json.clone(),
                    validate_docs: true,
                },
                InitShardSubrequest {
                    subrequest_id: 1,
                    shard: Some(Shard {
                        index_uid: Some(index_uid2.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        leader_id: ingester_ctx.node_id.to_string(),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json,
                    validate_docs: true,
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
                    doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test([
                        r#"{"doc": "test-doc-110"}"#,
                        r#"{"doc": "test-doc-111"}"#,
                    ])),
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
            &[(0, [0, 0], r#"{"doc": "test-doc-010"}"#), (1, [0, 1], "")],
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
                (0, [0, 0], r#"{"doc": "test-doc-110"}"#),
                (1, [0, 0], r#"{"doc": "test-doc-111"}"#),
                (2, [0, 1], ""),
            ],
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_empty() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![InitShardSubrequest {
                subrequest_id: 0,
                shard: Some(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    doc_mapping_uid: Some(doc_mapping_uid),
                    ..Default::default()
                }),
                doc_mapping_json,
                validate_docs: true,
            }],
        };
        let response = ingester.init_shards(init_shards_request).await.unwrap();
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.failures.len(), 0);

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
                shard_id: Some(ShardId::from(0)),
                doc_batch: None,
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 1);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success = &persist_response.successes[0];
        assert_eq!(persist_success.subrequest_id, 0);
        assert_eq!(persist_success.index_uid(), &index_uid);
        assert_eq!(persist_success.source_id, "test-source");
        assert_eq!(persist_success.shard_id(), ShardId::from(0));
        assert_eq!(
            persist_success.replication_position_inclusive,
            Some(Position::Beginning)
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_validates_docs() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}",
                "mode": "strict",
                "field_mappings": [{{"name": "doc", "type": "text"}}]
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![InitShardSubrequest {
                subrequest_id: 0,
                shard: Some(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    doc_mapping_uid: Some(doc_mapping_uid),
                    ..Default::default()
                }),
                doc_mapping_json,
                validate_docs: true,
            }],
        };
        let response = ingester.init_shards(init_shards_request).await.unwrap();
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.failures.len(), 0);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(0)),
                doc_batch: Some(DocBatchV2::for_test([
                    "",                           // invalid
                    "[]",                         // invalid
                    r#"{"foo": "bar"}"#,          // invalid
                    r#"{"doc": "test-doc-000"}"#, // valid
                ])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 1);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success = &persist_response.successes[0];
        assert_eq!(persist_success.num_persisted_docs, 1);
        assert_eq!(persist_success.parse_failures.len(), 3);

        let parse_failure_0 = &persist_success.parse_failures[0];
        assert_eq!(parse_failure_0.doc_uid(), DocUid::for_test(0));
        assert_eq!(parse_failure_0.reason(), ParseFailureReason::InvalidJson);
        assert!(parse_failure_0.message.contains("parse JSON document"));

        let parse_failure_1 = &persist_success.parse_failures[1];
        assert_eq!(parse_failure_1.doc_uid(), DocUid::for_test(1));
        assert_eq!(parse_failure_1.reason(), ParseFailureReason::InvalidJson);
        assert!(parse_failure_1.message.contains("not an object"));

        let parse_failure_2 = &persist_success.parse_failures[2];
        assert_eq!(parse_failure_2.doc_uid(), DocUid::for_test(2));
        assert_eq!(parse_failure_2.reason(), ParseFailureReason::InvalidSchema);
        assert!(parse_failure_2.message.contains("not declared"));
    }

    #[tokio::test]
    async fn test_ingester_persist_doesnt_validates_docs_when_requested() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}",
                "mode": "strict",
                "field_mappings": [{{"name": "doc", "type": "text"}}]
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![InitShardSubrequest {
                subrequest_id: 0,
                shard: Some(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    doc_mapping_uid: Some(doc_mapping_uid),
                    ..Default::default()
                }),
                doc_mapping_json,
                validate_docs: false,
            }],
        };
        let response = ingester.init_shards(init_shards_request).await.unwrap();
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.failures.len(), 0);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(0)),
                doc_batch: Some(DocBatchV2::for_test([
                    "",                           // invalid
                    "[]",                         // invalid
                    r#"{"foo": "bar"}"#,          // invalid
                    r#"{"doc": "test-doc-000"}"#, // valid
                ])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 1);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success = &persist_response.successes[0];
        assert_eq!(persist_success.num_persisted_docs, 4);
        assert_eq!(persist_success.parse_failures.len(), 0);
    }

    #[tokio::test]
    async fn test_ingester_persist_checks_capacity_before_validating_docs() {
        let (ingester_ctx, ingester) = IngesterForTest::default()
            .with_memory_capacity(ByteSize(0))
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}",
                "mode": "strict",
                "field_mappings": [{{"name": "doc", "type": "text"}}]
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![InitShardSubrequest {
                subrequest_id: 0,
                shard: Some(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    doc_mapping_uid: Some(doc_mapping_uid),
                    ..Default::default()
                }),
                doc_mapping_json,
                validate_docs: true,
            }],
        };
        let response = ingester.init_shards(init_shards_request).await.unwrap();
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.failures.len(), 0);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(0)),
                doc_batch: Some(DocBatchV2::for_test(["", "[]", r#"{"foo": "bar"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.reason(), PersistFailureReason::WalFull);
    }

    #[tokio::test]
    async fn test_ingester_persist_applies_rate_limiting_before_validating_docs() {
        let (ingester_ctx, ingester) = IngesterForTest::default()
            .with_rate_limiter_settings(RateLimiterSettings {
                burst_limit: 0,
                rate_limit: ConstantRate::bytes_per_sec(ByteSize(0)),
                refill_period: Duration::from_secs(1),
            })
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}",
                "mode": "strict",
                "field_mappings": [{{"name": "doc", "type": "text"}}]
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![InitShardSubrequest {
                subrequest_id: 0,
                shard: Some(Shard {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    leader_id: ingester_ctx.node_id.to_string(),
                    doc_mapping_uid: Some(doc_mapping_uid),
                    ..Default::default()
                }),
                doc_mapping_json,
                validate_docs: true,
            }],
        };
        let response = ingester.init_shards(init_shards_request).await.unwrap();
        assert_eq!(response.successes.len(), 1);
        assert_eq!(response.failures.len(), 0);

        let persist_request = PersistRequest {
            leader_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(0)),
                doc_batch: Some(DocBatchV2::for_test(["", "[]", r#"{"foo": "bar"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.leader_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::ShardRateLimited
        );
    }

    // This test should be run manually and independently of other tests with the `failpoints`
    // feature enabled:
    // ```sh
    // cargo test --manifest-path quickwit/Cargo.toml -p quickwit-ingest --features failpoints -- test_ingester_persist_closes_shard_on_io_error
    // ```
    #[cfg(all(feature = "failpoints", not(feature = "no-failpoints")))]
    #[tokio::test]
    async fn test_ingester_persist_closes_shard_on_io_error() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("ingester:append_records", "return").unwrap();

        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let solo_shard = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            Instant::now(),
            false,
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
                doc_batch: Some(DocBatchV2::for_test([r#"test-doc-foo"#])),
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
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let doc_mapper = try_build_doc_mapper("{}").unwrap();

        // Insert a dangling shard, i.e. a shard without a corresponding queue.
        let solo_shard = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Some(doc_mapper),
            Instant::now(),
            false,
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
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-foo"}"#])),
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
        let (leader_ctx, leader) = IngesterForTest::default()
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

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![
                InitShardSubrequest {
                    subrequest_id: 0,
                    shard: Some(Shard {
                        index_uid: Some(index_uid.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        leader_id: leader_ctx.node_id.to_string(),
                        follower_id: Some(follower_ctx.node_id.to_string()),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json: doc_mapping_json.clone(),
                    validate_docs: true,
                },
                InitShardSubrequest {
                    subrequest_id: 1,
                    shard: Some(Shard {
                        index_uid: Some(index_uid2.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        leader_id: leader_ctx.node_id.to_string(),
                        follower_id: Some(follower_ctx.node_id.to_string()),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json,
                    validate_docs: true,
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
                    doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test([
                        r#"{"doc": "test-doc-110"}"#,
                        r#"{"doc": "test-doc-111"}"#,
                    ])),
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
            &[(0, [0, 0], r#"{"doc": "test-doc-010"}"#), (1, [0, 1], "")],
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
                (0, [0, 0], r#"{"doc": "test-doc-110"}"#),
                (1, [0, 0], r#"{"doc": "test-doc-111"}"#),
                (2, [0, 1], ""),
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
            &[(0, [0, 0], r#"{"doc": "test-doc-010"}"#), (1, [0, 1], "")],
        );

        let replica_shard_11 = follower_state_guard.shards.get(&queue_id_11).unwrap();
        replica_shard_11.assert_is_replica();
        replica_shard_11.assert_is_open();
        replica_shard_11.assert_replication_position(Position::offset(2u64));

        follower_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[
                (0, [0, 0], r#"{"doc": "test-doc-110"}"#),
                (1, [0, 0], r#"{"doc": "test-doc-111"}"#),
                (2, [0, 1], ""),
            ],
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_replicate_grpc() {
        let (leader_ctx, leader) = IngesterForTest::default()
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
            None,
        );

        leader_ctx
            .ingester_pool
            .insert(follower_ctx.node_id.clone(), follower_grpc_client);

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index", 1);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let init_shards_request = InitShardsRequest {
            subrequests: vec![
                InitShardSubrequest {
                    subrequest_id: 0,
                    shard: Some(Shard {
                        index_uid: Some(index_uid.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        leader_id: leader_ctx.node_id.to_string(),
                        follower_id: Some(follower_ctx.node_id.to_string()),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json: doc_mapping_json.clone(),
                    validate_docs: true,
                },
                InitShardSubrequest {
                    subrequest_id: 1,
                    shard: Some(Shard {
                        index_uid: Some(index_uid2.clone()),
                        source_id: "test-source".to_string(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        leader_id: leader_ctx.node_id.to_string(),
                        follower_id: Some(follower_ctx.node_id.to_string()),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json,
                    validate_docs: true,
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
                    doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test([
                        r#"{"doc": "test-doc-110"}"#,
                        r#"{"doc": "test-doc-111"}"#,
                    ])),
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
            &[(0, [0, 0], r#"{"doc": "test-doc-010"}"#)],
        );

        let queue_id_11 = queue_id(&index_uid2, "test-source", &ShardId::from(1));
        let primary_shard_11 = leader_state_guard.shards.get(&queue_id_11).unwrap();
        primary_shard_11.assert_is_primary();
        primary_shard_11.assert_is_open();
        primary_shard_11.assert_replication_position(Position::offset(1u64));

        leader_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[
                (0, [0, 0], r#"{"doc": "test-doc-110"}"#),
                (1, [0, 0], r#"{"doc": "test-doc-111"}"#),
            ],
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
            &[(0, [0, 0], r#"{"doc": "test-doc-010"}"#)],
        );

        let replica_shard_11 = follower_state_guard.shards.get(&queue_id_11).unwrap();
        replica_shard_11.assert_is_replica();
        replica_shard_11.assert_is_open();
        replica_shard_11.assert_replication_position(Position::offset(1u64));

        follower_state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[
                (0, [0, 0], r#"{"doc": "test-doc-110"}"#),
                (1, [0, 0], r#"{"doc": "test-doc-111"}"#),
            ],
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_shard_closed() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let solo_shard = IngesterShard::new_solo(
            ShardState::Closed,
            Position::Beginning,
            Position::Beginning,
            None,
            Instant::now(),
            false,
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
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
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
        let (ingester_ctx, ingester) = IngesterForTest::default()
            .with_rate_limiter_settings(RateLimiterSettings {
                burst_limit: 0,
                rate_limit: ConstantRate::bytes_per_sec(ByteSize(0)),
                refill_period: Duration::from_millis(100),
            })
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let primary_shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                primary_shard,
                &doc_mapping_json,
                Instant::now(),
                true,
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
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
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
            PersistFailureReason::ShardRateLimited
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
    async fn test_ingester_persist_resource_exhausted() {
        let (ingester_ctx, ingester) = IngesterForTest::default()
            .with_disk_capacity(ByteSize(0))
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let primary_shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: ingester_ctx.node_id.to_string(),
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                primary_shard,
                &doc_mapping_json,
                Instant::now(),
                true,
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
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
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
        assert_eq!(persist_failure.reason(), PersistFailureReason::WalFull);

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
        let (_ingester_ctx, ingester) = IngesterForTest::default()
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
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

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

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard,
                &doc_mapping_json,
                Instant::now(),
                true,
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

        let shard = state_guard.shards.get(&queue_id).unwrap();
        assert!(shard.is_advertisable);
        shard.notify_shard_status();
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
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));

        let doc_mapping_uid_01 = DocMappingUid::random();
        let doc_mapping_json_01 = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid_01}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid_01),
            ..Default::default()
        };

        let doc_mapping_uid_02 = DocMappingUid::random();
        let doc_mapping_json_02 = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid_02}"
            }}"#
        );
        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid_02),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                &doc_mapping_json_01,
                now,
                true,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                &doc_mapping_json_02,
                now,
                true,
            )
            .await
            .unwrap();

        assert_eq!(state_guard.shards.len(), 2);
        assert_eq!(state_guard.doc_mappers.len(), 2);

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
            .truncate_shards(truncate_shards_request)
            .await
            .unwrap();

        let state_guard = ingester.state.lock_fully().await.unwrap();

        assert_eq!(state_guard.shards.len(), 1);
        assert_eq!(state_guard.doc_mappers.len(), 1);

        assert!(state_guard.shards.contains_key(&queue_id_01));
        assert!(state_guard.doc_mappers.contains_key(&doc_mapping_uid_01));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(1, [0, 0], "test-doc-bar")]);
    }

    #[tokio::test]
    async fn test_ingester_truncate_shards_deletes_dangling_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let solo_shard = IngesterShard::new_solo(
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            None,
            Instant::now(),
            false,
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
                assert_eq!(request.ingester_id, "test-ingester");
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
        let control_plane = ControlPlaneServiceClient::from_mock(mock_control_plane);

        let (_ingester_ctx, mut ingester) = IngesterForTest::default()
            .with_control_plane(control_plane)
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
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
                &doc_mapping_json,
                now,
                true,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                &doc_mapping_json,
                now,
                true,
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
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_17 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(17)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };

        let shard_18 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(18)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
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
                &doc_mapping_json,
                now,
                true,
            )
            .await
            .unwrap();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_18,
                &doc_mapping_json,
                now,
                true,
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
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard,
                &doc_mapping_json,
                Instant::now(),
                true,
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
            shard_pkeys: vec![
                ShardPKey {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                },
                ShardPKey {
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1337)),
                },
            ],
        };
        let closed_shards_response = ingester
            .close_shards(close_shards_request.clone())
            .await
            .unwrap();
        assert_eq!(closed_shards_response.successes.len(), 1);

        let close_shard_success = &closed_shards_response.successes[0];
        assert_eq!(close_shard_success.index_uid(), &index_uid);
        assert_eq!(close_shard_success.source_id, "test-source");
        assert_eq!(close_shard_success.shard_id(), ShardId::from(1));

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
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

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
                None,
                Instant::now(),
                false,
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

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
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
                &doc_mapping_json,
                now,
                true,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                &doc_mapping_json,
                now,
                true,
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
            .assert_records_eq(&queue_id_01, .., &[(1, [0, 0], "test-doc-bar")]);

        assert!(!state_guard.shards.contains_key(&queue_id_02));
        assert!(!state_guard.mrecordlog.queue_exists(&queue_id_02));
    }

    #[tokio::test]
    async fn test_ingester_closes_idle_shards() {
        // The `CloseIdleShardsTask` task is already unit tested, so this test ensures the task is
        // correctly spawned upon starting an ingester.
        let idle_shard_timeout = Duration::from_millis(200);
        let (_ingester_ctx, ingester) = IngesterForTest::default()
            .with_idle_shard_timeout(idle_shard_timeout)
            .build()
            .await;

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                &doc_mapping_json,
                now - idle_shard_timeout,
                true,
            )
            .await
            .unwrap();

        drop(state_guard);

        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let state_guard = ingester.state.lock_partially().await.unwrap();
            let shard = state_guard.shards.get(&queue_id_01).unwrap();

            if shard.is_closed() {
                return;
            }
            drop(state_guard);
        }
        panic!("idle shard was not closed");
    }

    #[tokio::test]
    async fn test_ingester_debug_info() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid_0: IndexUid = IndexUid::for_test("test-index-0", 0);
        let index_uid_1: IndexUid = IndexUid::for_test("test-index-1", 0);

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid_0.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: Some(index_uid_0.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let shard_03 = Shard {
            index_uid: Some(index_uid_1.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully().await.unwrap();
        let now = Instant::now();

        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                &doc_mapping_json,
                now,
                true,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_02,
                &doc_mapping_json,
                now,
                true,
            )
            .await
            .unwrap();
        ingester
            .init_primary_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_03,
                &doc_mapping_json,
                now,
                true,
            )
            .await
            .unwrap();
        drop(state_guard);

        let debug_info = ingester.debug_info().await;
        assert_eq!(debug_info["status"], "ready");

        let shards = &debug_info["shards"];
        assert_eq!(shards.as_object().unwrap().len(), 2);

        assert_eq!(
            shards["test-index-0:00000000000000000000000000"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            shards["test-index-1:00000000000000000000000000"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
}
