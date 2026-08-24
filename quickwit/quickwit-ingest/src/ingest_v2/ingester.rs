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

use async_trait::async_trait;
use bytesize::ByteSize;
use futures::StreamExt;
use mrecordlog::error::CreateQueueError;
use quickwit_cluster::Cluster;
use quickwit_common::metrics::IN_FLIGHT_INGESTER_PERSIST;
use quickwit_common::pretty::PrettyDisplay;
use quickwit_common::pubsub::{EventBroker, EventSubscriber};
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::{ServiceStream, rate_limited_error, rate_limited_warn};
use quickwit_metrics::{GaugeGuard, counter, label_values};
use quickwit_proto::control_plane::{
    AdviseResetShardsRequest, ControlPlaneService, ControlPlaneServiceClient,
};
use quickwit_proto::indexing::ShardPositionsUpdate;
use quickwit_proto::ingest::ingester::*;
use quickwit_proto::ingest::{
    CommitTypeV2, DocBatchV2, IngestV2Error, IngestV2Result, ParseFailure, Shard, ShardIds,
};
use quickwit_proto::types::{
    IndexUid, NodeId, Position, QueueId, ShardId, SourceId, SubrequestId, queue_id, split_queue_id,
};
use serde_json::{Value as JsonValue, json};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tracing::{Span, debug, error, info, instrument, warn};

use super::broadcast::{BroadcastIngesterCapacityScoreTask, BroadcastLocalShardsTask};
use super::doc_mapper::validate_doc_batch;
use super::fetch::FetchStreamTask;
use super::idle::CloseIdleShardsTask;
use super::models::IngesterShard;
use super::mrecordlog_utils::{
    AppendDocBatchError, append_non_empty_doc_batch, check_enough_capacity, wal_stats,
};
use super::rate_meter::RateMeter;
use super::state::{IngesterState, InnerIngesterState, WeakIngesterState};
use crate::estimate_size;
use crate::ingest_v2::doc_mapper::get_or_try_build_doc_mapper;
use crate::ingest_v2::metrics::{
    RESET_SHARDS_OPERATIONS_TOTAL, STATUS, report_wal_limits, report_wal_usage,
};
use crate::metrics::{DOCS_BYTES_TOTAL, DOCS_TOTAL, VALIDITY};
use crate::mrecordlog_async::MultiRecordLogAsync;

/// Minimum interval between two reset shards operations.
const MIN_RESET_SHARDS_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::ZERO
} else {
    Duration::from_mins(1)
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
    quickwit_common::get_from_env_cached!(
        usize,
        "QW_INGEST_BATCH_NUM_BYTES",
        DEFAULT_BATCH_NUM_BYTES,
        false
    )
}

#[derive(Clone)]
pub struct Ingester {
    self_node_id: NodeId,
    control_plane: ControlPlaneServiceClient,
    state: IngesterState,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
    rate_limiter_settings: RateLimiterSettings,
    // This semaphore ensures that the ingester that not run two reset shards operations
    // concurrently.
    reset_shards_permits: Arc<Semaphore>,
}

impl fmt::Debug for Ingester {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Ingester").finish()
    }
}

impl Ingester {
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        cluster: Cluster,
        control_plane: ControlPlaneServiceClient,
        wal_dir_path: &Path,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
        idle_shard_timeout: Duration,
    ) -> IngestV2Result<Self> {
        let self_node_id: NodeId = cluster.self_node_id();
        let state = IngesterState::load(
            cluster.clone(),
            wal_dir_path,
            disk_capacity,
            memory_capacity,
            rate_limiter_settings,
        )
        .await;

        let weak_state = state.weak();
        BroadcastLocalShardsTask::spawn(cluster.clone(), weak_state.clone());
        BroadcastIngesterCapacityScoreTask::spawn(cluster, weak_state.clone());
        CloseIdleShardsTask::spawn(weak_state, idle_shard_timeout);

        report_wal_limits(disk_capacity, memory_capacity);

        let ingester = Self {
            self_node_id,
            control_plane,
            state,
            disk_capacity,
            memory_capacity,
            rate_limiter_settings,
            reset_shards_permits: Arc::new(Semaphore::new(1)),
        };
        ingester.background_reset_shards();

        Ok(ingester)
    }

    /// Initializes a shard by creating a queue in the write-ahead log and inserting a new
    /// [`IngesterShard`] into the ingester state.
    async fn init_shard(
        &self,
        state: &mut InnerIngesterState,
        mrecordlog: &mut MultiRecordLogAsync,
        shard: Shard,
        doc_mapping_json: &str,
        now: Instant,
        validate_docs: bool,
    ) -> IngestV2Result<()> {
        let queue_id = shard.queue_id();
        info!(
            index_uid=%shard.index_uid(),
            source_id=shard.source_id,
            shard_id=%shard.shard_id(),
            "init shard"
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
        let index_uid = shard.index_uid().clone();
        let source_id = shard.source_id.clone();
        let shard_id = shard.shard_id().clone();
        let rate_limiter = RateLimiter::from_settings(self.rate_limiter_settings);
        let rate_meter = RateMeter::default();

        let shard = IngesterShard::builder(index_uid, source_id, shard_id)
            .with_rate_limiter(rate_limiter)
            .with_rate_meter(rate_meter)
            .with_doc_mapper(doc_mapper)
            .with_validate_docs(validate_docs)
            .with_last_write(now)
            .build();
        entry.insert(shard);
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
    #[instrument(name = "ingester.reset_shards", skip_all)]
    async fn reset_shards(&mut self) {
        let Ok(_permit) = self.reset_shards_permits.try_acquire() else {
            return;
        };
        self.state.wait_for_ready().await;

        info!("resetting shards");
        let now = Instant::now();

        let mut per_source_shard_ids: HashMap<(IndexUid, SourceId), Vec<ShardId>> = HashMap::new();

        let state_guard = self
            .state
            .lock_fully("reset_shards_init")
            .await
            .expect("ingester should be ready");

        for queue_id in state_guard.mrecordlog.list_queues() {
            let Some((index_uid, source_id, shard_id)) = split_queue_id(queue_id) else {
                // `split_queue_id` already logs an error.
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
                let mut state_guard = self
                    .state
                    .lock_fully("reset_shards_apply")
                    .await
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
                counter!(
                    parent: RESET_SHARDS_OPERATIONS_TOTAL,
                    labels: [label_values!(STATUS => "success")],
                )
                .inc();

                let wal_usage = state_guard.mrecordlog.resource_usage();
                report_wal_usage(wal_usage, self.disk_capacity, self.memory_capacity);
            }
            Ok(Err(error)) => {
                warn!("advise reset shards request failed: {error}");

                counter!(
                    parent: RESET_SHARDS_OPERATIONS_TOTAL,
                    labels: [label_values!(STATUS => "error")],
                )
                .inc();
            }
            Err(_) => {
                warn!("advise reset shards request timed out");

                counter!(
                    parent: RESET_SHARDS_OPERATIONS_TOTAL,
                    labels: [label_values!(STATUS => "timeout")],
                )
                .inc();
            }
        };
        // We still hold the permit while sleeping so we effectively rate limit the reset shards
        // operation to once per [`MIN_RESET_SHARDS_INTERVAL`].
        if let Some(sleep_for) = MIN_RESET_SHARDS_INTERVAL.checked_sub(now.elapsed()) {
            sleep(sleep_for).await;
        }
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
        if persist_request.ingester_id != self.self_node_id {
            return Err(IngestV2Error::Internal(format!(
                "routing error: expected ingester ID `{}`, got `{}`",
                self.self_node_id, persist_request.ingester_id,
            )));
        }
        let mut persist_successes = Vec::with_capacity(persist_request.subrequests.len());
        let mut persist_failures = Vec::new();
        let mut pending_persist_subrequests: HashMap<SubrequestId, PendingPersistSubrequest> =
            HashMap::with_capacity(persist_request.subrequests.len());

        // Keep track of the shards that need to be closed following an IO error.
        let mut shards_to_close: HashSet<QueueId> = HashSet::new();

        // Keep track of dangling shards, i.e., shards for which there is no longer a corresponding
        // queue in the WAL and should be deleted.
        let mut shards_to_delete: HashSet<QueueId> = HashSet::new();

        let commit_type = persist_request.commit_type();
        let force_commit = commit_type == CommitTypeV2::Force;

        let mut state_guard = self.state.lock_fully("persist").await?;
        let status = state_guard.status();

        if !status.accepts_write_requests() {
            persist_failures.reserve_exact(persist_request.subrequests.len());

            for subrequest in persist_request.subrequests {
                let persist_failure = PersistFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    reason: PersistFailureReason::NodeUnavailable as i32,
                };
                persist_failures.push(persist_failure);
            }
            let persist_response = PersistResponse {
                ingester_id: persist_request.ingester_id,
                successes: Vec::new(),
                failures: persist_failures,
                routing_update: None,
            };
            return Ok(persist_response);
        }
        // first verify if we would locally accept each subrequest
        {
            let mut total_requested_capacity = ByteSize::b(0);

            for subrequest in persist_request.subrequests {
                let Some(shard) = state_guard
                    .inner
                    .find_most_capacity_shard_mut(subrequest.index_uid(), &subrequest.source_id)
                else {
                    warn!(
                        index_uid=%subrequest.index_uid(),
                        source_id=%subrequest.source_id,
                        "no open shard found on ingester"
                    );
                    let persist_failure = PersistFailure {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        reason: PersistFailureReason::NoShardsAvailable as i32,
                    };
                    persist_failures.push(persist_failure);
                    continue;
                };
                let shard_id = shard.shard_id.clone();

                // A router can only know about a newly opened shard if it has been informed by the
                // control plane, which confirms that the shard was correctly opened in the
                // metastore.
                shard.is_advertisable = true;
                let doc_mapper = shard.doc_mapper_opt.clone().expect("shard should be open");
                let validate_docs = shard.validate_docs;
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
                        reason: PersistFailureReason::WalFull as i32,
                    };
                    persist_failures.push(persist_failure);
                    continue;
                };
                // Because we return the shard with the most available capacity, if this hits, it
                // means that no shard can receive this request, and it should be retried.
                if !shard.rate_limiter.acquire_bytes(requested_capacity) {
                    debug!(
                        "failed to persist records to shard `{}`: rate limited",
                        shard.queue_id()
                    );

                    let persist_failure = PersistFailure {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        reason: PersistFailureReason::NoShardsAvailable as i32,
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
                    counter!(
                        parent: DOCS_TOTAL,
                        labels: [label_values!(VALIDITY => "invalid")],
                    )
                    .inc_by(parse_failures.len() as u64);
                    counter!(
                        parent: DOCS_BYTES_TOTAL,
                        labels: [label_values!(VALIDITY => "invalid")],
                    )
                    .inc_by(original_batch_num_bytes);
                    let persist_success = PersistSuccess {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        shard_id: Some(shard_id),
                        replication_position_inclusive: Some(from_position_exclusive),
                        num_persisted_docs: 0,
                        parse_failures,
                    };
                    persist_successes.push(persist_success);
                    continue;
                };

                counter!(
                    parent: DOCS_TOTAL,
                    labels: [label_values!(VALIDITY => "valid")],
                )
                .inc_by(valid_doc_batch.num_docs() as u64);
                counter!(
                    parent: DOCS_BYTES_TOTAL,
                    labels: [label_values!(VALIDITY => "valid")],
                )
                .inc_by(valid_doc_batch.num_bytes() as u64);
                if !parse_failures.is_empty() {
                    counter!(
                        parent: DOCS_TOTAL,
                        labels: [label_values!(VALIDITY => "invalid")],
                    )
                    .inc_by(parse_failures.len() as u64);
                    counter!(
                        parent: DOCS_BYTES_TOTAL,
                        labels: [label_values!(VALIDITY => "invalid")],
                    )
                    .inc_by(original_batch_num_bytes - valid_doc_batch.num_bytes() as u64);
                }
                let valid_batch_num_bytes = valid_doc_batch.num_bytes() as u64;
                shard.rate_meter.update(valid_batch_num_bytes);
                total_requested_capacity += requested_capacity;

                let pending_persist_subrequest = PendingPersistSubrequest {
                    queue_id: shard.queue_id(),
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: Some(shard_id),
                    doc_batch: valid_doc_batch,
                    parse_failures,
                };
                pending_persist_subrequests.insert(
                    pending_persist_subrequest.subrequest_id,
                    pending_persist_subrequest,
                );
            }
        }
        // finally write locally
        {
            let now = Instant::now();
            for subrequest in pending_persist_subrequests.into_values() {
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
                                PersistFailureReason::NodeUnavailable
                            }
                            AppendDocBatchError::QueueNotFound(_) => {
                                error!(
                                    "failed to persist records to shard `{queue_id}`: WAL queue \
                                     not found"
                                );
                                shards_to_delete.insert(queue_id);
                                PersistFailureReason::NodeUnavailable
                            }
                        };
                        let persist_failure = PersistFailure {
                            subrequest_id: subrequest.subrequest_id,
                            index_uid: subrequest.index_uid,
                            source_id: subrequest.source_id,
                            reason: reason as i32,
                        };
                        persist_failures.push(persist_failure);
                        continue;
                    }
                };

                state_guard
                    .shards
                    .get_mut(&queue_id)
                    .expect("shard should exist")
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
                warn!("deleted dangling shard `{queue_id}`");
            }
        }
        let wal_usage = state_guard.mrecordlog.resource_usage();
        let disk_used = wal_usage.disk_used_bytes as u64;
        let memory_used = wal_usage.memory_used_bytes as u64;
        let (open_shard_counts, closed_shards) = state_guard.get_shard_snapshot();
        let capacity_score = state_guard
            .wal_capacity_tracker
            .score(ByteSize::b(disk_used), ByteSize::b(memory_used))
            as u32;
        drop(state_guard);

        if disk_used >= self.disk_capacity.as_u64() * 90 / 100 {
            self.background_reset_shards();
        }
        report_wal_usage(wal_usage, self.disk_capacity, self.memory_capacity);

        let source_shard_updates = open_shard_counts
            .into_iter()
            .map(|(index_uid, source_id, count)| SourceShardUpdate {
                index_uid: Some(index_uid),
                source_id,
                open_shard_count: count as u32,
            })
            .collect();

        let routing_update = RoutingUpdate {
            capacity_score,
            source_shard_updates,
            closed_shards,
        };

        #[cfg(test)]
        {
            persist_successes.sort_by_key(|success| success.subrequest_id);
            persist_failures.sort_by_key(|failure| failure.subrequest_id);
        }
        let ingester_id = self.self_node_id.to_string();
        let persist_response = PersistResponse {
            ingester_id,
            successes: persist_successes,
            failures: persist_failures,
            routing_update: Some(routing_update),
        };
        Ok(persist_response)
    }

    async fn open_fetch_stream_inner(
        &self,
        open_fetch_stream_request: OpenFetchStreamRequest,
    ) -> IngestV2Result<ServiceStream<IngestV2Result<FetchMessage>>> {
        let queue_id = open_fetch_stream_request.queue_id();

        let mut state_guard = self.state.lock_partially("open_fetch_stream").await?;

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
        let mrecordlog = self.state.mrecordlog();
        let observation_stream =
            ServiceStream::new(Box::pin(Box::pin(status_stream.then(move |status| {
                let self_node_id = self_node_id.clone();
                let mrecordlog = mrecordlog.clone();
                async move {
                    let mrecordlog_guard = mrecordlog.read().await;
                    let (wal_memory_used_bytes, wal_disk_used_bytes, wal_num_records) =
                        wal_stats(mrecordlog_guard.as_ref());
                    let observation_message = ObservationMessage {
                        node_id: self_node_id.to_string(),
                        status: status as i32,
                        wal_memory_used_bytes,
                        wal_disk_used_bytes,
                        wal_num_records,
                    };
                    Ok(observation_message)
                }
            }))));
        Ok(observation_stream)
    }

    async fn init_shards_inner(
        &self,
        init_shards_request: InitShardsRequest,
    ) -> IngestV2Result<InitShardsResponse> {
        let mut state_guard = self.state.lock_fully("init_shards").await?;
        let status = state_guard.status();

        if !status.accepts_write_requests() {
            let error = IngestV2Error::Unavailable(format!(
                "ingester {} is not ready: {status}",
                self.self_node_id
            ));
            return Err(error);
        }
        let mut successes = Vec::with_capacity(init_shards_request.subrequests.len());
        let mut failures = Vec::new();
        let now = Instant::now();

        for subrequest in init_shards_request.subrequests {
            let init_shard_result = self
                .init_shard(
                    &mut state_guard.inner,
                    &mut state_guard.mrecordlog,
                    subrequest.shard().clone(),
                    &subrequest.doc_mapping_json,
                    now,
                    subrequest.validate_docs,
                )
                .await;
            if init_shard_result.is_ok() {
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
        let mut state_guard = self.state.lock_fully("truncate_shards_rpc").await?;

        for subrequest in truncate_shards_request.subrequests {
            let queue_id = subrequest.queue_id();
            let truncate_up_to_position_inclusive = subrequest.truncate_up_to_position_inclusive();

            // We deliberately do NOT delete the shard when the indexer truncates up to EOF over
            // this gRPC path. Shard deletion is driven solely by the `ShardPositionsUpdate` gossip
            // event (see the `EventSubscriber<ShardPositionsUpdate>` impl below), which is the same
            // signal the control plane uses to delete the shard from the metastore and its model.
            //
            // Handling shard deletion through that single, shared signal keeps the ingester and
            // control plane views consistent: the ingester never removes a shard the
            // control plane does not also remove.
            state_guard
                .truncate_shard(&queue_id, truncate_up_to_position_inclusive, "indexer RPC")
                .await;
        }
        let wal_usage = state_guard.mrecordlog.resource_usage();
        report_wal_usage(wal_usage, self.disk_capacity, self.memory_capacity);

        state_guard.check_decommissioning_status().await;
        let truncate_response = TruncateShardsResponse {};
        Ok(truncate_response)
    }

    async fn close_shards_inner(
        &self,
        close_shards_request: CloseShardsRequest,
    ) -> IngestV2Result<CloseShardsResponse> {
        let mut state_guard = self.state.lock_partially("close_shards").await?;

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

    pub async fn debug_info(&self) -> JsonValue {
        let state_guard = match self.state.lock_fully("debug_info").await {
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
                // `split_queue_id` already logs an error.
                continue;
            };
            let shard_json = json!({
                "index_uid": index_uid,
                "source_id": source_id,
                "shard_id": shard_id,
                "state": shard.shard_state.as_json_str_name(),
                "replication_position_inclusive": shard.replication_position_inclusive,
                "truncation_position_inclusive": shard.truncation_position_inclusive,
                "type": "solo",
                "ingester_id": self.self_node_id.to_string(),
            });
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
        let _gauge_guard = GaugeGuard::new(&IN_FLIGHT_INGESTER_PERSIST, request_size_bytes as f64);

        self.persist_inner(persist_request).await
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
        let mut state_guard = self.state.lock_fully("retain_shards").await?;
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
        state_guard.check_decommissioning_status().await;
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
        _decommission_request: DecommissionRequest,
    ) -> IngestV2Result<DecommissionResponse> {
        // Retire the ingester immediately by setting its status to `Retiring`.
        info!("retiring ingester");
        let mut state_guard = self.state.lock_partially("retire").await?;
        state_guard.set_status(IngesterStatus::Retiring).await;
        drop(state_guard); // Dropping explicitly for readability.

        // Drain write requests by scheduling the decommissioning of the ingester after a delay
        // allowing the propagation of the `Retiring` status to other nodes.
        let self_clone = self.clone();
        tokio::spawn(async move {
            const DECOMMISSION_DELAY: Duration = if cfg!(any(test, feature = "testsuite")) {
                Duration::from_millis(200)
            } else {
                // Having to wait for 15s is not great but we can live with it. During this time, we
                // still make progress towards decommissioning because we gradually receive less
                // write requests and indexing is still ongoing. However, it sets a floor on the
                // amount of time with which we can fully decommission an ingester. This will be
                // most noticeable when using Quickwit locally.
                Duration::from_secs(15)
            };
            tokio::time::sleep(DECOMMISSION_DELAY).await;

            info!("decommissioning ingester");
            let mut state_guard = match self_clone.state.lock_partially("decommission").await {
                Ok(state_guard) => state_guard,
                Err(error) => {
                    error!(%error, "failed to decommission ingester");
                    return;
                }
            };
            state_guard
                .set_status(IngesterStatus::Decommissioning)
                .await;

            for shard in state_guard.shards.values_mut() {
                shard.close();
            }
            state_guard.check_decommissioning_status().await;
        });
        Ok(DecommissionResponse {})
    }
}

#[async_trait]
impl EventSubscriber<ShardPositionsUpdate> for WeakIngesterState {
    #[instrument(name = "ingester.truncate_shards_gossip", skip_all)]
    async fn handle_event(&mut self, shard_positions_update: ShardPositionsUpdate) {
        let Some(state) = self.upgrade() else {
            debug!("ingester was dropped: exiting");
            return;
        };
        let local_updates = filter_local_shard_updates(&state, shard_positions_update).await;

        if local_updates.is_empty() {
            return;
        }
        // We're in no rush to process the updates, so yield to avoid starving other tasks waiting
        // for the lock.
        tokio::task::yield_now().await;

        apply_local_shard_updates(&state, local_updates).await;
    }
}

/// The gossiped update is not scoped to this ingester: it carries the positions of every shard
/// of the source, most of which are typically hosted by other ingesters. This function filters
/// down to the updates that will actually mutate our local state, using the cheap partial lock
/// (`inner` only), sparing the caller from taking the full lock, which also holds the WAL write
/// lock contended by persist/fetch operations on unrelated shards. The per-entry conditions
/// below mirror the no-op checks performed by `delete_shard` and `truncate_shard` so we can
/// discard useless entries without ever taking the full lock.
#[instrument(
    name = "ingester.filter_local_shard_updates",
    skip_all,
    fields(num_global_updates, num_local_updates)
)]
async fn filter_local_shard_updates(
    state: &IngesterState,
    shard_positions_update: ShardPositionsUpdate,
) -> Vec<(QueueId, Position)> {
    let index_uid = shard_positions_update.source_uid.index_uid;
    let source_id = shard_positions_update.source_uid.source_id;

    let Ok(state_guard) = state.lock_partially("filter_local_shard_updates").await else {
        debug!("ingester was dropped: exiting");
        return Vec::new();
    };
    let num_global_updates = shard_positions_update.updated_shard_positions.len();

    let local_updates: Vec<(QueueId, Position)> = shard_positions_update
        .updated_shard_positions
        .into_iter()
        .map(|(shard_id, shard_position)| {
            (queue_id(&index_uid, &source_id, &shard_id), shard_position)
        })
        .filter(|(queue_id, shard_position)| {
            let Some(shard) = state_guard.shards.get(queue_id) else {
                return false;
            };
            if shard_position.is_eof() {
                return true;
            }
            if shard_position.is_beginning() {
                return false;
            }
            shard.truncation_position_inclusive < *shard_position
        })
        .collect();

    Span::current().record("num_global_updates", num_global_updates);
    Span::current().record("num_local_updates", local_updates.len());

    debug!(
        "filtered out {} of {num_global_updates} shard position update(s)",
        num_global_updates - local_updates.len(),
    );
    local_updates
}

#[instrument(
    name = "ingester.apply_local_shard_updates",
    skip_all,
    fields(num_deleted_shards, num_truncated_shards)
)]
async fn apply_local_shard_updates(state: &IngesterState, local_updates: Vec<(QueueId, Position)>) {
    let now = Instant::now();

    let Ok(mut state_guard) = state.lock_fully("apply_local_shard_updates").await else {
        debug!("ingester was dropped: exiting");
        return;
    };
    let mut num_deleted_shards = 0;
    let mut num_truncated_shards = 0;

    for (queue_id, shard_position) in local_updates {
        if shard_position.is_eof() {
            state_guard.delete_shard(&queue_id, "indexer gossip").await;
            num_deleted_shards += 1;
        } else if !shard_position.is_beginning() {
            state_guard
                .truncate_shard(&queue_id, shard_position, "indexer gossip")
                .await;
            num_truncated_shards += 1;
        }
    }
    state_guard.check_decommissioning_status().await;

    Span::current().record("num_deleted_shards", num_deleted_shards);
    Span::current().record("num_truncated_shards", num_truncated_shards);

    info!(
        "deleted {} and truncated {} shard(s) via gossip in {}",
        num_deleted_shards,
        num_truncated_shards,
        now.elapsed().pretty_display()
    );
}

struct PendingPersistSubrequest {
    queue_id: QueueId,
    subrequest_id: u32,
    index_uid: Option<IndexUid>,
    source_id: SourceId,
    shard_id: Option<ShardId>,
    doc_batch: DocBatchV2,
    parse_failures: Vec<ParseFailure>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::mutable_key_type)]

    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU16, Ordering};

    use bytes::Bytes;
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test_with_id};
    use quickwit_common::shared_consts::INGESTER_SHARDS_PREFIX;
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_common::tower::ConstantRate;
    use quickwit_config::service::QuickwitService;
    use quickwit_proto::control_plane::{AdviseResetShardsResponse, MockControlPlaneService};
    use quickwit_proto::ingest::ingester::{
        IngesterStatus, InitShardSubrequest, PersistSubrequest, TruncateShardsSubrequest,
    };
    use quickwit_proto::ingest::{
        DocBatchV2, ParseFailureReason, ShardIdPosition, ShardIdPositions, ShardIds, ShardPKey,
        ShardState,
    };
    use quickwit_proto::types::{DocMappingUid, DocUid, ShardId, SourceUid, queue_id};
    use tokio::time::timeout;

    use super::*;
    use crate::MRecord;
    use crate::ingest_v2::DEFAULT_IDLE_SHARD_TIMEOUT;
    use crate::ingest_v2::broadcast::ShardInfos;
    use crate::ingest_v2::doc_mapper::try_build_doc_mapper;
    use crate::ingest_v2::fetch::tests::{into_fetch_eof, into_fetch_payload};
    use crate::ingest_v2::helpers::wait_for_ingester_status;

    pub(super) struct IngesterForTest {
        node_id: NodeId,
        control_plane: ControlPlaneServiceClient,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
        rate_limiter_settings: RateLimiterSettings,
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
                node_id: NodeId::from_str("test-ingester"),
                control_plane,
                disk_capacity: ByteSize::mb(256),
                memory_capacity: ByteSize::mb(1),
                rate_limiter_settings: RateLimiterSettings::default(),
                idle_shard_timeout: DEFAULT_IDLE_SHARD_TIMEOUT,
            }
        }
    }

    impl IngesterForTest {
        pub fn with_control_plane(mut self, control_plane: ControlPlaneServiceClient) -> Self {
            self.control_plane = control_plane;
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

        pub fn with_idle_shard_timeout(mut self, idle_shard_timeout: Duration) -> Self {
            self.idle_shard_timeout = idle_shard_timeout;
            self
        }

        pub async fn build(self) -> (IngesterContext, Ingester) {
            static GOSSIP_ADVERTISE_PORT_SEQUENCE: AtomicU16 = AtomicU16::new(1u16);

            let tempdir = tempfile::tempdir().unwrap();
            let wal_dir_path = tempdir.path();
            let transport = ChitchatTransport::default();

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
                wal_dir_path,
                self.disk_capacity,
                self.memory_capacity,
                self.rate_limiter_settings,
                self.idle_shard_timeout,
            )
            .await
            .unwrap();

            wait_for_ingester_status(&ingester, IngesterStatus::Ready, Duration::from_secs(1))
                .await
                .unwrap();

            let ingester_env = IngesterContext {
                tempdir,
                _transport: transport,
                node_id: self.node_id,
                cluster,
            };
            (ingester_env, ingester)
        }
    }

    pub struct IngesterContext {
        tempdir: tempfile::TempDir,
        _transport: ChitchatTransport,
        node_id: NodeId,
        cluster: Cluster,
    }

    #[tokio::test]
    async fn test_ingester_init() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));
        let queue_id_03 = queue_id(&index_uid, &source_id, &ShardId::from(3));

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

        state_guard.set_status(IngesterStatus::Initializing).await;

        drop(state_guard);

        ingester
            .state
            .init(
                ingester_ctx.tempdir.path(),
                ByteSize::mb(256),
                ByteSize::mb(1),
                RateLimiterSettings::default(),
            )
            .await;

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 3);

        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        shard_01.assert_is_closed();
        shard_01.assert_replication_position(Position::offset(0u64));
        shard_01.assert_truncation_position(Position::offset(0u64));
        assert!(shard_01.is_advertisable);

        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        shard_02.assert_is_closed();
        shard_02.assert_replication_position(Position::offset(1u64));
        shard_02.assert_truncation_position(Position::offset(0u64));
        assert!(shard_02.is_advertisable);

        let shard_03 = state_guard.shards.get(&queue_id_03).unwrap();
        shard_03.assert_is_closed();
        shard_03.assert_replication_position(Position::Beginning);
        shard_03.assert_truncation_position(Position::Beginning);
        assert!(shard_03.is_advertisable);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_02, .., &[(1, [0, 0], "test-doc-bar")]);

        assert_eq!(state_guard.status(), IngesterStatus::Ready);
    }

    #[tokio::test]
    async fn test_ingester_broadcasts_local_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let shard_00 =
            IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(0)).build();
        state_guard.shards.insert(shard_00.queue_id(), shard_00);

        let shard_01 = IngesterShard::builder(index_uid.clone(), source_id, ShardId::from(1))
            .advertisable()
            .build();
        let queue_id_01 = shard_01.queue_id();
        state_guard.shards.insert(queue_id_01.clone(), shard_01);
        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let key = format!("{INGESTER_SHARDS_PREFIX}{}:{}", index_uid, "test-source");
        let value = ingester_ctx.cluster.get_self_key_value(&key).await.unwrap();

        let shard_infos: ShardInfos = serde_json::from_str(&value).unwrap();
        assert_eq!(shard_infos.len(), 1);

        let shard_info = shard_infos.iter().next().unwrap();
        assert_eq!(shard_info.shard_id, ShardId::from(1));
        assert_eq!(shard_info.shard_state, ShardState::Open);
        assert_eq!(shard_info.short_term_ingestion_rate, 0);

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
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

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        state_guard.shards.remove(&queue_id_01).unwrap();
        drop(state_guard);

        tokio::time::sleep(Duration::from_millis(100)).await;

        let value_opt = ingester_ctx.cluster.get_self_key_value(&key).await;
        assert!(value_opt.is_none());
    }

    #[tokio::test]
    async fn test_ingester_init_shard() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

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
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ingester_id: ingester_ctx.node_id.to_string(),
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

        ingester
            .init_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard,
                &doc_mapping_json,
                Instant::now(),
                true,
            )
            .await
            .unwrap();

        let queue_id = queue_id(&index_uid, &source_id, &ShardId::from(1));
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_open();
        shard.assert_replication_position(Position::Beginning);
        shard.assert_truncation_position(Position::Beginning);
        assert!(shard.doc_mapper_opt.is_some());
    }

    #[tokio::test]
    async fn test_ingester_init_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ingester_id: ingester_ctx.node_id.to_string(),
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

        let state_guard = ingester.state.lock_fully("test").await.unwrap();

        let queue_id = queue_id(&index_uid, &source_id, &ShardId::from(1));
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_open();
        shard.assert_replication_position(Position::Beginning);
        shard.assert_truncation_position(Position::Beginning);

        assert!(state_guard.mrecordlog.queue_exists(&queue_id));
    }

    #[tokio::test]
    async fn test_ingester_persist() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid_0 = IndexUid::for_test("test-index", 0);
        let index_uid_1 = IndexUid::for_test("test-index", 1);
        let source_id = SourceId::from("test-source");

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
                        index_uid: Some(index_uid_0.clone()),
                        source_id: source_id.clone(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        ingester_id: ingester_ctx.node_id.to_string(),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json: doc_mapping_json.clone(),
                    validate_docs: true,
                },
                InitShardSubrequest {
                    subrequest_id: 1,
                    shard: Some(Shard {
                        index_uid: Some(index_uid_1.clone()),
                        source_id: source_id.clone(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        ingester_id: ingester_ctx.node_id.to_string(),
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
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![
                PersistSubrequest {
                    subrequest_id: 0,
                    index_uid: Some(index_uid_0.clone()),
                    source_id: source_id.clone(),
                    doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid_1.clone()),
                    source_id: source_id.clone(),
                    doc_batch: Some(DocBatchV2::for_test([
                        r#"{"doc": "test-doc-110"}"#,
                        r#"{"doc": "test-doc-111"}"#,
                    ])),
                },
            ],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 2);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success_0 = &persist_response.successes[0];
        assert_eq!(persist_success_0.subrequest_id, 0);
        assert_eq!(persist_success_0.index_uid(), &index_uid_0);
        assert_eq!(persist_success_0.source_id, "test-source");
        assert_eq!(
            persist_success_0.replication_position_inclusive,
            Some(Position::offset(1u64))
        );

        let persist_success_1 = &persist_response.successes[1];
        assert_eq!(persist_success_1.subrequest_id, 1);
        assert_eq!(persist_success_1.index_uid(), &index_uid_1);
        assert_eq!(persist_success_1.source_id, "test-source");
        assert_eq!(
            persist_success_1.replication_position_inclusive,
            Some(Position::offset(2u64))
        );

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 2);

        let queue_id_01 = queue_id(&index_uid_0, &source_id, &ShardId::from(1));
        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        shard_01.assert_is_open();
        shard_01.assert_replication_position(Position::offset(1u64));

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, [0, 0], r#"{"doc": "test-doc-010"}"#), (1, [0, 1], "")],
        );

        let queue_id_11 = queue_id(&index_uid_1, &source_id, &ShardId::from(1));
        let shard_11 = state_guard.shards.get(&queue_id_11).unwrap();
        shard_11.assert_is_open();
        shard_11.assert_replication_position(Position::offset(2u64));

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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

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
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    ingester_id: ingester_ctx.node_id.to_string(),
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
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: Vec::new(),
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_request = PersistRequest {
            ingester_id: "test-ingester".to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: None,
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 1);
        assert_eq!(persist_response.failures.len(), 0);

        let persist_success = &persist_response.successes[0];
        assert_eq!(persist_success.subrequest_id, 0);
        assert_eq!(persist_success.index_uid(), &index_uid);
        assert_eq!(persist_success.source_id, "test-source");
        assert_eq!(
            persist_success.replication_position_inclusive,
            Some(Position::Beginning)
        );
    }

    #[tokio::test]
    async fn test_ingester_persist_validates_docs() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

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
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    ingester_id: ingester_ctx.node_id.to_string(),
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
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test([
                    "",                           // invalid
                    "[]",                         // invalid
                    r#"{"foo": "bar"}"#,          // invalid
                    r#"{"doc": "test-doc-000"}"#, // valid
                ])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

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
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    ingester_id: ingester_ctx.node_id.to_string(),
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
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test([
                    "",                           // invalid
                    "[]",                         // invalid
                    r#"{"foo": "bar"}"#,          // invalid
                    r#"{"doc": "test-doc-000"}"#, // valid
                ])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

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
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    ingester_id: ingester_ctx.node_id.to_string(),
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
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test(["", "[]", r#"{"foo": "bar"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

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
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(0)),
                    shard_state: ShardState::Open as i32,
                    ingester_id: ingester_ctx.node_id.to_string(),
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
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test(["", "[]", r#"{"foo": "bar"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::NoShardsAvailable
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

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let shard = IngesterShard::builder(index_uid.clone(), source_id, ShardId::from(1)).build();
        let queue_id = shard.queue_id();
        state_guard.shards.insert(queue_id.clone(), shard);

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
            ingester_id: "test-ingester".to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test([r#"test-doc-foo"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::NodeUnavailable,
        );

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_closed();

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_ingester_persist_deletes_dangling_shard() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapper = try_build_doc_mapper("{}").unwrap();

        // Insert a dangling shard, i.e. a shard without a corresponding queue.
        let shard = IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(1))
            .with_doc_mapper(doc_mapper)
            .build();
        state_guard.shards.insert(shard.queue_id(), shard);
        drop(state_guard);

        let persist_request = PersistRequest {
            ingester_id: "test-ingester".to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-foo"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::NodeUnavailable
        );

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 0);
    }

    #[tokio::test]
    async fn test_ingester_persist_no_available_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let shard = IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(1))
            .with_state(ShardState::Closed)
            .build();
        let queue_id = shard.queue_id();
        ingester
            .state
            .lock_fully("test")
            .await
            .unwrap()
            .shards
            .insert(queue_id.clone(), shard);

        let persist_request = PersistRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::NoShardsAvailable
        );

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_closed();
        shard.assert_replication_position(Position::Beginning);
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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ingester_id: ingester_ctx.node_id.to_string(),
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

        ingester
            .init_shard(
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

        let persist_request = PersistRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(
            persist_failure.reason(),
            PersistFailureReason::NoShardsAvailable
        );

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));

        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        shard_01.assert_is_open();
        shard_01.assert_replication_position(Position::Beginning);

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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ingester_id: ingester_ctx.node_id.to_string(),
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

        ingester
            .init_shard(
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

        let persist_request = PersistRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![PersistSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
            }],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.ingester_id, "test-ingester");
        assert_eq!(persist_response.successes.len(), 0);
        assert_eq!(persist_response.failures.len(), 1);

        let persist_failure = &persist_response.failures[0];
        assert_eq!(persist_failure.subrequest_id, 0);
        assert_eq!(persist_failure.index_uid(), &index_uid);
        assert_eq!(persist_failure.source_id, "test-source");
        assert_eq!(persist_failure.reason(), PersistFailureReason::WalFull);

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        let shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        shard_01.assert_is_open();
        shard_01.assert_replication_position(Position::Beginning);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[]);
    }

    #[tokio::test]
    async fn test_ingester_persist_returns_routing_update() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid_0 = IndexUid::for_test("test-index-0", 0);
        let index_uid_1 = IndexUid::for_test("test-index-1", 0);
        let source_id = SourceId::from("test-source");

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
                        index_uid: Some(index_uid_0.clone()),
                        source_id: source_id.clone(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        ingester_id: ingester_ctx.node_id.to_string(),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json: doc_mapping_json.clone(),
                    validate_docs: false,
                },
                InitShardSubrequest {
                    subrequest_id: 1,
                    shard: Some(Shard {
                        index_uid: Some(index_uid_1.clone()),
                        source_id: source_id.clone(),
                        shard_id: Some(ShardId::from(1)),
                        shard_state: ShardState::Open as i32,
                        ingester_id: ingester_ctx.node_id.to_string(),
                        doc_mapping_uid: Some(doc_mapping_uid),
                        ..Default::default()
                    }),
                    doc_mapping_json,
                    validate_docs: false,
                },
            ],
        };
        ingester.init_shards(init_shards_request).await.unwrap();

        let persist_request = PersistRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            commit_type: CommitTypeV2::Force as i32,
            subrequests: vec![
                PersistSubrequest {
                    subrequest_id: 0,
                    index_uid: Some(index_uid_0.clone()),
                    source_id: source_id.clone(),
                    doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-010"}"#])),
                },
                PersistSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid_1.clone()),
                    source_id: source_id.clone(),
                    doc_batch: Some(DocBatchV2::for_test([r#"{"doc": "test-doc-110"}"#])),
                },
            ],
        };
        let persist_response = ingester.persist(persist_request).await.unwrap();
        assert_eq!(persist_response.successes.len(), 2);

        let routing_update = persist_response
            .routing_update
            .expect("routing update should be present");

        assert!(
            routing_update.capacity_score > 0,
            "capacity score should be non-zero after a small persist"
        );

        let mut source_shard_updates = routing_update.source_shard_updates;
        source_shard_updates.sort_by(|a, b| a.index_uid().cmp(b.index_uid()));

        assert_eq!(source_shard_updates.len(), 2);
        assert_eq!(source_shard_updates[0].index_uid(), &index_uid_0);
        assert_eq!(source_shard_updates[0].source_id, source_id.as_str());
        assert_eq!(source_shard_updates[0].open_shard_count, 1);
        assert_eq!(source_shard_updates[1].index_uid(), &index_uid_1);
        assert_eq!(source_shard_updates[1].source_id, source_id.as_str());
        assert_eq!(source_shard_updates[1].open_shard_count, 1);

        assert!(routing_update.closed_shards.is_empty());
    }

    #[tokio::test]
    async fn test_ingester_open_fetch_stream() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: "test-client".to_string(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
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
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let queue_id = queue_id(&index_uid, &source_id, &ShardId::from(1));

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

        ingester
            .init_shard(
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
            source_id,
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

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));

        let doc_mapping_uid_01 = DocMappingUid::random();
        let doc_mapping_json_01 = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid_01}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
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
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid_02),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let now = Instant::now();

        ingester
            .init_shard(
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
            .init_shard(
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
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(1)),
                    truncate_up_to_position_inclusive: Some(Position::offset(0u64)),
                },
                TruncateShardsSubrequest {
                    index_uid: Some(index_uid.clone()),
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(2)),
                    truncate_up_to_position_inclusive: Some(Position::eof(0u64)),
                },
                TruncateShardsSubrequest {
                    index_uid: Some(IndexUid::for_test("test-index", 1337)),
                    source_id,
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

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 2);
        assert_eq!(state_guard.doc_mappers.len(), 2);

        assert!(state_guard.shards.contains_key(&queue_id_01));
        assert!(state_guard.shards.contains_key(&queue_id_02));
        assert!(state_guard.doc_mappers.contains_key(&doc_mapping_uid_01));
        assert!(state_guard.doc_mappers.contains_key(&doc_mapping_uid_02));

        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        shard_02.assert_truncation_position(Position::eof(0u64));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(1, [0, 0], "test-doc-bar")]);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_02, .., &[]);
    }

    #[tokio::test]
    async fn test_ingester_truncate_empty_shard_to_eof() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let queue_id = queue_id(&index_uid, &source_id, &ShardId::from(1));

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(r#"{{ "doc_mapping_uid": "{doc_mapping_uid}" }}"#);
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        ingester
            .init_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard,
                &doc_mapping_json,
                Instant::now(),
                true,
            )
            .await
            .unwrap();
        state_guard.shards.get_mut(&queue_id).unwrap().close();
        drop(state_guard);

        let truncate_shards_request = TruncateShardsRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            subrequests: vec![TruncateShardsSubrequest {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(1)),
                truncate_up_to_position_inclusive: Some(Position::Beginning.as_eof()),
            }],
        };
        ingester
            .truncate_shards(truncate_shards_request.clone())
            .await
            .unwrap();

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_truncation_position(Position::Beginning.as_eof());
        assert!(state_guard.mrecordlog.queue_exists(&queue_id));
        state_guard.mrecordlog.assert_records_eq(&queue_id, .., &[]);
    }

    #[tokio::test]
    async fn test_ingester_truncate_shards_deletes_dangling_shards() {
        let (ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let shard =
            IngesterShard::builder(index_uid.clone(), source_id.clone(), ShardId::from(1)).build();
        state_guard.shards.insert(shard.queue_id(), shard);
        drop(state_guard);

        let truncate_shards_request = TruncateShardsRequest {
            ingester_id: ingester_ctx.node_id.to_string(),
            subrequests: vec![TruncateShardsSubrequest {
                index_uid: Some(index_uid.clone()),
                source_id,
                shard_id: Some(ShardId::from(1)),
                truncate_up_to_position_inclusive: Some(Position::offset(0u64)),
            }],
        };
        ingester
            .truncate_shards(truncate_shards_request.clone())
            .await
            .unwrap();

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 0);
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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let now = Instant::now();

        ingester
            .init_shard(
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
            .init_shard(
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

        let state_guard = ingester.state.lock_partially("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        let shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        shard_02.assert_truncation_position(Position::offset(1u64));
    }

    #[tokio::test]
    async fn test_ingester_retain_shards() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_17 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(17)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };

        let shard_18 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
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

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let now = Instant::now();

        ingester
            .init_shard(
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
            .init_shard(
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
            let state_guard = ingester.state.lock_fully("test").await.unwrap();
            assert_eq!(state_guard.shards.len(), 2);
        }

        let retain_shards_request = RetainShardsRequest {
            retain_shards_for_sources: vec![RetainShardsForSource {
                index_uid: Some(index_uid.clone()),
                source_id,
                shard_ids: vec![ShardId::from(17u64)],
            }],
        };
        ingester.retain_shards(retain_shards_request).await.unwrap();

        {
            let state_guard = ingester.state.lock_fully("test").await.unwrap();
            assert_eq!(state_guard.shards.len(), 1);
            assert!(state_guard.shards.contains_key(&queue_id_17));
        }
    }

    #[tokio::test]
    async fn test_ingester_close_shards() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let queue_id = queue_id(&index_uid, &source_id, &ShardId::from(1));

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            publish_position_inclusive: Some(Position::Beginning),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        ingester
            .init_shard(
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
            source_id: source_id.clone(),
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
                    source_id: source_id.clone(),
                    shard_id: Some(ShardId::from(1)),
                },
                ShardPKey {
                    index_uid: Some(index_uid.clone()),
                    source_id,
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

        let state_guard = ingester.state.lock_partially("test").await.unwrap();
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

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        state_guard
            .set_status(IngesterStatus::Decommissioning)
            .await;
        drop(state_guard);

        let observation = observation_stream.next().await.unwrap().unwrap();
        assert_eq!(observation.node_id, ingester_ctx.node_id);
        assert_eq!(observation.status(), IngesterStatus::Decommissioning);

        drop(ingester);

        let observation_opt = observation_stream.next().await;
        assert!(observation_opt.is_none());
    }

    #[tokio::test]
    async fn test_ingester_decommission() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let shard = IngesterShard::builder(index_uid, source_id, ShardId::from(1))
            .with_replication_position_inclusive(Position::offset(12u64))
            .build();
        let queue_id = shard.queue_id();

        state_guard.shards.insert(queue_id.clone(), shard);
        drop(state_guard);

        let mut observation_stream = ingester
            .open_observation_stream(OpenObservationStreamRequest {})
            .await
            .unwrap();

        ingester.decommission(DecommissionRequest {}).await.unwrap();

        let next_observation = observation_stream.next().await.unwrap().unwrap();
        let next_status = next_observation.status();
        assert_eq!(next_status, IngesterStatus::Retiring);

        wait_for_ingester_status(
            &ingester,
            IngesterStatus::Decommissioning,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        let shard = state_guard.shards.get(&queue_id).unwrap();
        shard.assert_is_closed();
    }

    #[tokio::test]
    async fn test_check_decommissioning_status() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

        state_guard.check_decommissioning_status().await;
        assert_eq!(state_guard.status(), IngesterStatus::Ready);

        state_guard
            .set_status(IngesterStatus::Decommissioning)
            .await;
        state_guard.check_decommissioning_status().await;
        assert_eq!(state_guard.status(), IngesterStatus::Decommissioned);

        state_guard
            .set_status(IngesterStatus::Decommissioning)
            .await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let shard = IngesterShard::builder(index_uid.clone(), source_id, ShardId::from(1))
            .with_state(ShardState::Closed)
            .with_replication_position_inclusive(Position::offset(12u64))
            .build();
        let queue_id = shard.queue_id();

        state_guard.shards.insert(queue_id.clone(), shard);
        state_guard.check_decommissioning_status().await;
        assert_eq!(state_guard.status(), IngesterStatus::Decommissioning);

        let shard = state_guard.shards.get_mut(&queue_id).unwrap();
        shard.truncation_position_inclusive = Position::Beginning.as_eof();
        state_guard.check_decommissioning_status().await;
        assert_eq!(state_guard.status(), IngesterStatus::Decommissioning);

        state_guard.shards.remove(&queue_id);
        state_guard.check_decommissioning_status().await;
        assert_eq!(state_guard.status(), IngesterStatus::Decommissioned);
    }

    #[tokio::test]
    async fn test_check_decommissioning_status_with_empty_orphan_shard() {
        // A non-advertisable shard is invisible to gossip/RPC-driven cleanup and will never be
        // deleted, so the ingester must not wait for its removal to consider itself
        // decommissioned as long as it is empty.
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let empty_orphan_shard =
            IngesterShard::builder(index_uid.clone(), source_id, ShardId::from(1))
                .with_state(ShardState::Closed)
                .build();
        let queue_id = empty_orphan_shard.queue_id();
        state_guard
            .shards
            .insert(queue_id.clone(), empty_orphan_shard);

        state_guard
            .set_status(IngesterStatus::Decommissioning)
            .await;
        state_guard.check_decommissioning_status().await;

        assert_eq!(state_guard.status(), IngesterStatus::Decommissioned);
        assert!(state_guard.shards.contains_key(&queue_id));
    }

    #[tokio::test]
    async fn test_decommission_completes_when_empty_shard_deleted_via_gossip() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let event_broker = EventBroker::default();
        ingester.subscribe(&event_broker);

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let empty_shard =
            IngesterShard::builder(index_uid.clone(), source_id.clone(), shard_id.clone())
                .with_state(ShardState::Closed)
                .build();

        let mut status_rx = ingester.state.status_rx.clone();
        {
            let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
            state_guard.shards.insert(queue_id.clone(), empty_shard);
            state_guard
                .set_status(IngesterStatus::Decommissioning)
                .await;
        }

        assert_eq!(
            *status_rx.borrow_and_update(),
            IngesterStatus::Decommissioning
        );

        event_broker.publish(ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid: index_uid.clone(),
                source_id: source_id.clone(),
            },
            updated_shard_positions: vec![(shard_id, Position::Beginning.as_eof())],
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            while *status_rx.borrow_and_update() != IngesterStatus::Decommissioned {
                status_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("ingester should reach Decommissioned once its empty shard is deleted via gossip");

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert!(!state_guard.shards.contains_key(&queue_id));
    }

    #[tokio::test]
    async fn test_ingester_truncate_on_shard_positions_update() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;
        let event_broker = EventBroker::default();
        ingester.subscribe(&event_broker);

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));

        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let queue_id_02 = queue_id(&index_uid, &source_id, &ShardId::from(2));

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let now = Instant::now();

        ingester
            .init_shard(
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
            .init_shard(
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
                source_id,
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

        // Wait for both events to be processed.
        wait_until_predicate(
            || async {
                ingester
                    .state
                    .lock_fully("test")
                    .await
                    .unwrap()
                    .shards
                    .len()
                    == 1
            },
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await
        .expect("shard `2` should be deleted");

        let state_guard = ingester.state.lock_fully("test").await.unwrap();
        assert_eq!(state_guard.shards.len(), 1);

        assert!(state_guard.shards.contains_key(&queue_id_01));

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(1, [0, 0], "test-doc-bar")]);

        assert!(!state_guard.shards.contains_key(&queue_id_02));
        assert!(!state_guard.mrecordlog.queue_exists(&queue_id_02));
    }

    #[tokio::test]
    async fn test_filter_local_shard_updates() {
        let (_ingester_ctx, ingester) = IngesterForTest::default().build().await;

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));

        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let now = Instant::now();

        ingester
            .init_shard(
                &mut state_guard.inner,
                &mut state_guard.mrecordlog,
                shard_01,
                &doc_mapping_json,
                now,
                true,
            )
            .await
            .unwrap();

        // Truncate the shard once so that its truncation position is already at offset 0. This
        // lets us exercise the "stale update" case below.
        state_guard
            .truncate_shard(&queue_id_01, Position::offset(0u64), "test")
            .await;

        drop(state_guard);

        let shard_position_update = ShardPositionsUpdate {
            source_uid: SourceUid {
                index_uid,
                source_id,
            },
            updated_shard_positions: vec![
                // Shard hosted by another ingester: filtered out.
                (ShardId::from(2), Position::offset(5u64)),
                // Local shard, but the position does not advance the truncation position:
                // filtered out.
                (ShardId::from(1), Position::offset(0u64)),
                // Local shard, `Beginning` position: filtered out.
                (ShardId::from(1), Position::Beginning),
                // Local shard, advances the truncation position: kept.
                (ShardId::from(1), Position::offset(1u64)),
                // Local shard, EOF position: always kept, regardless of the current truncation
                // position.
                (ShardId::from(1), Position::eof(0u64)),
            ],
        };
        let local_updates =
            filter_local_shard_updates(&ingester.state, shard_position_update).await;
        assert_eq!(
            local_updates,
            vec![
                (queue_id_01.clone(), Position::offset(1u64)),
                (queue_id_01, Position::eof(0u64)),
            ]
        );
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

        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id = SourceId::from("test-source");
        let queue_id_01 = queue_id(&index_uid, &source_id, &ShardId::from(1));

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id,
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let now = Instant::now();

        ingester
            .init_shard(
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

            let state_guard = ingester.state.lock_partially("test").await.unwrap();
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
        let source_id = SourceId::from("test-source");

        let doc_mapping_uid = DocMappingUid::random();
        let doc_mapping_json = format!(
            r#"{{
                "doc_mapping_uid": "{doc_mapping_uid}"
            }}"#
        );
        let shard_01 = Shard {
            index_uid: Some(index_uid_0.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: Some(index_uid_0.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let shard_03 = Shard {
            index_uid: Some(index_uid_1.clone()),
            source_id,
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Closed as i32,
            doc_mapping_uid: Some(doc_mapping_uid),
            ..Default::default()
        };
        let mut state_guard = ingester.state.lock_fully("test").await.unwrap();
        let now = Instant::now();

        ingester
            .init_shard(
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
            .init_shard(
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
            .init_shard(
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
