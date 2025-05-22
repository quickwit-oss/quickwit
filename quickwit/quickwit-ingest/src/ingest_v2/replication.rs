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

use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytesize::ByteSize;
use futures::{Future, StreamExt};
use mrecordlog::error::CreateQueueError;
use quickwit_common::metrics::{GaugeGuard, MEMORY_METRICS};
use quickwit_common::{ServiceStream, rate_limited_warn};
use quickwit_proto::ingest::ingester::{
    AckReplicationMessage, IngesterStatus, InitReplicaRequest, InitReplicaResponse,
    ReplicateFailure, ReplicateFailureReason, ReplicateRequest, ReplicateResponse,
    ReplicateSubrequest, ReplicateSuccess, SynReplicationMessage, ack_replication_message,
    syn_replication_message,
};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error, IngestV2Result, Shard, ShardState};
use quickwit_proto::types::{NodeId, Position, QueueId};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, warn};

use super::metrics::report_wal_usage;
use super::models::IngesterShard;
use super::mrecordlog_utils::check_enough_capacity;
use super::state::IngesterState;
use crate::ingest_v2::mrecordlog_utils::{AppendDocBatchError, append_non_empty_doc_batch};
use crate::metrics::INGEST_METRICS;
use crate::{estimate_size, with_lock_metrics};

pub(super) const SYN_REPLICATION_STREAM_CAPACITY: usize = 5;

/// Duration after which replication requests time out with [`ReplicationError::Timeout`].
const REPLICATION_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(250)
} else {
    Duration::from_secs(3)
};

/// A replication request is sent by the leader to its follower to update the state of a replica
/// shard.
#[derive(Debug)]
pub(super) enum ReplicationRequest {
    Init(InitReplicaRequest),
    Replicate(ReplicateRequest),
}

impl ReplicationRequest {
    fn replication_seqno(&self) -> ReplicationSeqNo {
        match self {
            ReplicationRequest::Init(init_replica_request) => {
                init_replica_request.replication_seqno
            }
            ReplicationRequest::Replicate(replicate_request) => replicate_request.replication_seqno,
        }
    }

    fn set_replication_seqno(&mut self, replication_seqno: ReplicationSeqNo) {
        match self {
            ReplicationRequest::Init(init_replica_request) => {
                init_replica_request.replication_seqno = replication_seqno
            }
            ReplicationRequest::Replicate(replicate_request) => {
                replicate_request.replication_seqno = replication_seqno
            }
        }
    }

    fn into_syn_replication_message(self) -> SynReplicationMessage {
        match self {
            ReplicationRequest::Init(init_replica_request) => {
                SynReplicationMessage::new_init_replica_request(init_replica_request)
            }
            ReplicationRequest::Replicate(replicate_request) => {
                SynReplicationMessage::new_replicate_request(replicate_request)
            }
        }
    }
}

#[derive(Debug)]
pub(super) enum ReplicationResponse {
    Init(InitReplicaResponse),
    Replicate(ReplicateResponse),
}

impl ReplicationResponse {
    fn replication_seqno(&self) -> ReplicationSeqNo {
        match self {
            ReplicationResponse::Init(init_replica_response) => {
                init_replica_response.replication_seqno
            }
            ReplicationResponse::Replicate(replicate_response) => {
                replicate_response.replication_seqno
            }
        }
    }
}

type OneShotReplicationRequest = (ReplicationRequest, oneshot::Sender<ReplicationResponse>);

/// Replication sequence number.
type ReplicationSeqNo = u64;

/// Task that "powers" the replication stream between a leader and a follower.
pub(super) struct ReplicationStreamTask {
    leader_id: NodeId,
    follower_id: NodeId,
    replication_request_rx: mpsc::Receiver<OneShotReplicationRequest>,
    syn_replication_stream_tx: mpsc::Sender<SynReplicationMessage>,
    ack_replication_stream: ServiceStream<IngestV2Result<AckReplicationMessage>>,
}

impl ReplicationStreamTask {
    /// Spawns a [`ReplicationStreamTask`].
    pub fn spawn(
        leader_id: NodeId,
        follower_id: NodeId,
        syn_replication_stream_tx: mpsc::Sender<SynReplicationMessage>,
        ack_replication_stream: ServiceStream<IngestV2Result<AckReplicationMessage>>,
    ) -> ReplicationStreamTaskHandle {
        let (replication_request_tx, replication_request_rx) =
            mpsc::channel::<OneShotReplicationRequest>(3);

        let replication_stream_task = Self {
            leader_id,
            follower_id,
            replication_request_rx,
            syn_replication_stream_tx,
            ack_replication_stream,
        };
        let (enqueue_syn_requests_join_handle, dequeue_ack_responses_join_handle) =
            replication_stream_task.run();

        ReplicationStreamTaskHandle {
            replication_request_tx,
            enqueue_syn_requests_join_handle,
            dequeue_ack_responses_join_handle,
        }
    }

    /// Executes the request processing loop. It enqueues requests into the SYN replication stream
    /// going to the follower then dequeues the responses returned from the ACK replication
    /// stream. Additionally (and crucially), it ensures that requests and responses are
    /// processed and returned in the same order. Conceptually, it is akin to "zipping" the SYN and
    /// ACK replication streams together.
    fn run(mut self) -> (JoinHandle<()>, JoinHandle<()>) {
        // Response sequencer channel. It ensures that requests and responses are processed and
        // returned in the same order.
        //
        // Channel capacity: there is no need to bound the capacity of the channel here
        // because it is already virtually bounded by the capacity of the SYN replication
        // stream.
        let (response_sequencer_tx, mut response_sequencer_rx) = mpsc::unbounded_channel();

        // This loop enqueues SYN replication requests into the SYN replication stream and passes
        // the one-shot response sender to the "dequeue" loop via the sequencer channel.
        let enqueue_syn_requests_fut = async move {
            let mut replication_seqno = ReplicationSeqNo::default();
            while let Some((mut replication_request, oneshot_replication_response_tx)) =
                self.replication_request_rx.recv().await
            {
                replication_request.set_replication_seqno(replication_seqno);
                replication_seqno += 1;

                if response_sequencer_tx
                    .send((
                        replication_request.replication_seqno(),
                        oneshot_replication_response_tx,
                    ))
                    .is_err()
                {
                    // The response sequencer receiver was dropped.
                    return;
                }
                let syn_replication_message = replication_request.into_syn_replication_message();

                if self
                    .syn_replication_stream_tx
                    .send(syn_replication_message)
                    .await
                    .is_err()
                {
                    // The SYN replication stream was closed.
                    return;
                }
            }
            // The replication client was dropped.
        };
        // This loop dequeues ACK replication responses from the ACK replication stream and forwards
        // them to their respective clients using associated one-shot associated with each response.
        let dequeue_ack_responses_fut = async move {
            while let Some(ack_replication_message_res) = self.ack_replication_stream.next().await {
                let ack_replication_message = match ack_replication_message_res {
                    Ok(ack_replication_message) => ack_replication_message,
                    Err(_) => {
                        return;
                    }
                };
                let replication_response = match ack_replication_message.message {
                    Some(ack_replication_message::Message::InitResponse(init_replica_response)) => {
                        ReplicationResponse::Init(init_replica_response)
                    }
                    Some(ack_replication_message::Message::ReplicateResponse(
                        replicate_response,
                    )) => ReplicationResponse::Replicate(replicate_response),
                    Some(ack_replication_message::Message::OpenResponse(_)) => {
                        warn!("received unexpected ACK replication message");
                        continue;
                    }
                    None => {
                        warn!("received empty ACK replication message");
                        continue;
                    }
                };
                let oneshot_replication_response_tx = match response_sequencer_rx.try_recv() {
                    Ok((replication_seqno, oneshot_replication_response_tx)) => {
                        if replication_response.replication_seqno() != replication_seqno {
                            error!(
                                "received out-of-order replication response: expected replication \
                                 seqno `{}`, got `{}`; closing replication stream from leader \
                                 `{}` to follower `{}`",
                                replication_seqno,
                                replication_response.replication_seqno(),
                                self.leader_id,
                                self.follower_id,
                            );
                            return;
                        }
                        oneshot_replication_response_tx
                    }
                    Err(TryRecvError::Empty) => {
                        panic!("response sequencer should not be empty");
                    }
                    Err(TryRecvError::Disconnected) => {
                        // The response sequencer sender was dropped.
                        return;
                    }
                };
                // We intentionally ignore the error here. It is the responsibility of the
                // `replicate` method to surface it.
                let _ = oneshot_replication_response_tx.send(replication_response);
            }
            // The ACK replication stream was closed.
        };
        (
            tokio::spawn(enqueue_syn_requests_fut),
            tokio::spawn(dequeue_ack_responses_fut),
        )
    }
}

pub(super) struct ReplicationStreamTaskHandle {
    replication_request_tx: mpsc::Sender<OneShotReplicationRequest>,
    enqueue_syn_requests_join_handle: JoinHandle<()>,
    dequeue_ack_responses_join_handle: JoinHandle<()>,
}

impl ReplicationStreamTaskHandle {
    /// Returns a [`ReplicationClient`] that can be used to enqueue replication requests
    /// into the replication stream.
    pub fn replication_client(&self) -> ReplicationClient {
        ReplicationClient {
            replication_request_tx: self.replication_request_tx.clone(),
        }
    }
}

impl Drop for ReplicationStreamTaskHandle {
    fn drop(&mut self) {
        self.enqueue_syn_requests_join_handle.abort();
        self.dequeue_ack_responses_join_handle.abort();
    }
}

/// Error returned by the [`ReplicationClient`].
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("failed to replicate records from leader to follower")]
pub(super) enum ReplicationError {
    /// The replication stream was closed.
    #[error("replication stream was closed")]
    Closed,
    /// The replication request timed out.
    #[error("replication request timed out")]
    Timeout,
}

// DO NOT derive or implement `Clone` for this object.
#[derive(Debug)]
pub(super) struct ReplicationClient {
    replication_request_tx: mpsc::Sender<OneShotReplicationRequest>,
}

/// Single-use client that enqueues replication requests into the replication stream.
///
/// The `init_replica`, `replicate`, and `submit` methods take `self` instead of `&self`
/// to produce 'static futures and enforce single-use semantics.
impl ReplicationClient {
    /// Enqueues an init replica request into the replication stream and waits for the response.
    /// Times out after [`REPLICATION_REQUEST_TIMEOUT`] seconds.
    pub fn init_replica(
        self,
        replica_shard: Shard,
    ) -> impl Future<Output = Result<InitReplicaResponse, ReplicationError>> + Send + 'static {
        let init_replica_request = InitReplicaRequest {
            replica_shard: Some(replica_shard),
            replication_seqno: 0, // replication number are generated further down
        };
        let replication_request = ReplicationRequest::Init(init_replica_request);

        async {
            self.submit(replication_request)
                .await
                .map(|replication_response| {
                    if let ReplicationResponse::Init(init_replica_response) = replication_response {
                        init_replica_response
                    } else {
                        panic!("response should be an init replica response")
                    }
                })
        }
    }

    /// Enqueues a replicate request into the replication stream and waits for the response. Times
    /// out after [`REPLICATION_REQUEST_TIMEOUT`] seconds.
    pub fn replicate(
        self,
        leader_id: NodeId,
        follower_id: NodeId,
        subrequests: Vec<ReplicateSubrequest>,
        commit_type: CommitTypeV2,
    ) -> impl Future<Output = Result<ReplicateResponse, ReplicationError>> + Send + 'static {
        let replicate_request = ReplicateRequest {
            leader_id: leader_id.into(),
            follower_id: follower_id.into(),
            subrequests,
            commit_type: commit_type as i32,
            replication_seqno: 0, // replication number are generated further down
        };
        let replication_request = ReplicationRequest::Replicate(replicate_request);

        async {
            self.submit(replication_request)
                .await
                .map(|replication_response| {
                    if let ReplicationResponse::Replicate(replicate_response) = replication_response
                    {
                        replicate_response
                    } else {
                        panic!("response should be a replicate response")
                    }
                })
        }
    }

    /// Submits a replication request to the replication stream and waits for the response.
    fn submit(
        self,
        replication_request: ReplicationRequest,
    ) -> impl Future<Output = Result<ReplicationResponse, ReplicationError>> + Send + 'static {
        let (oneshot_replication_response_tx, oneshot_replication_response_rx) = oneshot::channel();

        let send_recv_fut = async move {
            self.replication_request_tx
                .send((replication_request, oneshot_replication_response_tx))
                .await
                .map_err(|_| ReplicationError::Closed)?;
            let replicate_response = oneshot_replication_response_rx
                .await
                .map_err(|_| ReplicationError::Closed)?;
            Ok(replicate_response)
        };
        async {
            tokio::time::timeout(REPLICATION_REQUEST_TIMEOUT, send_recv_fut)
                .await
                .map_err(|_| ReplicationError::Timeout)?
        }
    }
}

/// Replication task executed for each replication stream.
pub(super) struct ReplicationTask {
    leader_id: NodeId,
    follower_id: NodeId,
    state: IngesterState,
    syn_replication_stream: ServiceStream<SynReplicationMessage>,
    ack_replication_stream_tx: mpsc::UnboundedSender<IngestV2Result<AckReplicationMessage>>,
    current_replication_seqno: ReplicationSeqNo,
    disk_capacity: ByteSize,
    memory_capacity: ByteSize,
}

impl ReplicationTask {
    pub fn spawn(
        leader_id: NodeId,
        follower_id: NodeId,
        state: IngesterState,
        syn_replication_stream: ServiceStream<SynReplicationMessage>,
        ack_replication_stream_tx: mpsc::UnboundedSender<IngestV2Result<AckReplicationMessage>>,
        disk_capacity: ByteSize,
        memory_capacity: ByteSize,
    ) -> ReplicationTaskHandle {
        let mut replication_task = Self {
            leader_id,
            follower_id,
            state,
            syn_replication_stream,
            ack_replication_stream_tx,
            current_replication_seqno: 0,
            disk_capacity,
            memory_capacity,
        };
        let join_handle = tokio::spawn(async move { replication_task.run().await });
        ReplicationTaskHandle { join_handle }
    }

    async fn init_replica(
        &mut self,
        init_replica_request: InitReplicaRequest,
    ) -> IngestV2Result<InitReplicaResponse> {
        if init_replica_request.replication_seqno != self.current_replication_seqno {
            return Err(IngestV2Error::Internal(format!(
                "received out-of-order replication request: expected replication seqno `{}`, got \
                 `{}`",
                self.current_replication_seqno, init_replica_request.replication_seqno
            )));
        }
        self.current_replication_seqno += 1;

        let Some(replica_shard) = init_replica_request.replica_shard else {
            warn!("received empty init replica request");

            return Err(IngestV2Error::Internal(
                "init replica request is empty".to_string(),
            ));
        };
        let queue_id = replica_shard.queue_id();

        let mut state_guard =
            with_lock_metrics!(self.state.lock_fully(), "init_replica", "write").await?;

        match state_guard.mrecordlog.create_queue(&queue_id).await {
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
        let replica_shard = IngesterShard::new_replica(
            replica_shard.leader_id.into(),
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        state_guard.shards.insert(queue_id, replica_shard);

        let init_replica_response = InitReplicaResponse {
            replication_seqno: init_replica_request.replication_seqno,
        };
        Ok(init_replica_response)
    }

    async fn replicate(
        &mut self,
        replicate_request: ReplicateRequest,
    ) -> IngestV2Result<ReplicateResponse> {
        if replicate_request.leader_id != self.leader_id {
            return Err(IngestV2Error::Internal(format!(
                "routing error: expected leader ID `{}`, got `{}`",
                self.leader_id, replicate_request.leader_id
            )));
        }
        if replicate_request.follower_id != self.follower_id {
            return Err(IngestV2Error::Internal(format!(
                "routing error: expected follower ID `{}`, got `{}`",
                self.follower_id, replicate_request.follower_id
            )));
        }
        if replicate_request.replication_seqno != self.current_replication_seqno {
            return Err(IngestV2Error::Internal(format!(
                "received out-of-order replication request: expected replication seqno `{}`, got \
                 `{}`",
                self.current_replication_seqno, replicate_request.replication_seqno
            )));
        }
        let request_size_bytes = replicate_request.num_bytes();
        let mut gauge_guard = GaugeGuard::from_gauge(&MEMORY_METRICS.in_flight.ingester_replicate);
        gauge_guard.add(request_size_bytes as i64);

        self.current_replication_seqno += 1;

        let commit_type = replicate_request.commit_type();
        let force_commit = commit_type == CommitTypeV2::Force;

        let mut replicate_successes = Vec::with_capacity(replicate_request.subrequests.len());
        let mut replicate_failures = Vec::new();

        // Keep track of the shards that need to be closed following an IO error.
        let mut shards_to_close: HashSet<QueueId> = HashSet::new();

        // Keep track of dangling shards, i.e., shards for which there is no longer a corresponding
        // queue in the WAL and should be deleted.
        let mut shards_to_delete: HashSet<QueueId> = HashSet::new();

        let mut state_guard =
            with_lock_metrics!(self.state.lock_fully(), "replicate", "write").await?;

        if state_guard.status() != IngesterStatus::Ready {
            replicate_failures.reserve_exact(replicate_request.subrequests.len());

            for subrequest in replicate_request.subrequests {
                let replicate_failure = ReplicateFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: ReplicateFailureReason::ShardClosed as i32,
                };
                replicate_failures.push(replicate_failure);
            }
            let replicate_response = ReplicateResponse {
                follower_id: replicate_request.follower_id,
                successes: Vec::new(),
                failures: replicate_failures,
                replication_seqno: replicate_request.replication_seqno,
            };
            return Ok(replicate_response);
        }
        let now = Instant::now();

        for subrequest in replicate_request.subrequests {
            let queue_id = subrequest.queue_id();
            let from_position_exclusive = subrequest.from_position_exclusive();

            let Some(shard) = state_guard.shards.get(&queue_id) else {
                let replicate_failure = ReplicateFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: ReplicateFailureReason::ShardNotFound as i32,
                };
                replicate_failures.push(replicate_failure);
                continue;
            };
            assert!(shard.is_replica());

            if shard.is_closed() {
                let replicate_failure = ReplicateFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: ReplicateFailureReason::ShardClosed as i32,
                };
                replicate_failures.push(replicate_failure);
                continue;
            }
            if shard.replication_position_inclusive != from_position_exclusive {
                // TODO
            }
            let doc_batch = match subrequest.doc_batch {
                Some(doc_batch) if !doc_batch.is_empty() => doc_batch,
                _ => {
                    warn!("received empty replicate request");

                    let replicate_success = ReplicateSuccess {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        shard_id: subrequest.shard_id,
                        replication_position_inclusive: Some(
                            shard.replication_position_inclusive.clone(),
                        ),
                    };
                    replicate_successes.push(replicate_success);
                    continue;
                }
            };

            let batch_num_bytes = doc_batch.num_bytes() as u64;
            let batch_num_docs = doc_batch.num_docs() as u64;

            let requested_capacity = estimate_size(&doc_batch);

            if let Err(error) = check_enough_capacity(
                &state_guard.mrecordlog,
                self.disk_capacity,
                self.memory_capacity,
                requested_capacity,
            ) {
                rate_limited_warn!(
                    limit_per_min = 10,
                    "failed to replicate records to ingester `{}`: {error}",
                    self.follower_id,
                );
                let replicate_failure = ReplicateFailure {
                    subrequest_id: subrequest.subrequest_id,
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    reason: ReplicateFailureReason::WalFull as i32,
                };
                replicate_failures.push(replicate_failure);
                continue;
            };
            let append_result = append_non_empty_doc_batch(
                &mut state_guard.mrecordlog,
                &queue_id,
                doc_batch,
                force_commit,
            )
            .await;

            let current_position_inclusive = match append_result {
                Ok(current_position_inclusive) => current_position_inclusive,
                Err(append_error) => {
                    let reason = match &append_error {
                        AppendDocBatchError::Io(io_error) => {
                            error!("failed to replicate records to shard `{queue_id}`: {io_error}");
                            shards_to_close.insert(queue_id);
                            ReplicateFailureReason::ShardClosed
                        }
                        AppendDocBatchError::QueueNotFound(_) => {
                            error!(
                                "failed to replicate records to shard `{queue_id}`: WAL queue not \
                                 found"
                            );
                            shards_to_delete.insert(queue_id);
                            ReplicateFailureReason::ShardNotFound
                        }
                    };
                    let replicate_failure = ReplicateFailure {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        shard_id: subrequest.shard_id,
                        reason: reason as i32,
                    };
                    replicate_failures.push(replicate_failure);
                    continue;
                }
            };
            state_guard
                .shards
                .get_mut(&queue_id)
                .expect("replica shard should be initialized")
                .set_replication_position_inclusive(current_position_inclusive.clone(), now);

            INGEST_METRICS
                .replicated_num_bytes_total
                .inc_by(batch_num_bytes);
            INGEST_METRICS
                .replicated_num_docs_total
                .inc_by(batch_num_docs);

            let replicate_success = ReplicateSuccess {
                subrequest_id: subrequest.subrequest_id,
                index_uid: subrequest.index_uid,
                source_id: subrequest.source_id,
                shard_id: subrequest.shard_id,
                replication_position_inclusive: Some(current_position_inclusive),
            };
            replicate_successes.push(replicate_success);
        }
        if !shards_to_close.is_empty() {
            for queue_id in &shards_to_close {
                let shard = state_guard
                    .shards
                    .get_mut(queue_id)
                    .expect("shard should exist");

                shard.shard_state = ShardState::Closed;
                shard.notify_shard_status();
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

        report_wal_usage(wal_usage);

        let follower_id = self.follower_id.clone().into();

        let replicate_response = ReplicateResponse {
            follower_id,
            successes: replicate_successes,
            failures: replicate_failures,
            replication_seqno: replicate_request.replication_seqno,
        };
        Ok(replicate_response)
    }

    async fn run(&mut self) -> IngestV2Result<()> {
        while let Some(syn_replication_message) = self.syn_replication_stream.next().await {
            let ack_replication_message = match syn_replication_message.message {
                Some(syn_replication_message::Message::OpenRequest(_)) => {
                    panic!("TODO: this should not happen, internal error");
                }
                Some(syn_replication_message::Message::InitRequest(init_replica_request)) => self
                    .init_replica(init_replica_request)
                    .await
                    .map(AckReplicationMessage::new_init_replica_response),
                Some(syn_replication_message::Message::ReplicateRequest(replicate_request)) => self
                    .replicate(replicate_request)
                    .await
                    .map(AckReplicationMessage::new_replicate_response),
                None => {
                    warn!("received empty SYN replication message");
                    continue;
                }
            };
            if self
                .ack_replication_stream_tx
                .send(ack_replication_message)
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }
}

pub(super) struct ReplicationTaskHandle {
    join_handle: JoinHandle<IngestV2Result<()>>,
}

impl Drop for ReplicationTaskHandle {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

#[cfg(test)]
mod tests {

    use quickwit_proto::ingest::ingester::{ReplicateSubrequest, ReplicateSuccess};
    use quickwit_proto::ingest::{DocBatchV2, Shard};
    use quickwit_proto::types::{IndexUid, ShardId, queue_id};

    use super::*;

    fn into_init_replica_request(
        syn_replication_message: SynReplicationMessage,
    ) -> InitReplicaRequest {
        let Some(syn_replication_message::Message::InitRequest(init_replica_request)) =
            syn_replication_message.message
        else {
            panic!(
                "expected init replica SYN message, got `{:?}`",
                syn_replication_message.message
            );
        };
        init_replica_request
    }

    fn into_replicate_request(syn_replication_message: SynReplicationMessage) -> ReplicateRequest {
        let Some(syn_replication_message::Message::ReplicateRequest(replicate_request)) =
            syn_replication_message.message
        else {
            panic!(
                "expected replicate SYN message, got `{:?}`",
                syn_replication_message.message
            );
        };
        replicate_request
    }

    fn into_init_replica_response(
        ack_replication_message: AckReplicationMessage,
    ) -> InitReplicaResponse {
        let Some(ack_replication_message::Message::InitResponse(init_replica_response)) =
            ack_replication_message.message
        else {
            panic!(
                "expected init replica ACK message, got `{:?}`",
                ack_replication_message.message
            );
        };
        init_replica_response
    }

    fn into_replicate_response(
        ack_replication_message: AckReplicationMessage,
    ) -> ReplicateResponse {
        let Some(ack_replication_message::Message::ReplicateResponse(replicate_response)) =
            ack_replication_message.message
        else {
            panic!(
                "expected replicate ACK message, got `{:?}`",
                ack_replication_message.message
            );
        };
        replicate_response
    }

    #[tokio::test]
    async fn test_replication_stream_task_init() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (syn_replication_stream_tx, mut syn_replication_stream_rx) = mpsc::channel(5);
        let (ack_replication_stream_tx, ack_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let replication_stream_task_handle = ReplicationStreamTask::spawn(
            leader_id,
            follower_id,
            syn_replication_stream_tx,
            ack_replication_stream,
        );
        let dummy_replication_task_future = async move {
            while let Some(syn_replication_message) = syn_replication_stream_rx.recv().await {
                let init_replica_request = into_init_replica_request(syn_replication_message);
                let init_replica_response = InitReplicaResponse {
                    replication_seqno: init_replica_request.replication_seqno,
                };
                let ack_replication_message =
                    AckReplicationMessage::new_init_replica_response(init_replica_response);
                ack_replication_stream_tx
                    .send(Ok(ack_replication_message))
                    .await
                    .unwrap();
            }
        };
        tokio::spawn(dummy_replication_task_future);

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let replica_shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            leader_id: "test-leader".to_string(),
            follower_id: Some("test-follower".to_string()),
            ..Default::default()
        };
        let init_replica_response = replication_stream_task_handle
            .replication_client()
            .init_replica(replica_shard)
            .await
            .unwrap();
        assert_eq!(init_replica_response.replication_seqno, 0);
    }

    #[tokio::test]
    async fn test_replication_stream_task_replicate() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (syn_replication_stream_tx, mut syn_replication_stream_rx) = mpsc::channel(5);
        let (ack_replication_stream_tx, ack_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let replication_stream_task_handle = ReplicationStreamTask::spawn(
            leader_id.clone(),
            follower_id.clone(),
            syn_replication_stream_tx,
            ack_replication_stream,
        );
        let dummy_replication_task_future = async move {
            while let Some(syn_replication_message) = syn_replication_stream_rx.recv().await {
                let replicate_request = into_replicate_request(syn_replication_message);
                let replicate_successes = replicate_request
                    .subrequests
                    .iter()
                    .map(|subrequest| {
                        let batch_len = subrequest.doc_batch.as_ref().unwrap().num_docs();
                        let replication_position_inclusive = subrequest
                            .from_position_exclusive()
                            .as_usize()
                            .map(|pos| pos + batch_len)
                            .unwrap_or(batch_len - 1);
                        ReplicateSuccess {
                            subrequest_id: subrequest.subrequest_id,
                            index_uid: subrequest.index_uid.clone(),
                            source_id: subrequest.source_id.clone(),
                            shard_id: subrequest.shard_id.clone(),
                            replication_position_inclusive: Some(Position::offset(
                                replication_position_inclusive,
                            )),
                        }
                    })
                    .collect::<Vec<_>>();

                let replicate_response = ReplicateResponse {
                    follower_id: replicate_request.follower_id,
                    successes: replicate_successes,
                    failures: Vec::new(),
                    replication_seqno: replicate_request.replication_seqno,
                };
                let ack_replication_message =
                    AckReplicationMessage::new_replicate_response(replicate_response);
                ack_replication_stream_tx
                    .send(Ok(ack_replication_message))
                    .await
                    .unwrap();
            }
        };
        tokio::spawn(dummy_replication_task_future);

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index", 1);

        let subrequests = vec![
            ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: Some(Position::Beginning),
            },
            ReplicateSubrequest {
                subrequest_id: 1,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(2)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-bar", "test-doc-baz"])),
                from_position_exclusive: Some(Position::Beginning),
            },
            ReplicateSubrequest {
                subrequest_id: 2,
                index_uid: Some(index_uid2.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-qux", "test-doc-tux"])),
                from_position_exclusive: Some(Position::offset(0u64)),
            },
        ];
        let replicate_response = replication_stream_task_handle
            .replication_client()
            .replicate(
                leader_id.clone(),
                follower_id.clone(),
                subrequests,
                CommitTypeV2::Auto,
            )
            .await
            .unwrap();
        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 3);
        assert_eq!(replicate_response.failures.len(), 0);
        assert_eq!(replicate_response.replication_seqno, 0);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid(), &index_uid);
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_success_0.replication_position_inclusive(),
            Position::offset(0u64)
        );

        let replicate_success_1 = &replicate_response.successes[1];
        assert_eq!(replicate_success_1.index_uid(), &index_uid);
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id(), ShardId::from(2));
        assert_eq!(
            replicate_success_1.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let replicate_success_2 = &replicate_response.successes[2];
        assert_eq!(replicate_success_2.index_uid(), &index_uid2);
        assert_eq!(replicate_success_2.source_id, "test-source");
        assert_eq!(replicate_success_2.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_success_2.replication_position_inclusive(),
            Position::offset(2u64)
        );
    }

    #[tokio::test]
    async fn test_replication_stream_replicate_errors() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (syn_replication_stream_tx, _syn_replication_stream_rx) = mpsc::channel(5);
        let (_ack_replication_stream_tx, ack_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let replication_stream_task_handle = ReplicationStreamTask::spawn(
            leader_id.clone(),
            follower_id.clone(),
            syn_replication_stream_tx,
            ack_replication_stream,
        );
        let timeout_error = replication_stream_task_handle
            .replication_client()
            .replicate(
                leader_id.clone(),
                follower_id.clone(),
                Vec::new(),
                CommitTypeV2::Auto,
            )
            .await
            .unwrap_err();
        assert!(matches!(timeout_error, ReplicationError::Timeout));

        replication_stream_task_handle
            .enqueue_syn_requests_join_handle
            .abort();

        let closed_error = replication_stream_task_handle
            .replication_client()
            .replicate(leader_id, follower_id, Vec::new(), CommitTypeV2::Auto)
            .await
            .unwrap_err();

        assert!(matches!(closed_error, ReplicationError::Closed));
    }

    #[tokio::test]
    async fn test_replication_task_happy_path() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let (syn_replication_stream_tx, syn_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let (ack_replication_stream_tx, mut ack_replication_stream) =
            ServiceStream::new_unbounded();

        let disk_capacity = ByteSize::mb(256);
        let memory_capacity = ByteSize::mb(1);

        let _replication_task_handle = ReplicationTask::spawn(
            leader_id,
            follower_id,
            state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
            disk_capacity,
            memory_capacity,
        );

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid2: IndexUid = IndexUid::for_test("test-index", 1);

        // Init shard 01.
        let init_replica_request = InitReplicaRequest {
            replica_shard: Some(Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-leader".to_string(),
                follower_id: Some("test-follower".to_string()),
                ..Default::default()
            }),
            replication_seqno: 0,
        };
        let syn_replication_message =
            SynReplicationMessage::new_init_replica_request(init_replica_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let init_replica_response = into_init_replica_response(ack_replication_message);
        assert_eq!(init_replica_response.replication_seqno, 0);

        // Init shard 02.
        let init_replica_request = InitReplicaRequest {
            replica_shard: Some(Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-leader".to_string(),
                follower_id: Some("test-follower".to_string()),
                ..Default::default()
            }),
            replication_seqno: 1,
        };
        let syn_replication_message =
            SynReplicationMessage::new_init_replica_request(init_replica_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let init_replica_response = into_init_replica_response(ack_replication_message);
        assert_eq!(init_replica_response.replication_seqno, 1);

        // Init shard 11.
        let init_replica_request = InitReplicaRequest {
            replica_shard: Some(Shard {
                index_uid: Some(index_uid2.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-leader".to_string(),
                follower_id: Some("test-follower".to_string()),
                ..Default::default()
            }),
            replication_seqno: 2,
        };
        let syn_replication_message =
            SynReplicationMessage::new_init_replica_request(init_replica_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let init_replica_response = into_init_replica_response(ack_replication_message);
        assert_eq!(init_replica_response.replication_seqno, 2);

        let state_guard = state.lock_fully().await.unwrap();

        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));

        let replica_shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        replica_shard_01.assert_is_replica();
        replica_shard_01.assert_is_open();
        replica_shard_01.assert_replication_position(Position::Beginning);
        replica_shard_01.assert_truncation_position(Position::Beginning);

        assert!(state_guard.mrecordlog.queue_exists(&queue_id_01));

        let queue_id_02 = queue_id(&index_uid, "test-source", &ShardId::from(2));

        let replica_shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        replica_shard_02.assert_is_replica();
        replica_shard_02.assert_is_open();
        replica_shard_02.assert_replication_position(Position::Beginning);
        replica_shard_02.assert_truncation_position(Position::Beginning);

        let queue_id_11 = queue_id(&index_uid2, "test-source", &ShardId::from(1));

        let replica_shard_11 = state_guard.shards.get(&queue_id_11).unwrap();
        replica_shard_11.assert_is_replica();
        replica_shard_11.assert_is_open();
        replica_shard_11.assert_replication_position(Position::Beginning);
        replica_shard_11.assert_truncation_position(Position::Beginning);

        drop(state_guard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                ReplicateSubrequest {
                    subrequest_id: 0,
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                    from_position_exclusive: Some(Position::Beginning),
                },
                ReplicateSubrequest {
                    subrequest_id: 1,
                    index_uid: Some(index_uid.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(2)),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-bar", "test-doc-baz"])),
                    from_position_exclusive: Some(Position::Beginning),
                },
                ReplicateSubrequest {
                    subrequest_id: 2,
                    index_uid: Some(index_uid2.clone()),
                    source_id: "test-source".to_string(),
                    shard_id: Some(ShardId::from(1)),
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-qux", "test-doc-tux"])),
                    from_position_exclusive: Some(Position::Beginning),
                },
            ],
            replication_seqno: 3,
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replicate_response(ack_replication_message);

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 3);
        assert_eq!(replicate_response.failures.len(), 0);
        assert_eq!(replicate_response.replication_seqno, 3);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid(), &index_uid);
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_success_0.replication_position_inclusive(),
            Position::offset(0u64)
        );

        let replicate_success_1 = &replicate_response.successes[1];
        assert_eq!(replicate_success_1.index_uid(), &index_uid);
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id(), ShardId::from(2));
        assert_eq!(
            replicate_success_1.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let replicate_success_2 = &replicate_response.successes[2];
        assert_eq!(replicate_success_2.index_uid(), &index_uid2);
        assert_eq!(replicate_success_2.source_id, "test-source");
        assert_eq!(replicate_success_2.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_success_2.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let state_guard = state.lock_fully().await.unwrap();

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, [0, 0], "test-doc-foo")]);

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_02,
            ..,
            &[(0, [0, 0], "test-doc-bar"), (1, [0, 0], "test-doc-baz")],
        );

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[(0, [0, 0], "test-doc-qux"), (1, [0, 0], "test-doc-tux")],
        );
        drop(state_guard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-moo"])),
                from_position_exclusive: Some(Position::offset(0u64)),
            }],
            replication_seqno: 4,
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replicate_response(ack_replication_message);

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 1);
        assert_eq!(replicate_response.failures.len(), 0);
        assert_eq!(replicate_response.replication_seqno, 4);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid(), &index_uid);
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_success_0.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let state_guard = state.lock_fully().await.unwrap();

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, [0, 0], "test-doc-foo"), (1, [0, 0], "test-doc-moo")],
        );
    }

    #[tokio::test]
    async fn test_replication_task_shard_closed() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let (syn_replication_stream_tx, syn_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let (ack_replication_stream_tx, mut ack_replication_stream) =
            ServiceStream::new_unbounded();

        let disk_capacity = ByteSize::mb(256);
        let memory_capacity = ByteSize::mb(1);

        let _replication_task_handle = ReplicationTask::spawn(
            leader_id.clone(),
            follower_id,
            state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
            disk_capacity,
            memory_capacity,
        );

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let replica_shard = IngesterShard::new_replica(
            leader_id,
            ShardState::Closed,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        state
            .lock_fully()
            .await
            .unwrap()
            .shards
            .insert(queue_id_01.clone(), replica_shard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: Position::offset(0u64).into(),
            }],
            replication_seqno: 0,
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replicate_response(ack_replication_message);

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 0);
        assert_eq!(replicate_response.failures.len(), 1);

        let replicate_failure = &replicate_response.failures[0];
        assert_eq!(replicate_failure.index_uid(), &index_uid);
        assert_eq!(replicate_failure.source_id, "test-source");
        assert_eq!(replicate_failure.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_failure.reason(),
            ReplicateFailureReason::ShardClosed
        );
    }

    #[cfg(not(feature = "failpoints"))]
    #[tokio::test]
    async fn test_replication_task_deletes_dangling_shard() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let (syn_replication_stream_tx, syn_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let (ack_replication_stream_tx, mut ack_replication_stream) =
            ServiceStream::new_unbounded();

        let disk_capacity = ByteSize::mb(256);
        let memory_capacity = ByteSize::mb(1);

        let _replication_task_handle = ReplicationTask::spawn(
            leader_id.clone(),
            follower_id,
            state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
            disk_capacity,
            memory_capacity,
        );

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let replica_shard = IngesterShard::new_replica(
            leader_id,
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        state
            .lock_fully()
            .await
            .unwrap()
            .shards
            .insert(queue_id_01.clone(), replica_shard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: Position::offset(0u64).into(),
            }],
            replication_seqno: 0,
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replicate_response(ack_replication_message);

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 0);
        assert_eq!(replicate_response.failures.len(), 1);

        let replicate_failure = &replicate_response.failures[0];
        assert_eq!(replicate_failure.index_uid(), &index_uid);
        assert_eq!(replicate_failure.source_id, "test-source");
        assert_eq!(replicate_failure.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_failure.reason(),
            ReplicateFailureReason::ShardNotFound
        );

        let state_guard = state.lock_partially().await.unwrap();
        assert!(!state_guard.shards.contains_key(&queue_id_01));
    }

    // This test should be run manually and independently of other tests with the `failpoints`
    // feature enabled:
    // ```sh
    // cargo test --manifest-path quickwit/Cargo.toml -p quickwit-ingest --features failpoints -- test_replication_task_closes_shard_on_io_error
    // ```
    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn test_replication_task_closes_shard_on_io_error() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("ingester:append_records", "return").unwrap();

        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let (syn_replication_stream_tx, syn_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let (ack_replication_stream_tx, mut ack_replication_stream) =
            ServiceStream::new_unbounded();

        let disk_capacity = ByteSize::mb(256);
        let memory_capacity = ByteSize::mb(1);

        let _replication_task_handle = ReplicationTask::spawn(
            leader_id.clone(),
            follower_id,
            state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
            disk_capacity,
            memory_capacity,
        );

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let replica_shard = IngesterShard::new_replica(
            leader_id,
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        let mut state_guard = state.lock_fully().await.unwrap();

        state_guard
            .shards
            .insert(queue_id_01.clone(), replica_shard);

        state_guard
            .mrecordlog
            .create_queue(&queue_id_01)
            .await
            .unwrap();

        drop(state_guard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: Position::offset(0u64).into(),
            }],
            replication_seqno: 0,
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replicate_response(ack_replication_message);

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 0);
        assert_eq!(replicate_response.failures.len(), 1);

        let replicate_failure = &replicate_response.failures[0];
        assert_eq!(replicate_failure.index_uid(), &index_uid);
        assert_eq!(replicate_failure.source_id, "test-source");
        assert_eq!(replicate_failure.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_failure.reason(),
            ReplicateFailureReason::ShardClosed
        );

        let state_guard = state.lock_partially().await.unwrap();
        let replica_shard = state_guard.shards.get(&queue_id_01).unwrap();
        replica_shard.assert_is_closed();

        scenario.teardown();
    }

    #[tokio::test]
    async fn test_replication_task_resource_exhausted() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (_temp_dir, state) = IngesterState::for_test().await;
        let (syn_replication_stream_tx, syn_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let (ack_replication_stream_tx, mut ack_replication_stream) =
            ServiceStream::new_unbounded();

        let disk_capacity = ByteSize(0);
        let memory_capacity = ByteSize(0);

        let _replication_task_handle = ReplicationTask::spawn(
            leader_id.clone(),
            follower_id,
            state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
            disk_capacity,
            memory_capacity,
        );

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let queue_id_01 = queue_id(&index_uid, "test-source", &ShardId::from(1));
        let replica_shard = IngesterShard::new_replica(
            leader_id,
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
            Instant::now(),
        );
        state
            .lock_fully()
            .await
            .unwrap()
            .shards
            .insert(queue_id_01.clone(), replica_shard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: Some(Position::Beginning),
            }],
            replication_seqno: 0,
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replicate_response(ack_replication_message);

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 0);
        assert_eq!(replicate_response.failures.len(), 1);

        let replicate_failure_0 = &replicate_response.failures[0];
        assert_eq!(replicate_failure_0.index_uid(), &index_uid);
        assert_eq!(replicate_failure_0.source_id, "test-source");
        assert_eq!(replicate_failure_0.shard_id(), ShardId::from(1));
        assert_eq!(
            replicate_failure_0.reason(),
            ReplicateFailureReason::WalFull
        );
    }
}
