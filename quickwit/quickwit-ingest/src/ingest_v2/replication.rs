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

use std::iter::once;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytesize::ByteSize;
use futures::{Future, StreamExt};
use quickwit_common::ServiceStream;
use quickwit_proto::ingest::ingester::{
    ack_replication_message, syn_replication_message, AckReplicationMessage, IngesterStatus,
    InitReplicaRequest, InitReplicaResponse, ReplicateFailure, ReplicateFailureReason,
    ReplicateRequest, ReplicateResponse, ReplicateSubrequest, ReplicateSuccess,
    SynReplicationMessage,
};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error, IngestV2Result, Shard, ShardState};
use quickwit_proto::types::{NodeId, Position};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;
use tracing::{error, warn};

use super::ingester::IngesterState;
use super::models::IngesterShard;
use super::mrecord::MRecord;
use super::mrecordlog_utils::check_enough_capacity;
use crate::ingest_v2::metrics::INGEST_V2_METRICS;
use crate::metrics::INGEST_METRICS;
use crate::{estimate_size, with_request_metrics};

pub(super) const SYN_REPLICATION_STREAM_CAPACITY: usize = 5;

/// Duration after which replication requests time out with [`ReplicationError::Timeout`].
const REPLICATION_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
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
            replication_seqno_sequence: Default::default(),
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
            while let Some((replication_request, oneshot_replication_response_tx)) =
                self.replication_request_rx.recv().await
            {
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
    replication_seqno_sequence: AtomicU64,
    replication_request_tx: mpsc::Sender<OneShotReplicationRequest>,
    enqueue_syn_requests_join_handle: JoinHandle<()>,
    dequeue_ack_responses_join_handle: JoinHandle<()>,
}

impl ReplicationStreamTaskHandle {
    /// Returns a [`ReplicationClient`] that can be used to enqueue replication requests
    /// into the replication stream.
    pub fn replication_client(&self) -> ReplicationClient {
        let replication_seqno = self
            .replication_seqno_sequence
            .fetch_add(1, Ordering::Relaxed);

        ReplicationClient {
            replication_seqno,
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

impl ReplicationError {
    pub(super) fn label_value(&self) -> &'static str {
        match self {
            Self::Timeout { .. } => "timeout",
            _ => "error",
        }
    }
}

// DO NOT derive or implement `Clone` for this object.
#[derive(Debug)]
pub(super) struct ReplicationClient {
    replication_seqno: u64,
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
            replication_seqno: self.replication_seqno,
        };
        let replication_request = ReplicationRequest::Init(init_replica_request);

        async {
            with_request_metrics!(
                self.submit(replication_request).await,
                "ingester",
                "client",
                "init_replica"
            )
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
            replication_seqno: self.replication_seqno,
        };
        let replication_request = ReplicationRequest::Replicate(replicate_request);

        async {
            with_request_metrics!(
                self.submit(replication_request).await,
                "ingester",
                "client",
                "replicate"
            )
            .map(|replication_response| {
                if let ReplicationResponse::Replicate(replicate_response) = replication_response {
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
    state: Arc<RwLock<IngesterState>>,
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
        state: Arc<RwLock<IngesterState>>,
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

        let mut state_guard = self.state.write().await;

        state_guard
            .mrecordlog
            .create_queue(&queue_id)
            .await
            .expect("TODO: Handle IO error");

        let replica_shard = IngesterShard::new_replica(
            replica_shard.leader_id.into(),
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
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
        self.current_replication_seqno += 1;

        let commit_type = replicate_request.commit_type();
        let force_commit = commit_type == CommitTypeV2::Force;

        let mut replicate_successes = Vec::with_capacity(replicate_request.subrequests.len());
        let mut replicate_failures = Vec::new();

        let mut state_guard = self.state.write().await;

        if state_guard.status != IngesterStatus::Ready {
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
        for subrequest in replicate_request.subrequests {
            let queue_id = subrequest.queue_id();
            let from_position_exclusive = subrequest.from_position_exclusive();
            let to_position_inclusive = subrequest.to_position_inclusive();

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

            if shard.shard_state.is_closed() {
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
            let requested_capacity = estimate_size(&doc_batch);

            let current_usage = match check_enough_capacity(
                &state_guard.mrecordlog,
                self.disk_capacity,
                self.memory_capacity,
                requested_capacity,
            ) {
                Ok(usage) => usage,
                Err(error) => {
                    warn!("failed to replicate records: {error}");

                    let replicate_failure = ReplicateFailure {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid,
                        source_id: subrequest.source_id,
                        shard_id: subrequest.shard_id,
                        reason: ReplicateFailureReason::ResourceExhausted as i32,
                    };
                    replicate_failures.push(replicate_failure);
                    continue;
                }
            };
            let current_position_inclusive: Position = if force_commit {
                let encoded_mrecords = doc_batch
                    .docs()
                    .map(|doc| MRecord::Doc(doc).encode())
                    .chain(once(MRecord::Commit.encode()));
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, encoded_mrecords)
                    .await
                    .expect("TODO")
            } else {
                let encoded_mrecords = doc_batch.docs().map(|doc| MRecord::Doc(doc).encode());
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, encoded_mrecords)
                    .await
                    .expect("TODO")
            }
            .map(Position::offset)
            .expect("records should not be empty");

            let new_disk_usage = current_usage.disk + requested_capacity;
            let new_memory_usage = current_usage.memory + requested_capacity;

            INGEST_V2_METRICS
                .wal_disk_usage_bytes
                .set(new_disk_usage.as_u64() as i64);
            INGEST_V2_METRICS
                .wal_memory_usage_bytes
                .set(new_memory_usage.as_u64() as i64);

            let batch_num_bytes = doc_batch.num_bytes() as u64;
            let batch_num_docs = doc_batch.num_docs() as u64;

            INGEST_METRICS
                .replicated_num_bytes_total
                .inc_by(batch_num_bytes);
            INGEST_METRICS
                .replicated_num_docs_total
                .inc_by(batch_num_docs);

            if current_position_inclusive != to_position_inclusive {
                return Err(IngestV2Error::Internal(format!(
                    "bad replica position: expected {to_position_inclusive:?}, got \
                     {current_position_inclusive:?}"
                )));
            }
            let replica_shard = state_guard
                .shards
                .get_mut(&queue_id)
                .expect("replica shard should be initialized");
            replica_shard.set_replication_position_inclusive(current_position_inclusive.clone());

            let replicate_success = ReplicateSuccess {
                subrequest_id: subrequest.subrequest_id,
                index_uid: subrequest.index_uid,
                source_id: subrequest.source_id,
                shard_id: subrequest.shard_id,
                replication_position_inclusive: Some(current_position_inclusive),
            };
            replicate_successes.push(replicate_success);
        }
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
                Some(syn_replication_message::Message::InitRequest(init_replica_request)) => {
                    with_request_metrics!(
                        self.init_replica(init_replica_request).await,
                        "ingester",
                        "server",
                        "init_replica"
                    )
                    .map(AckReplicationMessage::new_init_replica_response)
                }
                Some(syn_replication_message::Message::ReplicateRequest(replicate_request)) => {
                    with_request_metrics!(
                        self.replicate(replicate_request).await,
                        "ingester",
                        "server",
                        "replicate"
                    )
                    .map(AckReplicationMessage::new_replicate_response)
                }
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
    use std::collections::HashMap;

    use mrecordlog::MultiRecordLog;
    use quickwit_proto::ingest::ingester::{
        ObservationMessage, ReplicateSubrequest, ReplicateSuccess,
    };
    use quickwit_proto::ingest::{DocBatchV2, Shard};
    use quickwit_proto::types::queue_id;
    use tokio::sync::watch;

    use super::*;
    use crate::ingest_v2::test_utils::MultiRecordLogTestExt;

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

        let replica_shard = Shard {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            shard_id: 1,
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
                    .map(|subrequest| ReplicateSuccess {
                        subrequest_id: subrequest.subrequest_id,
                        index_uid: subrequest.index_uid.clone(),
                        source_id: subrequest.source_id.clone(),
                        shard_id: subrequest.shard_id,
                        replication_position_inclusive: Some(subrequest.to_position_inclusive()),
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

        let subrequests = vec![
            ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: None,
                to_position_inclusive: Some(Position::offset(0u64)),
            },
            ReplicateSubrequest {
                subrequest_id: 1,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 2,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-bar", "test-doc-baz"])),
                from_position_exclusive: None,
                to_position_inclusive: Some(Position::offset(1u64)),
            },
            ReplicateSubrequest {
                subrequest_id: 2,
                index_uid: "test-index:1".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                doc_batch: Some(DocBatchV2::for_test(["test-qux", "test-doc-tux"])),
                from_position_exclusive: Some(Position::offset(0u64)),
                to_position_inclusive: Some(Position::offset(2u64)),
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
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(
            replicate_success_0.replication_position_inclusive(),
            Position::offset(0u64)
        );

        let replicate_success_1 = &replicate_response.successes[1];
        assert_eq!(replicate_success_1.index_uid, "test-index:0");
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id, 2);
        assert_eq!(
            replicate_success_1.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let replicate_success_2 = &replicate_response.successes[2];
        assert_eq!(replicate_success_2.index_uid, "test-index:1");
        assert_eq!(replicate_success_2.source_id, "test-source");
        assert_eq!(replicate_success_2.shard_id, 1);
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
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let (observation_tx, _observation_rx) = watch::channel(Ok(ObservationMessage::default()));
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            rate_trackers: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
            status: IngesterStatus::Ready,
            observation_tx,
        }));
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

        // Init shard 01.
        let init_replica_request = InitReplicaRequest {
            replica_shard: Some(Shard {
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
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
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 2,
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
                index_uid: "test-index:1".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
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

        let state_guard = state.read().await;

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);

        let replica_shard_01 = state_guard.shards.get(&queue_id_01).unwrap();
        replica_shard_01.assert_is_replica();
        replica_shard_01.assert_is_open();
        replica_shard_01.assert_replication_position(Position::Beginning);
        replica_shard_01.assert_truncation_position(Position::Beginning);

        assert!(state_guard.mrecordlog.queue_exists(&queue_id_01));

        let queue_id_02 = queue_id("test-index:0", "test-source", 2);

        let replica_shard_02 = state_guard.shards.get(&queue_id_02).unwrap();
        replica_shard_02.assert_is_replica();
        replica_shard_02.assert_is_open();
        replica_shard_02.assert_replication_position(Position::Beginning);
        replica_shard_02.assert_truncation_position(Position::Beginning);

        let queue_id_11 = queue_id("test-index:1", "test-source", 1);

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
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::offset(0u64)),
                },
                ReplicateSubrequest {
                    subrequest_id: 1,
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 2,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-bar", "test-doc-baz"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::offset(1u64)),
                },
                ReplicateSubrequest {
                    subrequest_id: 2,
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-qux", "test-doc-tux"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::offset(1u64)),
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
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(
            replicate_success_0.replication_position_inclusive(),
            Position::offset(0u64)
        );

        let replicate_success_1 = &replicate_response.successes[1];
        assert_eq!(replicate_success_1.index_uid, "test-index:0");
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id, 2);
        assert_eq!(
            replicate_success_1.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let replicate_success_2 = &replicate_response.successes[2];
        assert_eq!(replicate_success_2.index_uid, "test-index:1");
        assert_eq!(replicate_success_2.source_id, "test-source");
        assert_eq!(replicate_success_2.shard_id, 1);
        assert_eq!(
            replicate_success_2.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let state_guard = state.read().await;

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, "\0\0test-doc-foo")]);

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_02,
            ..,
            &[(0, "\0\0test-doc-bar"), (1, "\0\0test-doc-baz")],
        );

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[(0, "\0\0test-doc-qux"), (1, "\0\0test-doc-tux")],
        );
        drop(state_guard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-moo"])),
                from_position_exclusive: Some(Position::offset(0u64)),
                to_position_inclusive: Some(Position::offset(1u64)),
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
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(
            replicate_success_0.replication_position_inclusive(),
            Position::offset(1u64)
        );

        let state_guard = state.read().await;

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-foo"), (1, "\0\0test-doc-moo")],
        );
    }

    #[tokio::test]
    async fn test_replication_task_shard_closed() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let (observation_tx, _observation_rx) = watch::channel(Ok(ObservationMessage::default()));
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            rate_trackers: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
            status: IngesterStatus::Ready,
            observation_tx,
        }));
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

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let replica_shard = IngesterShard::new_replica(
            leader_id,
            ShardState::Closed,
            Position::Beginning,
            Position::Beginning,
        );
        state
            .write()
            .await
            .shards
            .insert(queue_id_01.clone(), replica_shard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: Position::offset(0u64).into(),
                to_position_inclusive: Some(Position::offset(1u64)),
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
        assert_eq!(replicate_failure.index_uid, "test-index:0");
        assert_eq!(replicate_failure.source_id, "test-source");
        assert_eq!(replicate_failure.shard_id, 1);
        assert_eq!(
            replicate_failure.reason(),
            ReplicateFailureReason::ShardClosed
        );
    }

    #[tokio::test]
    async fn test_replication_task_resource_exhausted() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let (observation_tx, _observation_rx) = watch::channel(Ok(ObservationMessage::default()));
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            rate_trackers: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
            status: IngesterStatus::Ready,
            observation_tx,
        }));
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

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let replica_shard = IngesterShard::new_replica(
            leader_id,
            ShardState::Open,
            Position::Beginning,
            Position::Beginning,
        );
        state
            .write()
            .await
            .shards
            .insert(queue_id_01.clone(), replica_shard);

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![ReplicateSubrequest {
                subrequest_id: 0,
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                from_position_exclusive: None,
                to_position_inclusive: Some(Position::offset(0u64)),
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
        assert_eq!(replicate_failure_0.index_uid, "test-index:0");
        assert_eq!(replicate_failure_0.source_id, "test-source");
        assert_eq!(replicate_failure_0.shard_id, 1);
        assert_eq!(
            replicate_failure_0.reason(),
            ReplicateFailureReason::ResourceExhausted
        );
    }
}
