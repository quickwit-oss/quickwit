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

use futures::{Future, StreamExt};
use quickwit_common::ServiceStream;
use quickwit_proto::ingest::ingester::{
    ack_replication_message, syn_replication_message, AckReplicationMessage, ReplicateRequest,
    ReplicateResponse, ReplicateSuccess, SynReplicationMessage,
};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error, IngestV2Result};
use quickwit_proto::types::{NodeId, Position};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;
use tracing::error;

use super::ingester::IngesterState;
use super::models::{IngesterShard, ReplicaShard};
use super::mrecord::MRecord;
use crate::metrics::INGEST_METRICS;

pub(super) const SYN_REPLICATION_STREAM_CAPACITY: usize = 5;

const REPLICATION_REQUEST_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(3)
};

type OneShotReplicateRequest = (ReplicateRequest, oneshot::Sender<ReplicateResponse>);

/// Replication sequence number.
type ReplicationSeqNo = u64;

/// Task that "powers" the replication stream between a leader and a follower.
pub(super) struct ReplicationStreamTask {
    leader_id: NodeId,
    follower_id: NodeId,
    replicate_request_queue_rx: mpsc::Receiver<OneShotReplicateRequest>,
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
        let (replicate_request_queue_tx, replicate_request_queue_rx) =
            mpsc::channel::<OneShotReplicateRequest>(3);

        let replication_stream_task = Self {
            leader_id,
            follower_id,
            replicate_request_queue_rx,
            syn_replication_stream_tx,
            ack_replication_stream,
        };
        let (enqueue_syn_requests_join_handle, dequeue_ack_responses_join_handle) =
            replication_stream_task.run();

        ReplicationStreamTaskHandle {
            replicate_request_queue_tx,
            replication_seqno_sequence: AtomicU64::default(),
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
        let leader_id = self.leader_id.clone();
        let follower_id = self.follower_id.clone();

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
            while let Some((replicate_request, oneshot_replicate_response_tx)) =
                self.replicate_request_queue_rx.recv().await
            {
                assert_eq!(
                    replicate_request.leader_id, leader_id,
                    "expected leader ID `{}`, got `{}`",
                    leader_id, replicate_request.leader_id
                );
                assert_eq!(
                    replicate_request.follower_id, follower_id,
                    "expected follower ID `{}`, got `{}`",
                    follower_id, replicate_request.follower_id
                );
                if response_sequencer_tx
                    .send((
                        replicate_request.replication_seqno,
                        oneshot_replicate_response_tx,
                    ))
                    .is_err()
                {
                    // The response sequencer receiver was dropped.
                    return;
                }
                let syn_replication_message =
                    SynReplicationMessage::new_replicate_request(replicate_request);
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
                let replicate_response = into_replicate_response(ack_replication_message);

                let oneshot_replicate_response_tx = match response_sequencer_rx.try_recv() {
                    Ok((replication_seqno, oneshot_replicate_response_tx)) => {
                        if replicate_response.replication_seqno != replication_seqno {
                            error!(
                                "received out-of-order replication response: expected replication \
                                 seqno `{}`, got `{}`; closing replication stream from leader \
                                 `{}` to follower `{}`",
                                replication_seqno,
                                replicate_response.replication_seqno,
                                self.leader_id,
                                self.follower_id,
                            );
                            return;
                        }
                        oneshot_replicate_response_tx
                    }
                    Err(TryRecvError::Empty) => {
                        panic!("the response sequencer should not be empty");
                    }
                    Err(TryRecvError::Disconnected) => {
                        // The response sequencer sender was dropped.
                        return;
                    }
                };
                // We intentionally ignore the error here. It is the responsibility of the
                // `replicate` method to surface it.
                let _ = oneshot_replicate_response_tx.send(replicate_response);
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
    replicate_request_queue_tx: mpsc::Sender<OneShotReplicateRequest>,
    enqueue_syn_requests_join_handle: JoinHandle<()>,
    dequeue_ack_responses_join_handle: JoinHandle<()>,
}

impl ReplicationStreamTaskHandle {
    pub fn replicate(
        &self,
        replicate_request: ReplicateRequest,
    ) -> impl Future<Output = Result<ReplicateResponse, ReplicationError>> + Send + 'static {
        let (oneshot_replicate_response_tx, oneshot_replicate_response_rx) = oneshot::channel();
        let replicate_request_queue_tx = self.replicate_request_queue_tx.clone();

        let send_recv_fut = async move {
            replicate_request_queue_tx
                .send((replicate_request, oneshot_replicate_response_tx))
                .await
                .map_err(|_| ReplicationError::Closed)?;
            let replicate_response = oneshot_replicate_response_rx
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

    pub fn next_replication_seqno(&self) -> ReplicationSeqNo {
        self.replication_seqno_sequence
            .fetch_add(1, Ordering::Relaxed)
    }
}

impl Drop for ReplicationStreamTaskHandle {
    fn drop(&mut self) {
        self.enqueue_syn_requests_join_handle.abort();
        self.dequeue_ack_responses_join_handle.abort();
    }
}

/// Error returned by [`ReplicationClient::replicate`].
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

/// Replication task executed for each replication stream.
pub(super) struct ReplicationTask {
    leader_id: NodeId,
    follower_id: NodeId,
    state: Arc<RwLock<IngesterState>>,
    syn_replication_stream: ServiceStream<SynReplicationMessage>,
    ack_replication_stream_tx: mpsc::UnboundedSender<IngestV2Result<AckReplicationMessage>>,
    current_replication_seqno: ReplicationSeqNo,
}

impl ReplicationTask {
    pub fn spawn(
        leader_id: NodeId,
        follower_id: NodeId,
        state: Arc<RwLock<IngesterState>>,
        syn_replication_stream: ServiceStream<SynReplicationMessage>,
        ack_replication_stream_tx: mpsc::UnboundedSender<IngestV2Result<AckReplicationMessage>>,
    ) -> ReplicationTaskHandle {
        let mut replication_task = Self {
            leader_id,
            follower_id,
            state,
            syn_replication_stream,
            ack_replication_stream_tx,
            current_replication_seqno: 0,
        };
        let join_handle = tokio::spawn(async move { replication_task.run().await });
        ReplicationTaskHandle { join_handle }
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

        let mut state_guard = self.state.write().await;

        for subrequest in replicate_request.subrequests {
            let queue_id = subrequest.queue_id();
            let from_position_exclusive = subrequest.from_position_exclusive();
            let to_position_inclusive = subrequest.to_position_inclusive();

            let replica_shard: &mut IngesterShard =
                if from_position_exclusive == Position::Beginning {
                    // Initialize the replica shard and corresponding mrecordlog queue.
                    state_guard
                        .mrecordlog
                        .create_queue(&queue_id)
                        .await
                        .expect("TODO");
                    let leader_id: NodeId = replicate_request.leader_id.clone().into();
                    let replica_shard = ReplicaShard::new(leader_id);
                    let shard = IngesterShard::Replica(replica_shard);
                    state_guard.shards.entry(queue_id.clone()).or_insert(shard)
                } else {
                    state_guard
                        .shards
                        .get_mut(&queue_id)
                        .expect("replica shard should be initialized")
                };
            if replica_shard.is_closed() {
                // TODO
            }
            if replica_shard.replication_position_inclusive() != from_position_exclusive {
                // TODO
            }
            let doc_batch = subrequest
                .doc_batch
                .expect("leader should not send empty replicate subrequests");

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
            .into();
            let batch_num_bytes = doc_batch.num_bytes() as u64;
            let batch_num_docs = doc_batch.num_docs() as u64;

            INGEST_METRICS
                .replicated_num_bytes_total
                .inc_by(batch_num_bytes);
            INGEST_METRICS
                .replicated_num_docs_total
                .inc_by(batch_num_docs);

            let replica_shard = state_guard
                .shards
                .get_mut(&queue_id)
                .expect("replica shard should exist");

            if current_position_inclusive != to_position_inclusive {
                return Err(IngestV2Error::Internal(format!(
                    "bad replica position: expected {to_position_inclusive:?}, got \
                     {current_position_inclusive:?}"
                )));
            }
            replica_shard.set_replication_position_inclusive(current_position_inclusive.clone());

            let replicate_success = ReplicateSuccess {
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
            failures: Vec::new(),
            replication_seqno: replicate_request.replication_seqno,
        };
        Ok(replicate_response)
    }

    async fn run(&mut self) -> IngestV2Result<()> {
        while let Some(syn_replication_message) = self.syn_replication_stream.next().await {
            let replicate_request = into_replicate_request(syn_replication_message);
            let ack_replication_message = self
                .replicate(replicate_request)
                .await
                .map(AckReplicationMessage::new_replicate_response);
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

fn into_replicate_request(syn_replication_message: SynReplicationMessage) -> ReplicateRequest {
    if let Some(syn_replication_message::Message::ReplicateRequest(replicate_request)) =
        syn_replication_message.message
    {
        return replicate_request;
    };
    panic!("SYN replication message should be a replicate request")
}

fn into_replicate_response(ack_replication_message: AckReplicationMessage) -> ReplicateResponse {
    if let Some(ack_replication_message::Message::ReplicateResponse(replicate_response)) =
        ack_replication_message.message
    {
        return replicate_response;
    };
    panic!("ACK replication message should be a replicate response")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mrecordlog::MultiRecordLog;
    use quickwit_proto::ingest::ingester::{ReplicateSubrequest, ReplicateSuccess};
    use quickwit_proto::ingest::DocBatchV2;
    use quickwit_proto::types::queue_id;

    use super::*;
    use crate::ingest_v2::test_utils::MultiRecordLogTestExt;

    #[tokio::test]
    async fn test_replication_stream_task() {
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
            while let Some(sync_replication_message) = syn_replication_stream_rx.recv().await {
                let replicate_request = sync_replication_message.into_replicate_request().unwrap();
                let replicate_successes = replicate_request
                    .subrequests
                    .iter()
                    .map(|subrequest| ReplicateSuccess {
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

        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                ReplicateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::from(0u64)),
                },
                ReplicateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 2,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-bar", "test-doc-baz"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::from(1u64)),
                },
                ReplicateSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    doc_batch: Some(DocBatchV2::for_test(["test-qux", "test-doc-tux"])),
                    from_position_exclusive: Some(Position::from(0u64)),
                    to_position_inclusive: Some(Position::from(2u64)),
                },
            ],
            replication_seqno: replication_stream_task_handle.next_replication_seqno(),
        };
        let replicate_response = replication_stream_task_handle
            .replicate(replicate_request)
            .await
            .unwrap();
        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 3);
        assert_eq!(replicate_response.failures.len(), 0);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(replicate_success_0.replication_position_inclusive(), 0u64);

        let replicate_success_1 = &replicate_response.successes[1];
        assert_eq!(replicate_success_1.index_uid, "test-index:0");
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id, 2);
        assert_eq!(replicate_success_1.replication_position_inclusive(), 1u64);

        let replicate_success_2 = &replicate_response.successes[2];
        assert_eq!(replicate_success_2.index_uid, "test-index:1");
        assert_eq!(replicate_success_2.source_id, "test-source");
        assert_eq!(replicate_success_2.shard_id, 1);
        assert_eq!(replicate_success_2.replication_position_inclusive(), 2u64);
    }

    #[tokio::test]
    async fn test_replication_stream_replicate_errors() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let (syn_replication_stream_tx, _syn_replication_stream_rx) = mpsc::channel(5);
        let (_ack_replication_stream_tx, ack_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let replication_stream_task_handle = ReplicationStreamTask::spawn(
            leader_id,
            follower_id,
            syn_replication_stream_tx,
            ack_replication_stream,
        );
        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            ..Default::default()
        };
        let timeout_error = replication_stream_task_handle
            .replicate(replicate_request.clone())
            .await
            .unwrap_err();
        assert!(matches!(timeout_error, ReplicationError::Timeout));

        replication_stream_task_handle
            .enqueue_syn_requests_join_handle
            .abort();

        let closed_error = replication_stream_task_handle
            .replicate(replicate_request)
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
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
        }));
        let (syn_replication_stream_tx, syn_replication_stream) =
            ServiceStream::new_bounded(SYN_REPLICATION_STREAM_CAPACITY);
        let (ack_replication_stream_tx, mut ack_replication_stream) =
            ServiceStream::new_unbounded();
        let _replication_task_handle = ReplicationTask::spawn(
            leader_id,
            follower_id,
            state.clone(),
            syn_replication_stream,
            ack_replication_stream_tx,
        );
        let replicate_request = ReplicateRequest {
            leader_id: "test-leader".to_string(),
            follower_id: "test-follower".to_string(),
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![
                ReplicateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-foo"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::from(0u64)),
                },
                ReplicateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 2,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-bar", "test-doc-baz"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::from(1u64)),
                },
                ReplicateSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    doc_batch: Some(DocBatchV2::for_test(["test-doc-qux", "test-doc-tux"])),
                    from_position_exclusive: None,
                    to_position_inclusive: Some(Position::from(1u64)),
                },
            ],
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
        assert_eq!(replicate_response.successes.len(), 3);
        assert_eq!(replicate_response.failures.len(), 0);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(replicate_success_0.replication_position_inclusive(), 0u64);

        let replicate_success_1 = &replicate_response.successes[1];
        assert_eq!(replicate_success_1.index_uid, "test-index:0");
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id, 2);
        assert_eq!(replicate_success_1.replication_position_inclusive(), 1u64);

        let replicate_success_2 = &replicate_response.successes[2];
        assert_eq!(replicate_success_2.index_uid, "test-index:1");
        assert_eq!(replicate_success_2.source_id, "test-source");
        assert_eq!(replicate_success_2.shard_id, 1);
        assert_eq!(replicate_success_2.replication_position_inclusive(), 1u64);

        let state_guard = state.read().await;

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, "\0\0test-doc-foo")]);

        let queue_id_02 = queue_id("test-index:0", "test-source", 2);

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_02,
            ..,
            &[(0, "\0\0test-doc-bar"), (1, "\0\0test-doc-baz")],
        );

        let queue_id_11 = queue_id("test-index:1", "test-source", 1);

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
                index_uid: "test-index:0".to_string(),
                source_id: "test-source".to_string(),
                shard_id: 1,
                doc_batch: Some(DocBatchV2::for_test(["test-doc-moo"])),
                from_position_exclusive: Some(Position::from(0u64)),
                to_position_inclusive: Some(Position::from(1u64)),
            }],
            replication_seqno: 1,
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

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(replicate_success_0.replication_position_inclusive(), 1u64);

        let state_guard = state.read().await;

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "\0\0test-doc-foo"), (1, "\0\0test-doc-moo")],
        );
    }
}
