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
use std::sync::Arc;

use futures::StreamExt;
use quickwit_common::ServiceStream;
use quickwit_proto::ingest::ingester::{
    ack_replication_message, syn_replication_message, AckReplicationMessage, ReplicateRequest,
    ReplicateResponse, ReplicateSuccess, SynReplicationMessage,
};
use quickwit_proto::ingest::{CommitTypeV2, IngestV2Error, IngestV2Result, ShardState};
use quickwit_proto::types::NodeId;
use tokio::sync::{mpsc, oneshot, watch, RwLock};
use tokio::task::JoinHandle;

use super::ingester::{commit_doc, IngesterState};
use super::models::{Position, ReplicaShard, ShardStatus};
use crate::metrics::INGEST_METRICS;

/// A replication request is sent by the leader to its follower to update the state of a replica
/// shard.
#[derive(Debug)]
pub(super) enum ReplicationRequest {
    Replicate(ReplicateRequest),
}

#[derive(Debug)]
pub(super) enum ReplicationResponse {
    Replicate(ReplicateResponse),
}

impl ReplicationResponse {
    pub fn into_replicate_response(self) -> Option<ReplicateResponse> {
        match self {
            ReplicationResponse::Replicate(replicate_response) => Some(replicate_response),
        }
    }
}

type OneShotReplicationRequest = (ReplicationRequest, oneshot::Sender<ReplicationResponse>);

/// Offers a request-response API on top of a gRPC bi-directional replication stream. There should
/// be one replication client per leader-follower pair.
#[derive(Clone)]
pub(super) struct ReplicationClient {
    oneshot_replication_request_tx: mpsc::UnboundedSender<OneShotReplicationRequest>,
}

impl ReplicationClient {
    /// Replicates a persist request from a leader to its follower and waits for its response.
    pub async fn replicate(
        &self,
        replicate_request: ReplicateRequest,
    ) -> IngestV2Result<ReplicateResponse> {
        let replication_request = ReplicationRequest::Replicate(replicate_request);
        let (replication_response_tx, replication_response_rx) = oneshot::channel();
        self.oneshot_replication_request_tx
            .clone()
            .send((replication_request, replication_response_tx))
            .expect("TODO");
        let replicate_response = replication_response_rx
            .await
            .expect("TODO")
            .into_replicate_response()
            .expect("TODO");
        Ok(replicate_response)
    }
}

/// Processes [`ReplicateRequest`] requests sent by a leader. It queues requests and pipes them into
/// its underlying replication stream, then waits for responses from the follower on the other end
/// of the stream.
pub(super) struct ReplicationClientTask {
    syn_replication_stream_tx: mpsc::Sender<SynReplicationMessage>,
    ack_replication_stream: ServiceStream<IngestV2Result<AckReplicationMessage>>,
    oneshot_replication_request_rx: mpsc::UnboundedReceiver<OneShotReplicationRequest>,
}

impl ReplicationClientTask {
    /// Spawns a [`ReplicationClientTask`].
    pub fn spawn(
        syn_replication_stream_tx: mpsc::Sender<SynReplicationMessage>,
        ack_replication_stream: ServiceStream<IngestV2Result<AckReplicationMessage>>,
    ) -> ReplicationClient {
        let (oneshot_replication_request_tx, oneshot_replication_request_rx) =
            mpsc::unbounded_channel::<OneShotReplicationRequest>(); // TODO: bound and handle backpressure on the other side.

        let mut replication_client_task = Self {
            syn_replication_stream_tx,
            ack_replication_stream,
            oneshot_replication_request_rx,
        };
        let future = async move {
            replication_client_task.run().await;
        };
        tokio::spawn(future);

        ReplicationClient {
            oneshot_replication_request_tx,
        }
    }

    /// Executes the processing loop.
    // TODO: There is a major flaw in this implementation: it processes requests sequentially, while
    // it should be able to enqueue incoming replication requests while waiting for the next
    // replication response from the follower.
    async fn run(&mut self) {
        while let Some((replication_request, replication_response_tx)) =
            self.oneshot_replication_request_rx.recv().await
        {
            // TODO: Batch requests.
            let syn_replication_message = match replication_request {
                ReplicationRequest::Replicate(replication_request) => {
                    SynReplicationMessage::new_replicate_request(replication_request)
                }
            };
            self.syn_replication_stream_tx
                .send(syn_replication_message)
                .await
                .expect("TODO");
            let ack_replication_message = self
                .ack_replication_stream
                .next()
                .await
                .expect("TODO")
                .expect("TODO");
            let replication_response =
                into_replication_response(ack_replication_message).expect("");
            replication_response_tx
                .send(replication_response)
                .expect("TODO");
        }
    }
}

pub(super) struct ReplicationTaskHandle {
    _join_handle: JoinHandle<IngestV2Result<()>>,
}

/// Replication task executed per replication stream.
pub(super) struct ReplicationTask {
    leader_id: NodeId,
    follower_id: NodeId,
    state: Arc<RwLock<IngesterState>>,
    syn_replication_stream: ServiceStream<SynReplicationMessage>,
    ack_replication_stream_tx: mpsc::Sender<IngestV2Result<AckReplicationMessage>>,
}

impl ReplicationTask {
    pub fn spawn(
        leader_id: NodeId,
        follower_id: NodeId,
        state: Arc<RwLock<IngesterState>>,
        syn_replication_stream: ServiceStream<SynReplicationMessage>,
        ack_replication_stream_tx: mpsc::Sender<IngestV2Result<AckReplicationMessage>>,
    ) -> ReplicationTaskHandle {
        let mut replication_task = Self {
            leader_id,
            follower_id,
            state,
            syn_replication_stream,
            ack_replication_stream_tx,
        };
        let future = async move { replication_task.run().await };
        let _join_handle = tokio::spawn(future);
        ReplicationTaskHandle { _join_handle }
    }

    async fn replicate(
        &mut self,
        replicate_request: ReplicateRequest,
    ) -> IngestV2Result<ReplicateResponse> {
        if replicate_request.leader_id != self.leader_id {
            return Err(IngestV2Error::Internal(format!(
                "invalid argument: expected leader ID `{}`, got `{}`",
                self.leader_id, replicate_request.leader_id
            )));
        }
        if replicate_request.follower_id != self.follower_id {
            return Err(IngestV2Error::Internal(format!(
                "invalid argument: expected follower ID `{}`, got `{}`",
                self.follower_id, replicate_request.follower_id
            )));
        }
        let commit_type = replicate_request.commit_type();
        let force_commit = commit_type == CommitTypeV2::Force;
        let mut replicate_successes = Vec::with_capacity(replicate_request.subrequests.len());

        let mut state_guard = self.state.write().await;

        for subrequest in replicate_request.subrequests {
            let queue_id = subrequest.queue_id();

            let replica_shard: &mut ReplicaShard = if subrequest.from_position_exclusive.is_none() {
                // Initialize the replica shard and corresponding mrecordlog queue.
                state_guard
                    .mrecordlog
                    .create_queue(&queue_id)
                    .await
                    .expect("TODO");
                state_guard
                    .replica_shards
                    .entry(queue_id.clone())
                    .or_insert_with(|| {
                        let (shard_status_tx, shard_status_rx) =
                            watch::channel(ShardStatus::default());
                        ReplicaShard {
                            _leader_id: replicate_request.leader_id.clone().into(),
                            shard_state: ShardState::Open,
                            publish_position_inclusive: Position::default(),
                            replica_position_inclusive: Position::default(),
                            shard_status_tx,
                            shard_status_rx,
                        }
                    })
            } else {
                state_guard
                    .replica_shards
                    .get_mut(&queue_id)
                    .expect("The replica shard should be initialized.")
            };
            if replica_shard.shard_state.is_closed() {
                // TODO
            }
            let to_position_inclusive = subrequest.to_position_inclusive();
            // let replica_position_inclusive = replica_shard.replica_position_inclusive;

            // TODO: Check if subrequest.from_position_exclusive == replica_position_exclusive.
            // If not, check if we should skip the subrequest or not.
            // if subrequest.from_position_exclusive != replica_position_exclusive {
            //     return Err(IngestV2Error::Internal(format!(
            //         "Bad replica position: expected {}, got {}.",
            //         subrequest.replica_position_inclusive, replica_position_exclusive
            //     )));
            let Some(doc_batch) = subrequest.doc_batch else {
                let replicate_success = ReplicateSuccess {
                    index_uid: subrequest.index_uid,
                    source_id: subrequest.source_id,
                    shard_id: subrequest.shard_id,
                    replica_position_inclusive: subrequest.from_position_exclusive,
                };
                replicate_successes.push(replicate_success);
                continue;
            };
            let replica_position_inclusive = if force_commit {
                let docs = doc_batch.docs().chain(once(commit_doc()));
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, docs)
                    .await
                    .expect("TODO")
            } else {
                let docs = doc_batch.docs();
                state_guard
                    .mrecordlog
                    .append_records(&queue_id, None, docs)
                    .await
                    .expect("TODO")
            };
            let batch_num_bytes = doc_batch.num_bytes() as u64;
            let batch_num_docs = doc_batch.num_docs() as u64;

            INGEST_METRICS
                .replicated_num_bytes_total
                .inc_by(batch_num_bytes);
            INGEST_METRICS
                .replicated_num_docs_total
                .inc_by(batch_num_docs);

            let replica_shard = state_guard
                .replica_shards
                .get_mut(&queue_id)
                .expect("Replica shard should exist.");

            if replica_position_inclusive != to_position_inclusive {
                return Err(IngestV2Error::Internal(format!(
                    "bad replica position: expected {to_position_inclusive:?}, got \
                     {replica_position_inclusive:?}"
                )));
            }
            replica_shard.set_replica_position_inclusive(replica_position_inclusive);

            let replicate_success = ReplicateSuccess {
                index_uid: subrequest.index_uid,
                source_id: subrequest.source_id,
                shard_id: subrequest.shard_id,
                replica_position_inclusive,
            };
            replicate_successes.push(replicate_success);
        }
        let follower_id = self.follower_id.clone().into();
        let replicate_response = ReplicateResponse {
            follower_id,
            successes: replicate_successes,
            failures: Vec::new(),
        };
        Ok(replicate_response)
    }

    async fn run(&mut self) -> IngestV2Result<()> {
        while let Some(syn_replication_message) = self.syn_replication_stream.next().await {
            let ack_replication_message = match syn_replication_message.message {
                Some(syn_replication_message::Message::ReplicateRequest(replicate_request)) => self
                    .replicate(replicate_request)
                    .await
                    .map(AckReplicationMessage::new_replicate_response),
                _ => panic!("TODO"),
            };
            if self
                .ack_replication_stream_tx
                .send(ack_replication_message)
                .await
                .is_err()
            {
                break;
            }
        }
        Ok(())
    }
}

fn into_replication_response(outer_message: AckReplicationMessage) -> Option<ReplicationResponse> {
    match outer_message.message {
        Some(ack_replication_message::Message::ReplicateResponse(replicate_response)) => {
            Some(ReplicationResponse::Replicate(replicate_response))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use mrecordlog::MultiRecordLog;
    use quickwit_proto::ingest::ingester::{ReplicateSubrequest, ReplicateSuccess};
    use quickwit_proto::ingest::DocBatchV2;
    use quickwit_proto::types::queue_id;

    use super::*;
    use crate::ingest_v2::test_utils::{MultiRecordLogTestExt, ReplicaShardTestExt};

    #[tokio::test]
    async fn test_replication_client() {
        let (syn_replication_stream_tx, mut syn_replication_stream_rx) = mpsc::channel(5);
        let (ack_replication_stream_tx, ack_replication_stream) = ServiceStream::new_bounded(5);
        let replication_client =
            ReplicationClientTask::spawn(syn_replication_stream_tx, ack_replication_stream);

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
                        replica_position_inclusive: subrequest.to_position_inclusive(),
                    })
                    .collect::<Vec<_>>();

                let replicate_response = ReplicateResponse {
                    follower_id: replicate_request.follower_id,
                    successes: replicate_successes,
                    failures: Vec::new(),
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
                    shard_id: 0,
                    from_position_exclusive: None,
                    doc_batch: None,
                },
                ReplicateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    from_position_exclusive: None,
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-010"),
                        doc_lengths: vec![12],
                    }),
                },
                ReplicateSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    from_position_exclusive: Some(0),
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-111test-doc-112"),
                        doc_lengths: vec![12],
                    }),
                },
            ],
        };
        let replicate_response = replication_client
            .replicate(replicate_request)
            .await
            .unwrap();
        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 3);
        assert_eq!(replicate_response.failures.len(), 0);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 0);
        assert_eq!(replicate_success_0.replica_position_inclusive, None);

        let replicate_success_0 = &replicate_response.successes[1];
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(replicate_success_0.replica_position_inclusive, Some(0));

        let replicate_success_1 = &replicate_response.successes[2];
        assert_eq!(replicate_success_1.index_uid, "test-index:1");
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id, 1);
        assert_eq!(replicate_success_1.replica_position_inclusive, Some(1));
    }

    #[tokio::test]
    async fn test_replication_task_happy_path() {
        let leader_id: NodeId = "test-leader".into();
        let follower_id: NodeId = "test-follower".into();
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            primary_shards: HashMap::new(),
            replica_shards: HashMap::new(),
            replication_clients: HashMap::new(),
            replication_tasks: HashMap::new(),
        }));
        let (syn_replication_stream_tx, syn_replication_stream) = ServiceStream::new_bounded(5);
        let (ack_replication_stream_tx, mut ack_replication_stream) = ServiceStream::new_bounded(5);
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
                    shard_id: 0,
                    from_position_exclusive: None,
                    doc_batch: None,
                },
                ReplicateSubrequest {
                    index_uid: "test-index:0".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    from_position_exclusive: None,
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-010"),
                        doc_lengths: vec![12],
                    }),
                },
                ReplicateSubrequest {
                    index_uid: "test-index:1".to_string(),
                    source_id: "test-source".to_string(),
                    shard_id: 1,
                    from_position_exclusive: None,
                    doc_batch: Some(DocBatchV2 {
                        doc_buffer: Bytes::from_static(b"test-doc-110test-doc-111"),
                        doc_lengths: vec![12, 12],
                    }),
                },
            ],
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replication_response(ack_replication_message)
            .unwrap()
            .into_replicate_response()
            .unwrap();

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 3);
        assert_eq!(replicate_response.failures.len(), 0);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 0);
        assert_eq!(replicate_success_0.replica_position_inclusive, None);

        let replicate_success_1 = &replicate_response.successes[1];
        assert_eq!(replicate_success_1.index_uid, "test-index:0");
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id, 1);
        assert_eq!(replicate_success_1.replica_position_inclusive, Some(0));

        let replicate_success_1 = &replicate_response.successes[2];
        assert_eq!(replicate_success_1.index_uid, "test-index:1");
        assert_eq!(replicate_success_1.source_id, "test-source");
        assert_eq!(replicate_success_1.shard_id, 1);
        assert_eq!(replicate_success_1.replica_position_inclusive, Some(1));

        let state_guard = state.read().await;

        assert!(state_guard.primary_shards.is_empty());
        assert_eq!(state_guard.replica_shards.len(), 3);

        let queue_id_00 = queue_id("test-index:0", "test-source", 0);
        let replica_shard_00 = state_guard.replica_shards.get(&queue_id_00).unwrap();
        replica_shard_00.assert_is_open(None);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_00, .., &[]);

        let queue_id_01 = queue_id("test-index:0", "test-source", 1);
        let replica_shard_01 = state_guard.replica_shards.get(&queue_id_01).unwrap();
        replica_shard_01.assert_is_open(0);

        state_guard
            .mrecordlog
            .assert_records_eq(&queue_id_01, .., &[(0, "test-doc-010")]);

        let queue_id_11 = queue_id("test-index:1", "test-source", 1);
        let replica_shard_11 = state_guard.replica_shards.get(&queue_id_11).unwrap();
        replica_shard_11.assert_is_open(1);

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_11,
            ..,
            &[(0, "test-doc-110"), (1, "test-doc-111")],
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
                from_position_exclusive: Some(0),
                doc_batch: Some(DocBatchV2 {
                    doc_buffer: Bytes::from_static(b"test-doc-011"),
                    doc_lengths: vec![12],
                }),
            }],
        };
        let syn_replication_message =
            SynReplicationMessage::new_replicate_request(replicate_request);
        syn_replication_stream_tx
            .send(syn_replication_message)
            .await
            .unwrap();
        let ack_replication_message = ack_replication_stream.next().await.unwrap().unwrap();
        let replicate_response = into_replication_response(ack_replication_message)
            .unwrap()
            .into_replicate_response()
            .unwrap();

        assert_eq!(replicate_response.follower_id, "test-follower");
        assert_eq!(replicate_response.successes.len(), 1);
        assert_eq!(replicate_response.failures.len(), 0);

        let replicate_success_0 = &replicate_response.successes[0];
        assert_eq!(replicate_success_0.index_uid, "test-index:0");
        assert_eq!(replicate_success_0.source_id, "test-source");
        assert_eq!(replicate_success_0.shard_id, 1);
        assert_eq!(replicate_success_0.replica_position_inclusive, Some(1));

        let state_guard = state.read().await;

        state_guard.mrecordlog.assert_records_eq(
            &queue_id_01,
            ..,
            &[(0, "test-doc-010"), (1, "test-doc-011")],
        );
        let replica_shard_01 = state_guard.replica_shards.get(&queue_id_01).unwrap();
        replica_shard_01.assert_is_open(1);
    }
}
