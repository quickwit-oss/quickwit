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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use bytesize::ByteSize;
use futures::StreamExt;
use mrecordlog::Record;
use quickwit_common::metrics::MEMORY_METRICS;
use quickwit_common::retry::RetryParams;
use quickwit_common::stream_utils::{InFlightValue, TrackedSender};
use quickwit_common::{ServiceStream, spawn_named_task};
use quickwit_proto::ingest::ingester::{
    FetchEof, FetchMessage, FetchPayload, IngesterService, OpenFetchStreamRequest, fetch_message,
};
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result, MRecordBatch};
use quickwit_proto::types::{IndexUid, NodeId, Position, QueueId, ShardId, SourceId, queue_id};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use super::models::ShardStatus;
use crate::mrecordlog_async::MultiRecordLogAsync;
use crate::{ClientId, IngesterPool, with_lock_metrics};

/// A fetch stream task is responsible for waiting and pushing new records written to a shard's
/// record log into a channel named `fetch_message_tx`.
pub(super) struct FetchStreamTask {
    /// Uniquely identifies the consumer of the fetch task for logging and debugging purposes.
    client_id: ClientId,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    queue_id: QueueId,
    /// The position of the next record fetched.
    from_position_inclusive: u64,
    mrecordlog: Arc<RwLock<Option<MultiRecordLogAsync>>>,
    fetch_message_tx: TrackedSender<IngestV2Result<FetchMessage>>,
    /// This channel notifies the fetch task when new records are available. This way the fetch
    /// task does not need to grab the lock and poll the mrecordlog queue unnecessarily.
    shard_status_rx: watch::Receiver<ShardStatus>,
    batch_num_bytes: usize,
}

impl fmt::Debug for FetchStreamTask {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FetchStreamTask")
            .field("client_id", &self.client_id)
            .field("index_uid", &self.index_uid)
            .field("source_id", &self.source_id)
            .field("shard_id", &self.shard_id)
            .finish()
    }
}

impl FetchStreamTask {
    pub fn spawn(
        open_fetch_stream_request: OpenFetchStreamRequest,
        mrecordlog: Arc<RwLock<Option<MultiRecordLogAsync>>>,
        shard_status_rx: watch::Receiver<ShardStatus>,
        batch_num_bytes: usize,
    ) -> (ServiceStream<IngestV2Result<FetchMessage>>, JoinHandle<()>) {
        let from_position_inclusive = open_fetch_stream_request
            .from_position_exclusive()
            .as_u64()
            .map(|offset| offset + 1)
            .unwrap_or_default();
        let (fetch_message_tx, fetch_stream) =
            ServiceStream::new_bounded_with_gauge(3, &MEMORY_METRICS.in_flight.fetch_stream);
        let mut fetch_task = Self {
            shard_id: open_fetch_stream_request.shard_id().clone(),
            queue_id: open_fetch_stream_request.queue_id(),
            index_uid: open_fetch_stream_request.index_uid().clone(),
            client_id: open_fetch_stream_request.client_id,
            source_id: open_fetch_stream_request.source_id,
            from_position_inclusive,
            mrecordlog,
            fetch_message_tx,
            shard_status_rx,
            batch_num_bytes,
        };
        let future = async move { fetch_task.run().await };
        let fetch_task_handle: JoinHandle<()> = spawn_named_task(future, "fetch_task");
        (fetch_stream, fetch_task_handle)
    }

    /// Runs the fetch task. It waits for new records in the log and pushes them into the fetch
    /// response channel until it reaches the end of the shard marked by an EOF record.
    async fn run(&mut self) {
        debug!(
            client_id=%self.client_id,
            index_uid=%self.index_uid,
            source_id=%self.source_id,
            shard_id=%self.shard_id,
            from_position_inclusive=%self.from_position_inclusive,
            "spawning fetch task"
        );
        let mut has_drained_queue = false;
        let mut to_position_inclusive = if self.from_position_inclusive == 0 {
            Position::Beginning
        } else {
            Position::offset(self.from_position_inclusive - 1)
        };

        loop {
            if has_drained_queue && self.shard_status_rx.changed().await.is_err() {
                // The shard was dropped.
                break;
            }
            has_drained_queue = true;

            let mut mrecord_buffer = BytesMut::with_capacity(self.batch_num_bytes);
            let mut mrecord_lengths = Vec::new();

            let mrecordlog_guard =
                with_lock_metrics!(self.mrecordlog.read().await, "fetch", "read");

            let Ok(mrecords) = mrecordlog_guard
                .as_ref()
                .expect("mrecordlog should be initialized")
                .range(&self.queue_id, self.from_position_inclusive..)
            else {
                // The queue was dropped.
                break;
            };
            for Record { payload, .. } in mrecords {
                // Accept at least one message
                if !mrecord_buffer.is_empty()
                    && (mrecord_buffer.len() + payload.len() > mrecord_buffer.capacity())
                {
                    has_drained_queue = false;
                    break;
                }
                mrecord_buffer.put(payload.borrow());
                mrecord_lengths.push(payload.len() as u32);
            }
            // Drop the lock while we send the message.
            drop(mrecordlog_guard);

            if !mrecord_lengths.is_empty() {
                let from_position_exclusive = if self.from_position_inclusive == 0 {
                    Position::Beginning
                } else {
                    Position::offset(self.from_position_inclusive - 1)
                };
                self.from_position_inclusive += mrecord_lengths.len() as u64;

                to_position_inclusive = Position::offset(self.from_position_inclusive - 1);

                let mrecord_batch = MRecordBatch {
                    mrecord_buffer: mrecord_buffer.freeze(),
                    mrecord_lengths,
                };
                let batch_size = mrecord_batch.estimate_size();
                let fetch_payload = FetchPayload {
                    index_uid: Some(self.index_uid.clone()),
                    source_id: self.source_id.clone(),
                    shard_id: Some(self.shard_id.clone()),
                    mrecord_batch: Some(mrecord_batch),
                    from_position_exclusive: Some(from_position_exclusive),
                    to_position_inclusive: Some(to_position_inclusive.clone()),
                };
                let fetch_message = FetchMessage::new_payload(fetch_payload);

                if self
                    .fetch_message_tx
                    .send(Ok(fetch_message), batch_size)
                    .await
                    .is_err()
                {
                    // The consumer was dropped.
                    return;
                }
            }
            if has_drained_queue {
                let has_reached_eof = {
                    let shard_status = self.shard_status_rx.borrow();
                    let shard_state = &shard_status.0;
                    let replication_position = &shard_status.1;
                    shard_state.is_closed() && to_position_inclusive >= *replication_position
                };
                if has_reached_eof {
                    debug!(
                        client_id=%self.client_id,
                        index_uid=%self.index_uid,
                        source_id=%self.source_id,
                        shard_id=%self.shard_id,
                        to_position_inclusive=%self.from_position_inclusive - 1,
                        "fetch stream reached end of shard"
                    );
                    let eof_position = to_position_inclusive.as_eof();

                    let fetch_eof = FetchEof {
                        index_uid: Some(self.index_uid.clone()),
                        source_id: self.source_id.clone(),
                        shard_id: Some(self.shard_id.clone()),
                        eof_position: Some(eof_position),
                    };
                    let fetch_message = FetchMessage::new_eof(fetch_eof);
                    let _ = self
                        .fetch_message_tx
                        .send(Ok(fetch_message), ByteSize(0))
                        .await;
                    return;
                }
            }
        }
        if !to_position_inclusive.is_eof() {
            // This can happen if we delete the associated source or index.
            warn!(
                client_id=%self.client_id,
                index_uid=%self.index_uid,
                source_id=%self.source_id,
                shard_id=%self.shard_id,
                "fetch stream ended before reaching end of shard"
            );
            let _ = self
                .fetch_message_tx
                .send(
                    Err(IngestV2Error::Internal(
                        "fetch stream ended before reaching end of shard".to_string(),
                    )),
                    ByteSize(0),
                )
                .await;
        }
    }
}

#[derive(Debug)]
pub struct FetchStreamError {
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub shard_id: ShardId,
    pub ingest_error: IngestV2Error,
}

/// Combines multiple fetch streams originating from different ingesters into a single stream. It
/// tolerates the failure of ingesters and automatically fails over to replica shards.
pub struct MultiFetchStream {
    self_node_id: NodeId,
    client_id: ClientId,
    ingester_pool: IngesterPool,
    retry_params: RetryParams,
    fetch_task_handles: HashMap<QueueId, JoinHandle<()>>,
    fetch_message_rx: mpsc::Receiver<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
}

impl MultiFetchStream {
    pub fn new(
        self_node_id: NodeId,
        client_id: ClientId,
        ingester_pool: IngesterPool,
        retry_params: RetryParams,
    ) -> Self {
        let (fetch_message_tx, fetch_message_rx) = mpsc::channel(3);
        Self {
            self_node_id,
            client_id,
            ingester_pool,
            retry_params,
            fetch_task_handles: HashMap::new(),
            fetch_message_rx,
            fetch_message_tx,
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn fetch_message_tx(
        &self,
    ) -> mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>> {
        self.fetch_message_tx.clone()
    }

    /// Subscribes to a shard and fails over to the replica if an error occurs.
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &mut self,
        leader_id: NodeId,
        follower_id_opt: Option<NodeId>,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
        from_position_exclusive: Position,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);
        let entry = self.fetch_task_handles.entry(queue_id.clone());

        if let Entry::Occupied(_) = entry {
            return Err(IngestV2Error::Internal(format!(
                "stream has already subscribed to shard `{queue_id}`"
            )));
        }
        let (preferred_ingester_id, failover_ingester_id_opt) =
            select_preferred_and_failover_ingesters(&self.self_node_id, leader_id, follower_id_opt);

        let mut ingester_ids = Vec::with_capacity(1 + failover_ingester_id_opt.is_some() as usize);
        ingester_ids.push(preferred_ingester_id);

        if let Some(failover_ingester_id) = failover_ingester_id_opt {
            ingester_ids.push(failover_ingester_id);
        }
        let fetch_stream_future = retrying_fetch_stream(
            self.client_id.clone(),
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            ingester_ids,
            self.ingester_pool.clone(),
            self.retry_params,
            self.fetch_message_tx.clone(),
        );
        let fetch_task_handle = spawn_named_task(fetch_stream_future, "fetch_stream");
        self.fetch_task_handles.insert(queue_id, fetch_task_handle);
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        index_uid: &IndexUid,
        source_id: &str,
        shard_id: ShardId,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(index_uid, source_id, &shard_id);

        if let Some(fetch_stream_handle) = self.fetch_task_handles.remove(&queue_id) {
            fetch_stream_handle.abort();
        }
        Ok(())
    }

    /// Returns the next fetch response. This method blocks until a response is available.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn next(&mut self) -> Result<FetchMessage, FetchStreamError> {
        // Because we always hold a sender and never call `close()` on the receiver, the channel is
        // always open.
        self.fetch_message_rx
            .recv()
            .await
            .expect("channel should be open")
            .map(|value: InFlightValue<FetchMessage>| value.into_inner())
    }

    /// Resets the stream by aborting all the active fetch tasks and dropping all queued responses.
    ///
    /// The borrow checker guarantees that both `next()` and `reset()` cannot be called
    /// simultaneously because they are both `&mut self` methods.
    pub fn reset(&mut self) {
        for (_queue_id, fetch_stream_handle) in self.fetch_task_handles.drain() {
            fetch_stream_handle.abort();
        }
        let (fetch_message_tx, fetch_message_rx) = mpsc::channel(3);
        self.fetch_message_tx = fetch_message_tx;
        self.fetch_message_rx = fetch_message_rx;
    }
}

impl Drop for MultiFetchStream {
    fn drop(&mut self) {
        self.reset();
    }
}

/// Chooses the ingester to stream records from, preferring "local" ingesters.
fn select_preferred_and_failover_ingesters(
    self_node_id: &NodeId,
    leader_id: NodeId,
    follower_id_opt: Option<NodeId>,
) -> (NodeId, Option<NodeId>) {
    // The replication factor is 1 and there is no follower.
    let Some(follower_id) = follower_id_opt else {
        return (leader_id, None);
    };
    if &leader_id == self_node_id {
        (leader_id, Some(follower_id))
    } else if &follower_id == self_node_id {
        (follower_id, Some(leader_id))
    } else if rand::random::<bool>() {
        (leader_id, Some(follower_id))
    } else {
        (follower_id, Some(leader_id))
    }
}

/// Performs multiple fault-tolerant fetch stream attempts until the stream reaches
/// the end of the shard.
#[allow(clippy::too_many_arguments)]
async fn retrying_fetch_stream(
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    mut from_position_exclusive: Position,
    ingester_ids: Vec<NodeId>,
    ingester_pool: IngesterPool,
    retry_params: RetryParams,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) {
    for num_attempts in 1..=retry_params.max_attempts {
        fault_tolerant_fetch_stream(
            client_id.clone(),
            index_uid.clone(),
            source_id.clone(),
            shard_id.clone(),
            &mut from_position_exclusive,
            &ingester_ids,
            ingester_pool.clone(),
            fetch_message_tx.clone(),
        )
        .await;

        if from_position_exclusive.is_eof() {
            break;
        }
        let delay = retry_params.compute_delay(num_attempts);
        tokio::time::sleep(delay).await;
    }
}

/// Streams records from the preferred ingester and fails over to the other ingester if an error
/// occurs.
#[allow(clippy::too_many_arguments)]
async fn fault_tolerant_fetch_stream(
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    from_position_exclusive: &mut Position,
    ingester_ids: &[NodeId],
    ingester_pool: IngesterPool,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) {
    // TODO: We can probably simplify this code by breaking it into smaller functions.
    'outer: for (ingester_idx, ingester_id) in ingester_ids.iter().enumerate() {
        let failover_ingester_id_opt = ingester_ids.get(ingester_idx + 1);

        let Some(ingester) = ingester_pool.get(ingester_id) else {
            if let Some(failover_ingester_id) = failover_ingester_id_opt {
                warn!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    "ingester `{ingester_id}` is unavailable: failing over to ingester `{failover_ingester_id}`"
                );
            } else {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    "ingester `{ingester_id}` is unavailable: closing fetch stream"
                );
                let message =
                    format!("ingester `{ingester_id}` is unavailable: closing fetch stream");
                let ingest_error = IngestV2Error::Unavailable(message);
                // Attempt to send the error to the consumer in a best-effort manner before
                // returning.
                let fetch_stream_error = FetchStreamError {
                    index_uid,
                    source_id,
                    shard_id,
                    ingest_error,
                };
                let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
                return;
            }
            continue;
        };
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(from_position_exclusive.clone()),
        };
        let mut fetch_stream = match ingester.open_fetch_stream(open_fetch_stream_request).await {
            Ok(fetch_stream) => fetch_stream,
            Err(not_found_error @ IngestV2Error::ShardNotFound { .. }) => {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    "failed to open fetch stream from ingester `{ingester_id}`: shard not found"
                );
                let fetch_stream_error = FetchStreamError {
                    index_uid,
                    source_id,
                    shard_id,
                    ingest_error: not_found_error,
                };
                let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
                from_position_exclusive.to_eof();
                return;
            }
            Err(other_ingest_error) => {
                if let Some(failover_ingester_id) = failover_ingester_id_opt {
                    warn!(
                        client_id=%client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        error=%other_ingest_error,
                        "failed to open fetch stream from ingester `{ingester_id}`: failing over to ingester `{failover_ingester_id}`"
                    );
                } else {
                    error!(
                        client_id=%client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        error=%other_ingest_error,
                        "failed to open fetch stream from ingester `{ingester_id}`: closing fetch stream"
                    );
                    let fetch_stream_error = FetchStreamError {
                        index_uid,
                        source_id,
                        shard_id,
                        ingest_error: other_ingest_error,
                    };
                    let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
                    return;
                }
                continue;
            }
        };
        while let Some(fetch_message_result) = fetch_stream.next().await {
            match fetch_message_result {
                Ok(fetch_message) => match &fetch_message.message {
                    Some(fetch_message::Message::Payload(fetch_payload)) => {
                        let batch_size = fetch_payload.estimate_size();
                        let to_position_inclusive = fetch_payload.to_position_inclusive();
                        let in_flight_value = InFlightValue::new(
                            fetch_message,
                            batch_size,
                            &MEMORY_METRICS.in_flight.multi_fetch_stream,
                        );
                        if fetch_message_tx.send(Ok(in_flight_value)).await.is_err() {
                            // The consumer was dropped.
                            return;
                        }
                        *from_position_exclusive = to_position_inclusive;
                    }
                    Some(fetch_message::Message::Eof(fetch_eof)) => {
                        let eof_position = fetch_eof.eof_position();
                        let in_flight_value = InFlightValue::new(
                            fetch_message,
                            ByteSize(0),
                            &MEMORY_METRICS.in_flight.multi_fetch_stream,
                        );
                        // We ignore the send error if the consumer was dropped because we're going
                        // to return anyway.
                        let _ = fetch_message_tx.send(Ok(in_flight_value)).await;

                        *from_position_exclusive = eof_position;
                        return;
                    }
                    None => {
                        warn!("received empty fetch message");
                        continue;
                    }
                },
                Err(ingest_error) => {
                    if let Some(failover_ingester_id) = failover_ingester_id_opt {
                        warn!(
                            client_id=%client_id,
                            index_uid=%index_uid,
                            source_id=%source_id,
                            shard_id=%shard_id,
                            error=%ingest_error,
                            "failed to fetch records from ingester `{ingester_id}`: failing over to ingester `{failover_ingester_id}`"
                        );
                    } else {
                        error!(
                            client_id=%client_id,
                            index_uid=%index_uid,
                            source_id=%source_id,
                            shard_id=%shard_id,
                            error=%ingest_error,
                            "failed to fetch records from ingester `{ingester_id}`: closing fetch stream"
                        );
                        let fetch_stream_error = FetchStreamError {
                            index_uid,
                            source_id,
                            shard_id,
                            ingest_error,
                        };
                        let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
                        return;
                    }
                    continue 'outer;
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::ingest::ingester::{IngesterServiceClient, MockIngesterService};
    use quickwit_proto::types::queue_id;
    use tokio::time::timeout;

    use super::*;
    use crate::MRecord;

    pub fn into_fetch_payload(fetch_message: FetchMessage) -> FetchPayload {
        match fetch_message.message.unwrap() {
            fetch_message::Message::Payload(fetch_payload) => fetch_payload,
            other => panic!("expected fetch payload, got `{other:?}`"),
        }
    }

    pub fn into_fetch_eof(fetch_message: FetchMessage) -> FetchEof {
        match fetch_message.message.unwrap() {
            fetch_message::Message::Eof(fetch_eof) => fetch_eof,
            other => panic!("expected fetch EOF, got `{other:?}`"),
        }
    }

    #[tokio::test]
    async fn test_fetch_task_happy_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();
        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-foo").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(fetch_payload.index_uid(), &index_uid);
        assert_eq!(fetch_payload.source_id, source_id);
        assert_eq!(fetch_payload.shard_id(), shard_id);
        assert_eq!(fetch_payload.from_position_exclusive(), Position::Beginning);
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-foo"
        );

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        // Trigger a spurious notification.
        let shard_status = (ShardState::Open, Position::offset(0u64));
        shard_status_tx.send(shard_status).unwrap();

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-bar").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(1u64));
        shard_status_tx.send(shard_status.clone()).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-bar"
        );

        let mut mrecordlog_guard = mrecordlog.write().await;

        let mrecords = [
            MRecord::new_doc("test-doc-baz").encode(),
            MRecord::new_doc("test-doc-qux").encode(),
        ]
        .into_iter();

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(&queue_id, None, mrecords)
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(3u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(3u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14, 14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-baz\0\0test-doc-qux"
        );

        let shard_status = (ShardState::Closed, Position::offset(3u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.index_uid(), &index_uid);
        assert_eq!(fetch_eof.source_id, source_id);
        assert_eq!(fetch_eof.shard_id(), shard_id);
        assert_eq!(fetch_eof.eof_position, Some(Position::eof(3u64)));

        fetch_task_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_task_signals_eof() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-foo").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::offset(0u64)),
        };
        let shard_status = (ShardState::Closed, Position::offset(0u64));
        let (_shard_status_tx, shard_status_rx) = watch::channel(shard_status);

        let (mut fetch_stream, fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.index_uid(), &index_uid);
        assert_eq!(fetch_eof.source_id, source_id);
        assert_eq!(fetch_eof.shard_id(), shard_id);
        assert_eq!(fetch_eof.eof_position, Some(Position::eof(0u64).as_eof()));

        fetch_task_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_task_signals_eof_at_beginning() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Closed, Position::Beginning);
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.index_uid(), &index_uid);
        assert_eq!(fetch_eof.source_id, source_id);
        assert_eq!(fetch_eof.shard_id(), shard_id);
        assert_eq!(fetch_eof.eof_position, Some(Position::Beginning.as_eof()));

        fetch_task_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_task_from_position_exclusive() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::offset(0u64)),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();
        drop(mrecordlog_guard);

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-foo").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(0u64));
        shard_status_tx.send(shard_status).unwrap();

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                std::iter::once(MRecord::new_doc("test-doc-bar").encode()),
            )
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(1u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(fetch_payload.index_uid(), &index_uid);
        assert_eq!(fetch_payload.source_id, source_id);
        assert_eq!(fetch_payload.shard_id(), shard_id);
        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-bar"
        );
    }

    #[tokio::test]
    async fn test_fetch_task_error() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (_shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let ingest_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(ingest_error, IngestV2Error::Internal(_)));

        fetch_task_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_fetch_task_batch_num_bytes() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            30,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();

        let records = [
            Bytes::from_static(b"test-doc-foo"),
            Bytes::from_static(b"test-doc-bar"),
            Bytes::from_static(b"test-doc-baz"),
        ]
        .into_iter();

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(&queue_id, None, records)
            .await
            .unwrap();
        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(2u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [12, 12]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "test-doc-footest-doc-bar"
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [12]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "test-doc-baz"
        );
    }

    #[tokio::test]
    async fn test_fetch_task_batch_num_bytes_less_than_record_payload() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(Some(
            MultiRecordLogAsync::open(tempdir.path()).await.unwrap(),
        )));
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let shard_id = ShardId::from(1);
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);

        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            from_position_exclusive: Some(Position::Beginning),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, _fetch_task_handle) = FetchStreamTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            10, //< we request batch larger than 10 bytes.
        );

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .create_queue(&queue_id)
            .await
            .unwrap();

        mrecordlog_guard
            .as_mut()
            .unwrap()
            .append_records(
                &queue_id,
                None,
                // This doc is longer than 10 bytes.
                std::iter::once(MRecord::new_doc("test-doc-foo").encode()),
            )
            .await
            .unwrap();

        drop(mrecordlog_guard);

        let shard_status = (ShardState::Open, Position::offset(1u64));
        shard_status_tx.send(shard_status).unwrap();

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_payload.mrecord_batch.as_ref().unwrap().mrecord_buffer,
            "\0\0test-doc-foo"
        );
    }

    #[test]
    fn test_select_preferred_and_failover_ingesters() {
        let self_node_id: NodeId = "test-ingester-0".into();

        let (preferred, failover) =
            select_preferred_and_failover_ingesters(&self_node_id, "test-ingester-0".into(), None);
        assert_eq!(preferred, "test-ingester-0");
        assert!(failover.is_none());

        let (preferred, failover) = select_preferred_and_failover_ingesters(
            &self_node_id,
            "test-ingester-0".into(),
            Some("test-ingester-1".into()),
        );
        assert_eq!(preferred, "test-ingester-0");
        assert_eq!(failover.unwrap(), "test-ingester-1");

        let (preferred, failover) = select_preferred_and_failover_ingesters(
            &self_node_id,
            "test-ingester-1".into(),
            Some("test-ingester-0".into()),
        );
        assert_eq!(preferred, "test-ingester-0");
        assert_eq!(failover.unwrap(), "test-ingester-1");
    }

    #[tokio::test]
    async fn test_fault_tolerant_fetch_stream_ingester_unavailable_failover() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let mut from_position_exclusive = Position::offset(0u64);

        let ingester_ids: Vec<NodeId> = vec!["test-ingester-0".into(), "test-ingester-1".into()];
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);
        let (service_stream_tx_1, service_stream_1) = ServiceStream::new_unbounded();

        let mut mock_ingester_1 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_1
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Ok(service_stream_1)
            });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);

        ingester_pool.insert("test-ingester-1".into(), ingester_1);

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-foo"]),
            from_position_exclusive: Some(Position::offset(0u64)),
            to_position_inclusive: Some(Position::offset(1u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        let fetch_eof = FetchEof {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            eof_position: Some(Position::eof(1u64)),
        };
        let fetch_message = FetchMessage::new_eof(fetch_eof);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        fault_tolerant_fetch_stream(
            client_id,
            index_uid,
            source_id,
            shard_id,
            &mut from_position_exclusive,
            &ingester_ids,
            ingester_pool,
            fetch_message_tx,
        )
        .await;

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.eof_position(), Position::eof(1u64));

        assert!(
            timeout(Duration::from_millis(100), fetch_stream.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_fault_tolerant_fetch_stream_open_fetch_stream_error_failover() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let mut from_position_exclusive = Position::offset(0u64);

        let ingester_ids: Vec<NodeId> = vec!["test-ingester-0".into(), "test-ingester-1".into()];
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);
        let (service_stream_tx_1, service_stream_1) = ServiceStream::new_unbounded();

        let mut mock_ingester_0 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Err(IngestV2Error::Internal(
                    "open fetch stream error".to_string(),
                ))
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);

        let mut mock_ingester_1 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_1
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Ok(service_stream_1)
            });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);

        ingester_pool.insert("test-ingester-0".into(), ingester_0);
        ingester_pool.insert("test-ingester-1".into(), ingester_1);

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-foo"]),
            from_position_exclusive: Some(Position::offset(0u64)),
            to_position_inclusive: Some(Position::offset(1u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        let fetch_eof = FetchEof {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            eof_position: Some(Position::eof(1u64)),
        };
        let fetch_message = FetchMessage::new_eof(fetch_eof);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        fault_tolerant_fetch_stream(
            client_id,
            index_uid,
            source_id,
            shard_id,
            &mut from_position_exclusive,
            &ingester_ids,
            ingester_pool,
            fetch_message_tx,
        )
        .await;

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.eof_position(), Position::eof(1u64));

        assert!(
            timeout(Duration::from_millis(100), fetch_stream.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_fault_tolerant_fetch_stream_error_failover() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let mut from_position_exclusive = Position::offset(0u64);

        let ingester_ids: Vec<NodeId> = vec!["test-ingester-0".into(), "test-ingester-1".into()];
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);
        let (service_stream_tx_0, service_stream_0) = ServiceStream::new_unbounded();
        let (service_stream_tx_1, service_stream_1) = ServiceStream::new_unbounded();

        let mut mock_ingester_0 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Ok(service_stream_0)
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);

        let mut mock_ingester_1 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_1
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(1u64));

                Ok(service_stream_1)
            });
        let ingester_1 = IngesterServiceClient::from_mock(mock_ingester_1);

        ingester_pool.insert("test-ingester-0".into(), ingester_0);
        ingester_pool.insert("test-ingester-1".into(), ingester_1);

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-foo"]),
            from_position_exclusive: Some(Position::offset(0u64)),
            to_position_inclusive: Some(Position::offset(1u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_0.send(Ok(fetch_message)).unwrap();

        let ingest_error = IngestV2Error::Internal("fetch stream error".into());
        service_stream_tx_0.send(Err(ingest_error)).unwrap();

        let fetch_eof = FetchEof {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            eof_position: Some(Position::eof(1u64)),
        };
        let fetch_message = FetchMessage::new_eof(fetch_eof);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        fault_tolerant_fetch_stream(
            client_id,
            index_uid,
            source_id,
            shard_id,
            &mut from_position_exclusive,
            &ingester_ids,
            ingester_pool,
            fetch_message_tx,
        )
        .await;

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_eof = into_fetch_eof(fetch_message);

        assert_eq!(fetch_eof.eof_position(), Position::eof(1u64));

        assert!(
            timeout(Duration::from_millis(100), fetch_stream.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_fault_tolerant_fetch_stream_shard_not_found() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let mut from_position_exclusive = Position::offset(0u64);

        let ingester_ids: Vec<NodeId> = vec!["test-ingester-0".into(), "test-ingester-1".into()];
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);

        let mut mock_ingester_0 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Err(IngestV2Error::ShardNotFound {
                    shard_id: ShardId::from(1),
                })
            });
        let ingester_0 = IngesterServiceClient::from_mock(mock_ingester_0);
        ingester_pool.insert("test-ingester-0".into(), ingester_0);

        fault_tolerant_fetch_stream(
            client_id,
            index_uid,
            source_id,
            shard_id,
            &mut from_position_exclusive,
            &ingester_ids,
            ingester_pool,
            fetch_message_tx,
        )
        .await;

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            fetch_stream_error.ingest_error,
            IngestV2Error::ShardNotFound { shard_id } if shard_id == ShardId::from(1)
        ));
        assert!(from_position_exclusive.is_eof());
    }

    #[tokio::test]
    async fn test_retrying_fetch_stream() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let from_position_exclusive = Position::offset(0u64);

        let ingester_ids: Vec<NodeId> = vec!["test-ingester".into()];
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);
        let (service_stream_tx_1, service_stream_1) = ServiceStream::new_unbounded();
        let (service_stream_tx_2, service_stream_2) = ServiceStream::new_unbounded();

        let mut retry_params = RetryParams::for_test();
        retry_params.max_attempts = 3;

        let mut mock_ingester = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .returning(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Err(IngestV2Error::Internal(
                    "open fetch stream error".to_string(),
                ))
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Ok(service_stream_1)
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(1u64));

                Ok(service_stream_2)
            });
        let ingester = IngesterServiceClient::from_mock(mock_ingester);

        ingester_pool.insert("test-ingester".into(), ingester);

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-foo"]),
            from_position_exclusive: Some(Position::offset(0u64)),
            to_position_inclusive: Some(Position::offset(1u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        let ingest_error = IngestV2Error::Internal("fetch stream error #1".into());
        service_stream_tx_1.send(Err(ingest_error)).unwrap();

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-bar"]),
            from_position_exclusive: Some(Position::offset(1u64)),
            to_position_inclusive: Some(Position::offset(2u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_2.send(Ok(fetch_message)).unwrap();

        let ingest_error = IngestV2Error::Internal("fetch stream error #2".into());
        service_stream_tx_2.send(Err(ingest_error)).unwrap();

        retrying_fetch_stream(
            client_id,
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            ingester_ids,
            ingester_pool,
            retry_params,
            fetch_message_tx,
        )
        .await;

        let ingest_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .ingest_error;
        assert!(
            matches!(ingest_error, IngestV2Error::Internal(message) if message == "open fetch stream error")
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(fetch_stream_error.ingest_error, IngestV2Error::Internal(message) if message == "fetch stream error #1")
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(2u64)
        );

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(fetch_stream_error.ingest_error, IngestV2Error::Internal(message) if message == "fetch stream error #2")
        );

        assert!(
            timeout(Duration::from_millis(100), fetch_stream.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_multi_fetch_stream() {
        let self_node_id: NodeId = "test-node".into();
        let client_id = "test-client".to_string();
        let ingester_pool = IngesterPool::default();
        let retry_params = RetryParams::for_test();
        let _multi_fetch_stream =
            MultiFetchStream::new(self_node_id, client_id, ingester_pool, retry_params);
        // TODO: Backport from original branch.
    }
}
