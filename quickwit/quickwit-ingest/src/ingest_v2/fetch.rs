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

use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use futures::StreamExt;
use quickwit_common::ServiceStream;
use quickwit_proto::ingest::ingester::{FetchResponseV2, IngesterService, OpenFetchStreamRequest};
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result, MRecordBatch};
use quickwit_proto::types::{queue_id, IndexUid, NodeId, Position, QueueId, ShardId, SourceId};
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use super::ingester::IngesterState;
use crate::ingest_v2::mrecord::is_eof_mrecord;
use crate::{ClientId, IngesterPool};

/// A fetch task is responsible for waiting and pushing new records written to a shard's record log
/// into a channel named `fetch_response_tx`.
pub(super) struct FetchTask {
    /// Uniquely identifies the consumer of the fetch task for logging and debugging purposes.
    client_id: ClientId,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    queue_id: QueueId,
    /// Range of records to fetch.
    fetch_range: FetchRange,
    state: Arc<RwLock<IngesterState>>,
    fetch_response_tx: mpsc::Sender<IngestV2Result<FetchResponseV2>>,
    /// This channel notifies the fetch task when new records are available. This way the fetch
    /// task does not need to grab the lock and poll the mrecordlog queue unnecessarily.
    new_records_rx: watch::Receiver<()>,
    batch_num_bytes: usize,
}

impl fmt::Debug for FetchTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetchTask")
            .field("client_id", &self.client_id)
            .field("index_uid", &self.index_uid)
            .field("source_id", &self.source_id)
            .field("shard_id", &self.shard_id)
            .finish()
    }
}

type FetchTaskHandle = JoinHandle<(u64, Position)>;

impl FetchTask {
    pub const DEFAULT_BATCH_NUM_BYTES: usize = 1024 * 1024; // 1 MiB

    pub fn spawn(
        open_fetch_stream_request: OpenFetchStreamRequest,
        state: Arc<RwLock<IngesterState>>,
        new_records_rx: watch::Receiver<()>,
        batch_num_bytes: usize,
    ) -> (
        ServiceStream<IngestV2Result<FetchResponseV2>>,
        FetchTaskHandle,
    ) {
        let (fetch_response_tx, fetch_stream) = ServiceStream::new_bounded(3);
        let fetch_range = FetchRange::new(
            open_fetch_stream_request.from_position_exclusive(),
            open_fetch_stream_request.to_position_inclusive(),
        );
        let mut fetch_task = Self {
            queue_id: open_fetch_stream_request.queue_id(),
            client_id: open_fetch_stream_request.client_id,
            index_uid: open_fetch_stream_request.index_uid.into(),
            source_id: open_fetch_stream_request.source_id,
            shard_id: open_fetch_stream_request.shard_id,
            fetch_range,
            state,
            fetch_response_tx,
            new_records_rx,
            batch_num_bytes,
        };
        let future = async move { fetch_task.run().await };
        let fetch_task_handle: FetchTaskHandle = tokio::spawn(future);
        (fetch_stream, fetch_task_handle)
    }

    /// Runs the fetch task. It waits for new records in the log and pushes them into the fetch
    /// response channel until `to_position_inclusive` is reached, the shard is closed and
    /// `to_position_inclusive` is reached, or the ingester is dropped. It returns the total number
    /// of records fetched and the position of the last record fetched.
    async fn run(&mut self) -> (u64, Position) {
        debug!(
            client_id=%self.client_id,
            index_uid=%self.index_uid,
            source_id=%self.source_id,
            shard_id=%self.shard_id,
            fetch_range=?self.fetch_range,
            "spawning fetch task"
        );
        let mut has_drained_queue = false;
        let mut has_reached_eof = false;
        let mut num_records_total = 0;

        while !has_reached_eof && !self.fetch_range.is_empty() {
            if has_drained_queue && self.new_records_rx.changed().await.is_err() {
                // The shard was dropped.
                break;
            }
            has_drained_queue = true;
            let mut mrecord_buffer = BytesMut::with_capacity(self.batch_num_bytes);
            let mut mrecord_lengths = Vec::new();

            let state_guard = self.state.read().await;

            let Ok(mrecords) = state_guard
                .mrecordlog
                .range(&self.queue_id, self.fetch_range)
            else {
                // The queue was dropped.
                break;
            };
            for (_position, mrecord) in mrecords {
                if mrecord_buffer.len() + mrecord.len() > mrecord_buffer.capacity() {
                    has_drained_queue = false;
                    break;
                }
                mrecord_buffer.put(mrecord.borrow());
                mrecord_lengths.push(mrecord.len() as u32);
            }
            // Drop the lock while we send the message.
            drop(state_guard);

            if mrecord_buffer.is_empty() {
                continue;
            }
            let last_mrecord_len = *mrecord_lengths
                .last()
                .expect("`mrecord_lengths` should not be empty")
                as usize;
            let last_mrecord = &mrecord_buffer[mrecord_buffer.len() - last_mrecord_len..];

            has_reached_eof = is_eof_mrecord(last_mrecord);

            let mrecord_batch = MRecordBatch {
                mrecord_buffer: mrecord_buffer.freeze(),
                mrecord_lengths,
            };
            let num_records = mrecord_batch.num_mrecords() as u64;
            num_records_total += num_records;

            let from_position_exclusive = self.fetch_range.from_position_exclusive();
            self.fetch_range.advance_by(num_records);

            let to_position_inclusive = if has_reached_eof {
                Position::Eof
            } else {
                self.fetch_range.from_position_exclusive()
            };
            let fetch_response = FetchResponseV2 {
                index_uid: self.index_uid.clone().into(),
                source_id: self.source_id.clone(),
                shard_id: self.shard_id,
                mrecord_batch: Some(mrecord_batch),
                from_position_exclusive: Some(from_position_exclusive),
                to_position_inclusive: Some(to_position_inclusive),
            };
            if self
                .fetch_response_tx
                .send(Ok(fetch_response))
                .await
                .is_err()
            {
                // The consumer was dropped.
                break;
            }
        }
        if !has_reached_eof || !self.fetch_range.is_empty() {
            error!(
                client_id=%self.client_id,
                index_uid=%self.index_uid,
                source_id=%self.source_id,
                shard_id=%self.shard_id,
                "fetch stream ended unexpectedly"
            );
            let _ = self
                .fetch_response_tx
                .send(Err(IngestV2Error::Internal(
                    "fetch stream ended unexpectedly".to_string(),
                )))
                .await;
        }
        (
            num_records_total,
            self.fetch_range.from_position_exclusive(),
        )
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
    fetch_task_handles: HashMap<QueueId, JoinHandle<()>>,
    fetch_response_rx: mpsc::Receiver<Result<FetchResponseV2, FetchStreamError>>,
    fetch_response_tx: mpsc::Sender<Result<FetchResponseV2, FetchStreamError>>,
}

impl MultiFetchStream {
    pub fn new(self_node_id: NodeId, client_id: ClientId, ingester_pool: IngesterPool) -> Self {
        let (fetch_response_tx, fetch_response_rx) = mpsc::channel(3);
        Self {
            self_node_id,
            client_id,
            ingester_pool,
            fetch_task_handles: HashMap::new(),
            fetch_response_rx,
            fetch_response_tx,
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn fetch_response_tx(&self) -> mpsc::Sender<Result<FetchResponseV2, FetchStreamError>> {
        self.fetch_response_tx.clone()
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
        to_position_inclusive: Position,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(index_uid.as_str(), &source_id, shard_id);
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
        let fetch_task_future = fault_tolerant_fetch_task(
            self.client_id.clone(),
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            to_position_inclusive,
            ingester_ids,
            self.ingester_pool.clone(),
            self.fetch_response_tx.clone(),
        );
        let fetch_task_handle = tokio::spawn(fetch_task_future);
        self.fetch_task_handles.insert(queue_id, fetch_task_handle);
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        index_uid: &str,
        source_id: &str,
        shard_id: u64,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(index_uid, source_id, shard_id);

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
    pub async fn next(&mut self) -> Result<FetchResponseV2, FetchStreamError> {
        // Because we always hold a sender and never call `close()` on the receiver, the channel is
        // always open.
        self.fetch_response_rx
            .recv()
            .await
            .expect("the channel should be open")
    }

    /// Resets the stream by aborting all the active fetch tasks and dropping all queued responses.
    ///
    /// The borrow checker guarantees that both `next()` and `reset()` cannot be called
    /// simultaneously because they are both `&mut self` methods.
    pub fn reset(&mut self) {
        for (_queue_id, fetch_stream_handle) in self.fetch_task_handles.drain() {
            fetch_stream_handle.abort();
        }
        let (fetch_response_tx, fetch_response_rx) = mpsc::channel(3);
        self.fetch_response_tx = fetch_response_tx;
        self.fetch_response_rx = fetch_response_rx;
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

/// Streams records from the preferred ingester and fails over to the other ingester if an error
/// occurs.
#[allow(clippy::too_many_arguments)]
async fn fault_tolerant_fetch_task(
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    mut from_position_exclusive: Position,
    to_position_inclusive: Position,
    ingester_ids: Vec<NodeId>,
    ingester_pool: IngesterPool,
    fetch_response_tx: mpsc::Sender<Result<FetchResponseV2, FetchStreamError>>,
) {
    // TODO: We can probably simplify this code by breaking it into smaller functions.
    'outer: for (ingester_idx, ingester_id) in ingester_ids.iter().enumerate() {
        let failover_ingester_id_opt = ingester_ids.get(ingester_idx + 1);

        let mut ingester = match ingester_pool.get(ingester_id) {
            Some(ingester) => ingester,
            _ => {
                if let Some(failover_ingester_id) = failover_ingester_id_opt {
                    warn!(
                        client_id=%client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        "ingester `{ingester_id}` is not available: failing over to ingester `{failover_ingester_id}`"
                    );
                } else {
                    error!(
                        client_id=%client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        "ingester `{ingester_id}` is not available: closing fetch stream"
                    );
                    let ingest_error = IngestV2Error::IngesterUnavailable {
                        ingester_id: ingester_id.clone(),
                    };
                    // Attempt to send the error to the consumer in a best-effort manner before
                    // returning.
                    let fetch_stream_error = FetchStreamError {
                        index_uid,
                        source_id,
                        shard_id,
                        ingest_error,
                    };
                    let _ = fetch_response_tx.send(Err(fetch_stream_error)).await;
                    return;
                }
                continue;
            }
        };
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id,
            from_position_exclusive: Some(from_position_exclusive.clone()),
            to_position_inclusive: Some(to_position_inclusive.clone()),
        };
        let mut fetch_stream = match ingester.open_fetch_stream(open_fetch_stream_request).await {
            Ok(fetch_stream) => fetch_stream,
            Err(ingest_error) => {
                if let Some(failover_ingester_id) = failover_ingester_id_opt {
                    warn!(
                        client_id=%client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        error=%ingest_error,
                        "failed to open fetch stream from ingester `{ingester_id}`: failing over to ingester `{failover_ingester_id}`"
                    );
                } else {
                    error!(
                        client_id=%client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        error=%ingest_error,
                        "failed to open fetch stream from ingester `{ingester_id}`: closing fetch stream"
                    );
                    let fetch_stream_error = FetchStreamError {
                        index_uid,
                        source_id,
                        shard_id,
                        ingest_error,
                    };
                    let _ = fetch_response_tx.send(Err(fetch_stream_error)).await;
                    return;
                }
                continue;
            }
        };
        while let Some(fetch_response_result) = fetch_stream.next().await {
            match fetch_response_result {
                Ok(fetch_response) => {
                    let to_position_inclusive = fetch_response.to_position_inclusive();

                    if fetch_response_tx.send(Ok(fetch_response)).await.is_err() {
                        // The stream was dropped.
                        return;
                    }
                    if to_position_inclusive == Position::Eof {
                        // The stream has reached the end of the shard.
                        return;
                    }
                    from_position_exclusive = to_position_inclusive;
                }
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
                        let _ = fetch_response_tx.send(Err(fetch_stream_error)).await;
                        return;
                    }
                    continue 'outer;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FetchRange {
    from_position_exclusive_opt: Option<u64>,
    to_position_inclusive_opt: Option<u64>,
}

impl FetchRange {
    pub(super) fn new(from_position_exclusive: Position, to_position_inclusive: Position) -> Self {
        Self {
            from_position_exclusive_opt: from_position_exclusive.as_u64(),
            to_position_inclusive_opt: to_position_inclusive.as_u64(),
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_position_exclusive(&self) -> Position {
        Position::from(self.from_position_exclusive_opt)
    }

    fn is_empty(&self) -> bool {
        match (
            self.from_position_exclusive_opt,
            self.to_position_inclusive_opt,
        ) {
            (Some(from_position_exclusive), Some(to_position_inclusive)) => {
                from_position_exclusive >= to_position_inclusive
            }
            _ => false,
        }
    }

    fn advance_by(&mut self, num_records: u64) {
        if let Some(from_position_exclusive) = self.from_position_exclusive_opt {
            self.from_position_exclusive_opt = Some(from_position_exclusive + num_records);
        } else {
            self.from_position_exclusive_opt = Some(num_records - 1);
        }
    }
}

impl RangeBounds<u64> for FetchRange {
    fn start_bound(&self) -> std::ops::Bound<&u64> {
        self.from_position_exclusive_opt
            .as_ref()
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded)
    }

    fn end_bound(&self) -> std::ops::Bound<&u64> {
        self.to_position_inclusive_opt
            .as_ref()
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use mrecordlog::MultiRecordLog;
    use quickwit_proto::ingest::ingester::IngesterServiceClient;
    use quickwit_proto::types::queue_id;
    use tokio::time::timeout;

    use super::*;
    use crate::MRecord;

    #[test]
    fn test_fetch_range() {
        let mut fetch_range = FetchRange::new(Position::Beginning, Position::Eof);
        assert_eq!(fetch_range.start_bound(), Bound::Unbounded);
        assert_eq!(fetch_range.end_bound(), Bound::Unbounded);
        assert_eq!(fetch_range.from_position_exclusive(), Position::Beginning);
        assert!(!fetch_range.is_empty());

        fetch_range.advance_by(1);
        assert_eq!(fetch_range.start_bound(), Bound::Excluded(&0));
        assert_eq!(fetch_range.from_position_exclusive(), 0u64);
        assert!(!fetch_range.is_empty());

        fetch_range.advance_by(10);
        assert_eq!(fetch_range.from_position_exclusive(), Position::from(10u64));

        let mut fetch_range = FetchRange::new(Position::Beginning, Position::from(1u64));
        assert!(!fetch_range.is_empty());

        fetch_range.advance_by(1);
        assert_eq!(fetch_range.from_position_exclusive(), 0u64);
        assert!(!fetch_range.is_empty());

        fetch_range.advance_by(1);
        assert_eq!(fetch_range.from_position_exclusive(), 1u64);
        assert!(fetch_range.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_task() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let client_id = "test-client".to_string();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 1,
            from_position_exclusive: None,
            to_position_inclusive: None,
        };
        let (new_records_tx, new_records_rx) = watch::channel(());
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
        }));
        let (mut fetch_stream, fetch_task_handle) = FetchTask::spawn(
            open_fetch_stream_request,
            state.clone(),
            new_records_rx,
            1024,
        );
        let queue_id = queue_id(&index_uid, &source_id, 1);

        let mut state_guard = state.write().await;

        state_guard
            .mrecordlog
            .create_queue(&queue_id)
            .await
            .unwrap();
        state_guard
            .mrecordlog
            .append_record(&queue_id, None, MRecord::new_doc("test-doc-foo").encode())
            .await
            .unwrap();
        drop(state_guard);

        let fetch_response = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetch_response.index_uid, "test-index:0");
        assert_eq!(fetch_response.source_id, "test-source");
        assert_eq!(fetch_response.shard_id, 1);
        assert_eq!(
            fetch_response.from_position_exclusive(),
            Position::Beginning
        );
        assert_eq!(fetch_response.to_position_inclusive(), 0u64);
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_buffer,
            "\0\0test-doc-foo"
        );

        let mut state_guard = state.write().await;

        state_guard
            .mrecordlog
            .append_record(&queue_id, None, MRecord::new_doc("test-doc-bar").encode())
            .await
            .unwrap();
        drop(state_guard);

        timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap_err();

        new_records_tx.send(()).unwrap();

        // Trigger a spurious notification.
        new_records_tx.send(()).unwrap();

        let fetch_response = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fetch_response.from_position_exclusive(),
            Position::from(0u64)
        );
        assert_eq!(fetch_response.to_position_inclusive(), 1u64);
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14]
        );
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_buffer,
            "\0\0test-doc-bar"
        );

        let mut state_guard = state.write().await;

        let mrecords = [
            MRecord::new_doc("test-doc-baz").encode(),
            MRecord::new_doc("test-doc-qux").encode(),
            MRecord::Eof.encode(),
        ]
        .into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id, None, mrecords)
            .await
            .unwrap();
        drop(state_guard);

        new_records_tx.send(()).unwrap();

        let fetch_response = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fetch_response.from_position_exclusive(),
            Position::from(1u64)
        );
        assert_eq!(fetch_response.to_position_inclusive(), Position::Eof);
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [14, 14, 2]
        );
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_buffer,
            "\0\0test-doc-baz\0\0test-doc-qux\0\x02"
        );

        let (num_records, last_position) = fetch_task_handle.await.unwrap();
        assert_eq!(num_records, 5);
        assert_eq!(last_position, Position::from(4u64));
    }

    #[tokio::test]
    async fn test_fetch_task_error() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let client_id = "test-client".to_string();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 1,
            from_position_exclusive: None,
            to_position_inclusive: None,
        };
        let (_new_records_tx, new_records_rx) = watch::channel(());
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
        }));
        let (mut fetch_stream, fetch_task_handle) = FetchTask::spawn(
            open_fetch_stream_request,
            state.clone(),
            new_records_rx,
            1024,
        );
        let ingest_error = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(matches!(ingest_error, IngestV2Error::Internal(_)));

        let (num_records, last_position) = fetch_task_handle.await.unwrap();
        assert_eq!(num_records, 0);
        assert_eq!(last_position, Position::Beginning);
    }

    #[tokio::test]
    async fn test_fetch_task_up_to_position() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let client_id = "test-client".to_string();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 1,
            from_position_exclusive: None,
            to_position_inclusive: Some(Position::from(0u64)),
        };
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
        }));
        let (new_records_tx, new_records_rx) = watch::channel(());
        let (mut fetch_stream, fetch_task_handle) = FetchTask::spawn(
            open_fetch_stream_request,
            state.clone(),
            new_records_rx,
            1024,
        );
        let queue_id = queue_id(&index_uid, &source_id, 1);

        let mut state_guard = state.write().await;

        state_guard
            .mrecordlog
            .create_queue(&queue_id)
            .await
            .unwrap();
        state_guard
            .mrecordlog
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-foo"))
            .await
            .unwrap();
        drop(state_guard);

        new_records_tx.send(()).unwrap();

        let fetch_response = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths
                .len(),
            1
        );

        let (num_records, last_position) = fetch_task_handle.await.unwrap();
        assert_eq!(num_records, 1);
        assert_eq!(last_position, 0u64);
    }

    #[tokio::test]
    async fn test_fetch_task_batch_num_bytes() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();
        let client_id = "test-client".to_string();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 1,
            from_position_exclusive: None,
            to_position_inclusive: Some(Position::from(2u64)),
        };
        let state = Arc::new(RwLock::new(IngesterState {
            mrecordlog,
            shards: HashMap::new(),
            replication_streams: HashMap::new(),
            replication_tasks: HashMap::new(),
        }));
        let (new_records_tx, new_records_rx) = watch::channel(());
        let (mut fetch_stream, _fetch_task_handle) =
            FetchTask::spawn(open_fetch_stream_request, state.clone(), new_records_rx, 30);
        let queue_id = queue_id(&index_uid, &source_id, 1);

        let mut state_guard = state.write().await;

        state_guard
            .mrecordlog
            .create_queue(&queue_id)
            .await
            .unwrap();

        let records = [
            Bytes::from_static(b"test-doc-foo"),
            Bytes::from_static(b"test-doc-bar"),
            Bytes::from_static(b"test-doc-baz"),
        ]
        .into_iter();

        state_guard
            .mrecordlog
            .append_records(&queue_id, None, records)
            .await
            .unwrap();
        drop(state_guard);

        new_records_tx.send(()).unwrap();

        let fetch_response = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [12, 12]
        );
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_buffer,
            "test-doc-footest-doc-bar"
        );

        let fetch_response = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_lengths,
            [12]
        );
        assert_eq!(
            fetch_response
                .mrecord_batch
                .as_ref()
                .unwrap()
                .mrecord_buffer,
            "test-doc-baz"
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
    async fn test_fault_tolerant_fetch_task_happy_failover() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = "test-index:0".into();
        let source_id: SourceId = "test-source".into();
        let shard_id: ShardId = 1;
        let from_position_exclusive = Position::from(0u64);
        let to_position_inclusive = Position::Eof;
        let ingester_ids: Vec<NodeId> = vec!["test-ingester-0".into(), "test-ingester-1".into()];
        let (fetch_response_tx, mut fetch_stream) = ServiceStream::new_bounded(5);

        let ingester_pool = IngesterPool::default();

        let (service_stream_tx_0, service_stream_0) = ServiceStream::new_unbounded();
        let (service_stream_tx_1, service_stream_1) = ServiceStream::new_unbounded();

        let mut ingester_mock_0 = IngesterServiceClient::mock();
        ingester_mock_0
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid, "test-index:0");
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id, 1);
                assert_eq!(request.from_position_exclusive(), 0u64);
                assert_eq!(request.to_position_inclusive(), Position::Eof);

                Ok(service_stream_0)
            });
        let ingester_0: IngesterServiceClient = ingester_mock_0.into();

        let mut ingester_mock_1 = IngesterServiceClient::mock();
        ingester_mock_1
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid, "test-index:0");
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id, 1);
                assert_eq!(request.from_position_exclusive(), 1u64);
                assert_eq!(request.to_position_inclusive(), Position::Eof);

                Ok(service_stream_1)
            });
        let ingester_1: IngesterServiceClient = ingester_mock_1.into();

        ingester_pool.insert("test-ingester-0".into(), ingester_0);
        ingester_pool.insert("test-ingester-1".into(), ingester_1);

        let fetch_response = FetchResponseV2 {
            index_uid: "test-index:0".into(),
            source_id: "test-source".into(),
            shard_id: 1,
            mrecord_batch: None,
            from_position_exclusive: Some(Position::from(0u64)),
            to_position_inclusive: Some(Position::from(1u64)),
        };
        service_stream_tx_0.send(Ok(fetch_response)).unwrap();

        let ingest_error = IngestV2Error::Internal("test-error-0".into());
        service_stream_tx_0.send(Err(ingest_error)).unwrap();

        let fetch_response = FetchResponseV2 {
            index_uid: "test-index:0".into(),
            source_id: "test-source".into(),
            shard_id: 1,
            mrecord_batch: None,
            from_position_exclusive: Some(Position::from(1u64)),
            to_position_inclusive: Some(Position::Eof),
        };
        service_stream_tx_1.send(Ok(fetch_response)).unwrap();

        fault_tolerant_fetch_task(
            client_id,
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            to_position_inclusive,
            ingester_ids,
            ingester_pool,
            fetch_response_tx,
        )
        .await;

        let fetch_reponse = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fetch_reponse.from_position_exclusive(),
            Position::from(0u64)
        );
        assert_eq!(fetch_reponse.to_position_inclusive(), 1u64);

        let fetch_reponse = timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            fetch_reponse.from_position_exclusive(),
            Position::from(1u64)
        );
        assert_eq!(fetch_reponse.to_position_inclusive(), Position::Eof);

        assert!(timeout(Duration::from_millis(50), fetch_stream.next())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_multi_fetch_stream() {
        let self_node_id: NodeId = "test-node".into();
        let client_id = "test-client".to_string();
        let ingester_pool = IngesterPool::default();
        let _multi_fetch_stream = MultiFetchStream::new(self_node_id, client_id, ingester_pool);
        // TODO: Backport from original branch.
    }
}
