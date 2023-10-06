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
use std::ops::RangeInclusive;
use std::sync::Arc;

use futures::StreamExt;
use mrecordlog::MultiRecordLog;
use quickwit_common::ServiceStream;
use quickwit_proto::ingest::ingester::{FetchResponseV2, IngesterService, OpenFetchStreamRequest};
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result};
use quickwit_proto::types::{queue_id, NodeId, QueueId, ShardId, SourceId};
use quickwit_proto::IndexUid;
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use super::models::ShardStatus;
use crate::{ClientId, DocBatchBuilderV2, IngesterPool};

/// A fetch task is responsible for waiting and pushing new records written to a shard's record log
/// into a channel named `fetch_response_tx`.
pub(super) struct FetchTask {
    /// Uniquely identifies the consumer of the fetch task for logging and debugging purposes.
    client_id: ClientId,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    queue_id: QueueId,
    /// Range of records to fetch. When there is no upper bound, the end of the range is set to
    /// `u64::MAX`.
    fetch_range: RangeInclusive<u64>,
    mrecordlog: Arc<RwLock<MultiRecordLog>>,
    fetch_response_tx: mpsc::Sender<IngestV2Result<FetchResponseV2>>,
    /// This channel notifies the fetch task when new records are available. This way the fetch
    /// task does not need to grab the lock and poll the log unnecessarily.
    shard_status_rx: watch::Receiver<ShardStatus>,
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

// TODO: Drop when `Iterator::advance_by()` is stabilized.
fn advance_by(range: &mut RangeInclusive<u64>, len: u64) {
    *range = *range.start() + len..=*range.end();
}

type FetchTaskHandle = JoinHandle<(u64, Option<u64>)>;

impl FetchTask {
    pub const DEFAULT_BATCH_NUM_BYTES: usize = 1024 * 1024; // 1 MiB

    pub fn spawn(
        open_fetch_stream_request: OpenFetchStreamRequest,
        mrecordlog: Arc<RwLock<MultiRecordLog>>,
        shard_status_rx: watch::Receiver<ShardStatus>,
        batch_num_bytes: usize,
    ) -> (
        ServiceStream<IngestV2Result<FetchResponseV2>>,
        FetchTaskHandle,
    ) {
        let (fetch_response_tx, fetch_stream) = ServiceStream::new_bounded(3);
        let from_position_inclusive = open_fetch_stream_request
            .from_position_exclusive
            .map(|position| position + 1)
            .unwrap_or(0);
        let to_position_inclusive = open_fetch_stream_request
            .to_position_inclusive
            .unwrap_or(u64::MAX);
        let mut fetch_task = Self {
            queue_id: open_fetch_stream_request.queue_id(),
            client_id: open_fetch_stream_request.client_id,
            index_uid: open_fetch_stream_request.index_uid.into(),
            source_id: open_fetch_stream_request.source_id,
            shard_id: open_fetch_stream_request.shard_id,
            fetch_range: from_position_inclusive..=to_position_inclusive,
            mrecordlog,
            fetch_response_tx,
            shard_status_rx,
            batch_num_bytes,
        };
        let future = async move { fetch_task.run().await };
        let fetch_task_handle: FetchTaskHandle = tokio::spawn(future);
        (fetch_stream, fetch_task_handle)
    }

    /// Waits for new records. Returns `false` if the ingester is dropped.
    async fn wait_for_new_records(&mut self) -> bool {
        loop {
            let shard_status = self.shard_status_rx.borrow().clone();

            if shard_status.shard_state.is_closed()
                && shard_status.publish_position_inclusive <= *self.fetch_range.start()
            {
                // The shard is closed and we have fetched all records up to the publish position.
                return false;
            }
            if shard_status.replication_position_inclusive >= *self.fetch_range.start() {
                // Some new records are available.
                return true;
            }
            if self.shard_status_rx.changed().await.is_err() {
                // The ingester was dropped.
                return false;
            }
        }
    }

    /// Runs the fetch task. It waits for new records in the log and pushes them into the fetch
    /// response channel until `to_position_inclusive` is reached, the shard is closed and
    /// `to_position_inclusive` is reached, or the ingester is dropped. It returns the total number
    /// of records fetched and the position of the last record fetched.
    async fn run(&mut self) -> (u64, Option<u64>) {
        debug!(
            client_id=%self.client_id,
            index_uid=%self.index_uid,
            source_id=%self.source_id,
            shard_id=%self.shard_id,
            fetch_range=?self.fetch_range,
            "Spawning fetch task."
        );
        let mut total_num_docs = 0;

        while !self.fetch_range.is_empty() {
            if !self.wait_for_new_records().await {
                break;
            }
            let fetch_range = self.fetch_range.clone();
            let mrecordlog_guard = self.mrecordlog.read().await;

            let Ok(docs) = mrecordlog_guard.range(&self.queue_id, fetch_range) else {
                warn!(
                    client_id=%self.client_id,
                    index_uid=%self.index_uid,
                    source_id=%self.source_id,
                    shard_id=%self.shard_id,
                    "Failed to read from record log because it was dropped."
                );
                break;
            };
            let mut doc_batch_builder = DocBatchBuilderV2::with_capacity(self.batch_num_bytes);

            for (_position, doc) in docs {
                if doc_batch_builder.num_bytes() + doc.len() > doc_batch_builder.capacity() {
                    break;
                }
                doc_batch_builder.add_doc(doc.borrow());
            }
            // Drop the lock while we send the message.
            drop(mrecordlog_guard);

            let doc_batch = doc_batch_builder.build();
            let num_docs = doc_batch.num_docs() as u64;
            total_num_docs += num_docs;

            let fetch_response = FetchResponseV2 {
                index_uid: self.index_uid.clone().into(),
                source_id: self.source_id.clone(),
                shard_id: self.shard_id,
                doc_batch: Some(doc_batch),
                from_position_inclusive: *self.fetch_range.start(),
            };
            advance_by(&mut self.fetch_range, num_docs);

            if self
                .fetch_response_tx
                .send(Ok(fetch_response))
                .await
                .is_err()
            {
                // The ingester was dropped.
                break;
            }
        }
        debug!(
            client_id=%self.client_id,
            index_uid=%self.index_uid,
            source_id=%self.source_id,
            shard_id=%self.shard_id,
            "Fetch task completed."
        );
        if total_num_docs == 0 {
            (0, None)
        } else {
            (total_num_docs, Some(*self.fetch_range.start() - 1))
        }
    }
}

/// Combines multiple fetch streams originating from different ingesters into a single stream. It
/// tolerates the failure of ingesters and automatically fails over to replica shards.
pub struct MultiFetchStream {
    self_node_id: NodeId,
    client_id: ClientId,
    ingester_pool: IngesterPool,
    fetch_task_handles: HashMap<QueueId, JoinHandle<()>>,
    fetch_response_rx: mpsc::Receiver<IngestV2Result<FetchResponseV2>>,
    fetch_response_tx: mpsc::Sender<IngestV2Result<FetchResponseV2>>,
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
    pub fn fetch_response_tx(&self) -> mpsc::Sender<IngestV2Result<FetchResponseV2>> {
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
        from_position_exclusive: Option<u64>,
        to_position_inclusive: Option<u64>,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(index_uid.as_str(), &source_id, shard_id);
        let entry = self.fetch_task_handles.entry(queue_id.clone());

        if let Entry::Occupied(_) = entry {
            return Err(IngestV2Error::Internal(format!(
                "stream has already subscribed to shard `{queue_id}`"
            )));
        }
        let (mut preferred_ingester_id, mut failover_ingester_id) =
            select_preferred_and_failover_ingesters(&self.self_node_id, leader_id, follower_id_opt);

        // Obtain a fetch stream from the preferred or failover ingester.
        let fetch_stream = loop {
            let Some(mut ingester) = self.ingester_pool.get(&preferred_ingester_id) else {
                if let Some(failover_ingester_id) = failover_ingester_id.take() {
                    warn!(
                        client_id=%self.client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        "Ingester `{preferred_ingester_id}` is not available. Failing over to ingester `{failover_ingester_id}`."
                    );
                    preferred_ingester_id = failover_ingester_id;
                    continue;
                };
                return Err(IngestV2Error::Internal(format!(
                    "shard `{queue_id}` is unavailable"
                )));
            };
            let open_fetch_stream_request = OpenFetchStreamRequest {
                client_id: self.client_id.clone(),
                index_uid: index_uid.clone().into(),
                source_id: source_id.clone(),
                shard_id,
                from_position_exclusive,
                to_position_inclusive,
            };
            match ingester.open_fetch_stream(open_fetch_stream_request).await {
                Ok(fetch_stream) => {
                    break fetch_stream;
                }
                Err(error) => {
                    if let Some(failover_ingester_id) = failover_ingester_id.take() {
                        warn!(
                            client_id=%self.client_id,
                            index_uid=%index_uid,
                            source_id=%source_id,
                            shard_id=%shard_id,
                            error=?error,
                            "Failed to open fetch stream from `{preferred_ingester_id}`. Failing over to ingester `{failover_ingester_id}`."
                        );
                        preferred_ingester_id = failover_ingester_id;
                        continue;
                    };
                    error!(
                        client_id=%self.client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        error=?error,
                        "Failed to open fetch stream from `{preferred_ingester_id}`."
                    );
                    return Err(IngestV2Error::Internal(format!(
                        "shard `{queue_id}` is unavailable"
                    )));
                }
            };
        };
        let client_id = self.client_id.clone();
        let ingester_pool = self.ingester_pool.clone();
        let fetch_response_tx = self.fetch_response_tx.clone();
        let fetch_task_future = fault_tolerant_fetch_task(
            client_id,
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            to_position_inclusive,
            preferred_ingester_id,
            failover_ingester_id,
            ingester_pool,
            fetch_stream,
            fetch_response_tx,
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
    pub async fn next(&mut self) -> IngestV2Result<FetchResponseV2> {
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
    mut from_position_exclusive: Option<u64>,
    to_position_inclusive: Option<u64>,

    preferred_ingester_id: NodeId,
    mut failover_ingester_id: Option<NodeId>,

    ingester_pool: IngesterPool,

    mut fetch_stream: ServiceStream<IngestV2Result<FetchResponseV2>>,
    fetch_response_tx: mpsc::Sender<IngestV2Result<FetchResponseV2>>,
) {
    while let Some(fetch_response_result) = fetch_stream.next().await {
        match fetch_response_result {
            Ok(fetch_response) => {
                from_position_exclusive = fetch_response.to_position_inclusive();
                if fetch_response_tx.send(Ok(fetch_response)).await.is_err() {
                    // The stream was dropped.
                    break;
                }
            }
            Err(ingest_error) => {
                if let Some(failover_ingester_id) = failover_ingester_id.take() {
                    warn!(
                        client_id=%client_id,
                        index_uid=%index_uid,
                        source_id=%source_id,
                        shard_id=%shard_id,
                        error=?ingest_error,
                        "Error fetching from `{preferred_ingester_id}`. Failing over to ingester `{failover_ingester_id}`."
                    );
                    let mut ingester = ingester_pool
                        .get(&preferred_ingester_id)
                        .expect("TODO: handle error");
                    let open_fetch_stream_request = OpenFetchStreamRequest {
                        client_id: client_id.clone(),
                        index_uid: index_uid.clone().into(),
                        source_id: source_id.clone(),
                        shard_id,
                        from_position_exclusive,
                        to_position_inclusive,
                    };
                    fetch_stream = ingester
                        .open_fetch_stream(open_fetch_stream_request)
                        .await
                        .expect("TODO:");
                    continue;
                }
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    error=?ingest_error,
                    "Error fetching from `{preferred_ingester_id}`."
                );
                let _ = fetch_response_tx.send(Err(ingest_error)).await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use mrecordlog::MultiRecordLog;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::types::queue_id;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn test_fetch_task() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(
            MultiRecordLog::open(tempdir.path()).await.unwrap(),
        ));
        let client_id = "test-client".to_string();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: None,
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        let queue_id = queue_id(&index_uid, &source_id, 0);
        mrecordlog_guard.create_queue(&queue_id).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-000"))
            .await
            .unwrap();
        let shard_status = ShardStatus {
            replication_position_inclusive: 0.into(),
            ..Default::default()
        };
        shard_status_tx.send(shard_status).unwrap();
        drop(mrecordlog_guard);

        let fetch_response = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetch_response.index_uid, "test-index:0");
        assert_eq!(fetch_response.source_id, "test-source");
        assert_eq!(fetch_response.shard_id, 0);
        assert_eq!(fetch_response.from_position_inclusive, 0);
        assert_eq!(fetch_response.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_response.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-000"
        );

        let mut mrecordlog_guard = mrecordlog.write().await;

        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-001"))
            .await
            .unwrap();
        drop(mrecordlog_guard);

        timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap_err();

        let shard_status = ShardStatus {
            replication_position_inclusive: 1.into(),
            ..Default::default()
        };
        shard_status_tx.send(shard_status).unwrap();

        let fetch_response = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetch_response.from_position_inclusive, 1);
        assert_eq!(fetch_response.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_response.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-001"
        );

        let shard_status = ShardStatus {
            shard_state: ShardState::Closed,
            replication_position_inclusive: 1.into(),
            publish_position_inclusive: 1.into(),
        };
        shard_status_tx.send(shard_status).unwrap();

        let (num_docs, last_position) = fetch_task_handle.await.unwrap();
        assert_eq!(num_docs, 2);
        assert_eq!(last_position, Some(1));
    }

    #[tokio::test]
    async fn test_fetch_task_up_to_position() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(
            MultiRecordLog::open(tempdir.path()).await.unwrap(),
        ));
        let client_id = "test-client".to_string();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: Some(0),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            1024,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        let queue_id = queue_id(&index_uid, &source_id, 0);
        mrecordlog_guard.create_queue(&queue_id).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-000"))
            .await
            .unwrap();

        let shard_status = ShardStatus {
            replication_position_inclusive: 0.into(),
            ..Default::default()
        };
        shard_status_tx.send(shard_status).unwrap();
        drop(mrecordlog_guard);

        let fetch_response = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetch_response.index_uid, "test-index:0");
        assert_eq!(fetch_response.source_id, "test-source");
        assert_eq!(fetch_response.shard_id, 0);
        assert_eq!(fetch_response.from_position_inclusive, 0);
        assert_eq!(fetch_response.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_response.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-000"
        );

        let (num_docs, last_position) = fetch_task_handle.await.unwrap();
        assert_eq!(num_docs, 1);
        assert_eq!(last_position, Some(0));
    }

    #[tokio::test]
    async fn test_fetch_task_batch_num_bytes() {
        let tempdir = tempfile::tempdir().unwrap();
        let mrecordlog = Arc::new(RwLock::new(
            MultiRecordLog::open(tempdir.path()).await.unwrap(),
        ));
        let client_id = "test-client".to_string();
        let index_uid = "test-index:0".to_string();
        let source_id = "test-source".to_string();
        let open_fetch_stream_request = OpenFetchStreamRequest {
            client_id: client_id.clone(),
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: 0,
            from_position_exclusive: None,
            to_position_inclusive: Some(2),
        };
        let (shard_status_tx, shard_status_rx) = watch::channel(ShardStatus::default());
        let (mut fetch_stream, fetch_task_handle) = FetchTask::spawn(
            open_fetch_stream_request,
            mrecordlog.clone(),
            shard_status_rx,
            30,
        );
        let mut mrecordlog_guard = mrecordlog.write().await;

        let queue_id = queue_id(&index_uid, &source_id, 0);
        mrecordlog_guard.create_queue(&queue_id).await.unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-000"))
            .await
            .unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-001"))
            .await
            .unwrap();
        mrecordlog_guard
            .append_record(&queue_id, None, Bytes::from_static(b"test-doc-002"))
            .await
            .unwrap();

        let shard_status = ShardStatus {
            replication_position_inclusive: 2.into(),
            ..Default::default()
        };
        shard_status_tx.send(shard_status).unwrap();
        drop(mrecordlog_guard);

        let fetch_response = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetch_response.index_uid, "test-index:0");
        assert_eq!(fetch_response.source_id, "test-source");
        assert_eq!(fetch_response.shard_id, 0);
        assert_eq!(fetch_response.from_position_inclusive, 0);
        assert_eq!(
            fetch_response.doc_batch.as_ref().unwrap().doc_lengths,
            [12, 12]
        );
        assert_eq!(
            fetch_response.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-000test-doc-001"
        );
        let fetch_response = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetch_response.index_uid, "test-index:0");
        assert_eq!(fetch_response.source_id, "test-source");
        assert_eq!(fetch_response.shard_id, 0);
        assert_eq!(fetch_response.from_position_inclusive, 2);
        assert_eq!(fetch_response.doc_batch.as_ref().unwrap().doc_lengths, [12]);
        assert_eq!(
            fetch_response.doc_batch.as_ref().unwrap().doc_buffer,
            "test-doc-002"
        );

        let (num_docs, last_position) = fetch_task_handle.await.unwrap();
        assert_eq!(num_docs, 3);
        assert_eq!(last_position, Some(2));
    }

    #[tokio::test]
    async fn test_fault_tolerant_fetch_task() {
        // TODO: Backport from original branch.
    }

    #[tokio::test]
    async fn test_multi_fetch_stream() {
        let node_id: NodeId = "test-node".into();
        let client_id = "test-client".to_string();
        let ingester_pool = IngesterPool::default();
        let _multi_fetch_stream = MultiFetchStream::new(node_id, client_id, ingester_pool);
        // TODO: Backport from original branch.
    }
}
