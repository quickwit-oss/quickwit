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

use super::ingester::ShardStatus;
use crate::{ClientId, DocBatchBuilderV2, IngesterPool};

pub(super) struct FetchTask {
    client_id: ClientId,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    queue_id: QueueId,
    from_position_inclusive: u64,
    to_position_inclusive: u64,
    mrecordlog: Arc<RwLock<MultiRecordLog>>,
    fetch_response_tx: mpsc::Sender<IngestV2Result<FetchResponseV2>>,
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

impl FetchTask {
    pub const DEFAULT_BATCH_NUM_BYTES: usize = 1024 * 1024; // 1 MiB

    pub fn spawn(
        open_fetch_stream_request: OpenFetchStreamRequest,
        mrecordlog: Arc<RwLock<MultiRecordLog>>,
        shard_status_rx: watch::Receiver<ShardStatus>,
        batch_num_bytes: usize,
    ) -> ServiceStream<IngestV2Result<FetchResponseV2>> {
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
            from_position_inclusive,
            to_position_inclusive,
            mrecordlog,
            fetch_response_tx,
            shard_status_rx,
            batch_num_bytes,
        };
        let future = async move { fetch_task.run().await };
        tokio::spawn(future);
        fetch_stream
    }

    async fn run(&mut self) {
        debug!(
            client_id=%self.client_id,
            index_uid=%self.index_uid,
            source_id=%self.source_id,
            shard_id=%self.shard_id,
            from_position_exclusive=%self.from_position_inclusive,
            to_position_inclusive=%self.to_position_inclusive,
            "Spawning fetch task."
        );
        'outer: while self.from_position_inclusive <= self.to_position_inclusive {
            while self.shard_status_rx.borrow().replication_position_inclusive
                < self.from_position_inclusive
            {
                if self.shard_status_rx.changed().await.is_err() {
                    // The ingester was dropped.
                    break 'outer;
                }
            }
            let fetch_range = self.from_position_inclusive..=self.to_position_inclusive;
            let mrecordlog_guard = self.mrecordlog.read().await;

            let Ok(docs) = mrecordlog_guard.range(&self.queue_id, fetch_range) else {
                // The queue no longer exists.
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

            let fetch_response = FetchResponseV2 {
                index_uid: self.index_uid.clone().into(),
                source_id: self.source_id.clone(),
                shard_id: self.shard_id,
                doc_batch: Some(doc_batch),
                from_position_inclusive: self.from_position_inclusive,
            };
            self.from_position_inclusive += num_docs;

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
            to_position_inclusive=%self.to_position_inclusive,
            "Fetch task completed."
        )
    }
}

/// Combines multiple fetch streams originating from different ingesters into a single stream. It
/// tolerates the failure of ingesters and automatically fails over to replica shards.
pub struct MultiFetchStream {
    node_id: NodeId,
    client_id: ClientId,
    ingester_pool: IngesterPool,
    fetch_task_handles: HashMap<QueueId, JoinHandle<()>>,
    fetch_response_rx: mpsc::Receiver<IngestV2Result<FetchResponseV2>>,
    fetch_response_tx: mpsc::Sender<IngestV2Result<FetchResponseV2>>,
}

impl MultiFetchStream {
    pub fn new(node_id: NodeId, client_id: ClientId, ingester_pool: IngesterPool) -> Self {
        let (fetch_response_tx, fetch_response_rx) = mpsc::channel(3);
        Self {
            node_id,
            client_id,
            ingester_pool,
            fetch_task_handles: HashMap::new(),
            fetch_response_rx,
            fetch_response_tx,
        }
    }

    /// Subscribes to a shard and fails over to the replica if an error occurs.
    pub async fn subscribe(
        &mut self,
        leader_id: NodeId,
        follower_id: Option<NodeId>,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
        mut from_position_exclusive: Option<u64>,
        to_position_inclusive: Option<u64>,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(index_uid.as_str(), &source_id, shard_id);
        let entry = self.fetch_task_handles.entry(queue_id.clone());

        if let Entry::Occupied(_) = entry {
            return Err(IngestV2Error::Internal(format!(
                "Stream has already subscribed to shard `{queue_id}`."
            )));
        }
        let (mut preferred_ingester_id, mut failover_ingester_id) =
            if let Some(follower_id) = follower_id {
                if leader_id == self.node_id {
                    (leader_id, Some(follower_id))
                } else if follower_id == self.node_id {
                    (follower_id, Some(leader_id))
                } else if rand::random::<bool>() {
                    (leader_id, Some(follower_id))
                } else {
                    (follower_id, Some(leader_id))
                }
            } else {
                (leader_id, None)
            };
        let mut fetch_stream = loop {
            let Some(mut ingester) = self.ingester_pool.get(&preferred_ingester_id).await else {
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
                    "Shard `{queue_id}` is unavailable."
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
                        "Shard `{queue_id}` is unavailable."
                    )));
                }
            };
        };
        let client_id = self.client_id.clone();
        let ingester_pool = self.ingester_pool.clone();
        let fetch_response_tx = self.fetch_response_tx.clone();

        let fetch_task = async move {
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
                                .await
                                .expect("FIXME: handle error");
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
                                .expect("FIXME:");
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
        };
        let fetch_task_handle = tokio::spawn(fetch_task);
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

    pub async fn next(&mut self) -> Option<IngestV2Result<FetchResponseV2>> {
        self.fetch_response_rx.recv().await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use mrecordlog::MultiRecordLog;
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
        let mut fetch_stream = FetchTask::spawn(
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
    }

    #[tokio::test]
    async fn test_fetch_task_to_position() {
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
        let mut fetch_stream = FetchTask::spawn(
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
        let mut fetch_stream = FetchTask::spawn(
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
