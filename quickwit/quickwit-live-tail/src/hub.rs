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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use quickwit_doc_mapper::{DocMapper, JsonObject};
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::types::IndexUid;
use tokio::sync::broadcast;
use tracing::debug;

use crate::metrics::LIVE_TAIL_DOCS_PUBLISHED;

/// Number of batches buffered per index before slow subscribers start lagging.
const DEFAULT_CHANNEL_CAPACITY: usize = 64;

/// A batch of documents persisted by an ingester, as seen by live tail subscribers.
///
/// The documents are parsed lazily, once for all the sessions, the first time a session filters
/// the batch, so that publishing costs nothing on the ingest path.
pub struct TailBatch {
    /// Doc mapper of the shard that persisted the documents. Filtering with it, rather than with
    /// the doc mapping of the index at the time the session was opened, keeps live tail
    /// consistent with indexing across index updates.
    pub doc_mapper: Arc<DocMapper>,
    doc_batch: DocBatchV2,
    docs: OnceLock<Vec<TailDoc>>,
}

impl TailBatch {
    pub fn new(doc_mapper: Arc<DocMapper>, doc_batch: DocBatchV2) -> Self {
        Self {
            doc_mapper,
            doc_batch,
            docs: OnceLock::new(),
        }
    }

    pub fn num_docs(&self) -> usize {
        self.doc_batch.num_docs()
    }

    /// Returns the documents of the batch that are JSON objects, in order.
    ///
    /// The first call parses the documents: it is CPU-intensive and must be made from the live
    /// tail thread pool.
    pub fn docs(&self) -> &[TailDoc] {
        self.docs.get_or_init(|| parse_docs(&self.doc_batch))
    }
}

/// Parses the documents of `doc_batch` as JSON objects.
///
/// The documents were validated by the ingester, unless validation is disabled for the index:
/// the invalid ones are skipped, like the indexing pipeline does.
fn parse_docs(doc_batch: &DocBatchV2) -> Vec<TailDoc> {
    let mut tail_docs = Vec::with_capacity(doc_batch.num_docs());

    for (_doc_uid, doc_bytes) in doc_batch.docs() {
        let Ok(json_obj) = serde_json::from_slice::<JsonObject>(&doc_bytes) else {
            continue;
        };
        tail_docs.push(TailDoc {
            json_obj,
            num_bytes: doc_bytes.len() as u64,
        });
    }
    tail_docs
}

impl fmt::Debug for TailBatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TailBatch")
            .field("doc_mapping_uid", &self.doc_mapper.doc_mapping_uid())
            .field("num_docs", &self.doc_batch.num_docs())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailDoc {
    pub json_obj: JsonObject,
    /// Size of the raw document, as passed to `DocMapper::doc_from_json_obj` by the indexing
    /// pipeline, so that `_doc_length` evaluates identically in live tail and search.
    pub num_bytes: u64,
}

/// Fans out the documents persisted by the ingester of this node to live tail sessions.
///
/// Contract: publishing never blocks and never fails ingestion. Subscribers that fall behind lose
/// batches and are notified via [`broadcast::error::RecvError::Lagged`].
#[derive(Debug, Clone)]
pub struct LiveTailHub {
    channels: Arc<Mutex<HashMap<IndexUid, broadcast::Sender<Arc<TailBatch>>>>>,
    channel_capacity: usize,
}

impl Default for LiveTailHub {
    fn default() -> Self {
        Self::with_channel_capacity(DEFAULT_CHANNEL_CAPACITY)
    }
}

impl LiveTailHub {
    pub fn with_channel_capacity(channel_capacity: usize) -> Self {
        assert!(channel_capacity > 0, "channel capacity must be positive");
        Self {
            channels: Arc::default(),
            channel_capacity,
        }
    }

    /// Returns whether at least one live tail session is subscribed to `index_uid`.
    ///
    /// The ingester calls this before publishing, so that live tail costs nothing when nobody is
    /// watching.
    pub fn is_active(&self, index_uid: &IndexUid) -> bool {
        let channels = self.channels.lock().expect("lock should not be poisoned");
        let Some(sender) = channels.get(index_uid) else {
            return false;
        };
        sender.receiver_count() > 0
    }

    /// Publishes a batch of documents persisted for `index_uid`. Never blocks.
    ///
    /// Precondition: `doc_mapper` is the doc mapper of the shard that persisted the documents.
    pub fn publish(&self, index_uid: &IndexUid, doc_mapper: Arc<DocMapper>, doc_batch: DocBatchV2) {
        if doc_batch.is_empty() {
            return;
        }
        let channels = self.channels.lock().expect("lock should not be poisoned");
        let Some(sender) = channels.get(index_uid) else {
            return;
        };
        let num_docs = doc_batch.num_docs() as u64;
        debug!(%index_uid, num_docs, "publishing live tail batch");
        let tail_batch = TailBatch::new(doc_mapper, doc_batch);
        // An error means that nobody is subscribed to the index.
        if sender.send(Arc::new(tail_batch)).is_ok() {
            LIVE_TAIL_DOCS_PUBLISHED.inc_by(num_docs);
        }
    }

    /// Subscribes to the documents persisted for `index_uid` by the ingester of this node,
    /// including in the shards opened after the subscription.
    pub fn subscribe(&self, index_uid: &IndexUid) -> broadcast::Receiver<Arc<TailBatch>> {
        let mut channels = self.channels.lock().expect("lock should not be poisoned");
        channels
            .entry(index_uid.clone())
            .or_insert_with(|| broadcast::channel(self.channel_capacity).0)
            .subscribe()
    }
}

#[cfg(test)]
pub(crate) fn doc_batch_for_test(docs: &[serde_json::Value]) -> DocBatchV2 {
    let mut doc_buffer: Vec<u8> = Vec::new();
    let mut doc_lengths: Vec<u32> = Vec::new();

    for doc in docs {
        let doc_json = serde_json::to_vec(doc).unwrap();
        doc_lengths.push(doc_json.len() as u32);
        doc_buffer.extend(doc_json);
    }
    DocBatchV2 {
        doc_uids: (0..docs.len())
            .map(|doc_uid| quickwit_proto::types::DocUid::for_test(doc_uid as u128))
            .collect(),
        doc_buffer: bytes::Bytes::from(doc_buffer),
        doc_lengths,
    }
}

#[cfg(test)]
mod tests {
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use serde_json::json;
    use tokio::sync::broadcast::error::RecvError;

    use super::*;

    fn publish(hub: &LiveTailHub, index_uid: &IndexUid, docs: &[serde_json::Value]) {
        hub.publish(
            index_uid,
            Arc::new(default_doc_mapper_for_test()),
            doc_batch_for_test(docs),
        );
    }

    fn bodies(tail_batch: &TailBatch) -> Vec<serde_json::Value> {
        tail_batch
            .docs()
            .iter()
            .map(|tail_doc| serde_json::Value::Object(tail_doc.json_obj.clone()))
            .collect()
    }

    #[tokio::test]
    async fn test_live_tail_hub_is_active_only_with_subscribers() {
        let hub = LiveTailHub::default();
        let index_uid = IndexUid::for_test("test-index", 0);
        assert!(!hub.is_active(&index_uid));

        // Publishing without subscriber is a no-op.
        publish(&hub, &index_uid, &[json!({"body": "lost"})]);

        let mut receiver = hub.subscribe(&index_uid);
        assert!(hub.is_active(&index_uid));

        publish(&hub, &index_uid, &[json!({"body": "hello"})]);
        let tail_batch = receiver.recv().await.unwrap();
        assert_eq!(tail_batch.num_docs(), 1);
        assert_eq!(bodies(&tail_batch), [json!({"body": "hello"})]);

        drop(receiver);
        assert!(!hub.is_active(&index_uid));
    }

    #[tokio::test]
    async fn test_live_tail_hub_isolates_indexes() {
        let hub = LiveTailHub::default();
        let index_uid_a = IndexUid::for_test("test-index-a", 0);
        let index_uid_b = IndexUid::for_test("test-index-b", 0);

        let mut receiver_a = hub.subscribe(&index_uid_a);
        assert!(hub.is_active(&index_uid_a));
        assert!(!hub.is_active(&index_uid_b));

        publish(&hub, &index_uid_b, &[json!({"body": "b"})]);
        publish(&hub, &index_uid_a, &[json!({"body": "a"})]);

        let tail_batch = receiver_a.recv().await.unwrap();
        assert_eq!(bodies(&tail_batch), [json!({"body": "a"})]);
        assert!(receiver_a.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_live_tail_hub_slow_subscriber_lags() {
        let hub = LiveTailHub::with_channel_capacity(2);
        let index_uid = IndexUid::for_test("test-index", 0);
        let mut receiver = hub.subscribe(&index_uid);

        for i in 0..5 {
            publish(&hub, &index_uid, &[json!({"i": i})]);
        }
        let RecvError::Lagged(num_lagged_batches) = receiver.recv().await.unwrap_err() else {
            panic!("expected lagged error");
        };
        assert_eq!(num_lagged_batches, 3);

        let tail_batch = receiver.recv().await.unwrap();
        assert_eq!(bodies(&tail_batch), [json!({"i": 3})]);
    }

    #[test]
    fn test_tail_batch_skips_docs_that_are_not_json_objects() {
        let doc_batch = DocBatchV2::for_test([r#"{"body": "hello"}"#, "not json", "[1, 2]"]);
        let tail_batch = TailBatch::new(Arc::new(default_doc_mapper_for_test()), doc_batch);
        assert_eq!(tail_batch.num_docs(), 3);
        assert_eq!(bodies(&tail_batch), [json!({"body": "hello"})]);
        assert_eq!(
            tail_batch.docs()[0].num_bytes,
            r#"{"body": "hello"}"#.len() as u64
        );
    }
}
