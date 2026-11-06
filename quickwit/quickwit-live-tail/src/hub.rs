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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use quickwit_doc_mapper::{DocMapper, JsonObject};
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::types::IndexUid;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tracing::debug;

use crate::metrics::LIVE_TAIL_DOCS_PUBLISHED;

/// Number of batches buffered per index before slow subscribers start lagging.
const DEFAULT_CHANNEL_CAPACITY: usize = 64;

/// Maximum size of the documents of the batches of an index retained by the hub and the sessions.
const DEFAULT_MAX_RETAINED_NUM_BYTES_PER_INDEX: usize = 64 * 1024 * 1024;

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
    /// Size of the documents of the batch retained by the channel of the index. Decremented when
    /// the batch is dropped, i.e. when neither the channel nor any session holds it anymore.
    retained_num_bytes: Arc<AtomicUsize>,
}

impl TailBatch {
    pub fn new(doc_mapper: Arc<DocMapper>, doc_batch: DocBatchV2) -> Self {
        Self::with_retained_num_bytes(doc_mapper, doc_batch, Arc::default())
    }

    fn with_retained_num_bytes(
        doc_mapper: Arc<DocMapper>,
        doc_batch: DocBatchV2,
        retained_num_bytes: Arc<AtomicUsize>,
    ) -> Self {
        retained_num_bytes.fetch_add(doc_batch.num_bytes(), Ordering::Relaxed);
        Self {
            doc_mapper,
            doc_batch,
            docs: OnceLock::new(),
            retained_num_bytes,
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

impl Drop for TailBatch {
    fn drop(&mut self) {
        self.retained_num_bytes
            .fetch_sub(self.doc_batch.num_bytes(), Ordering::Relaxed);
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

#[derive(Debug)]
struct IndexChannel {
    sender: broadcast::Sender<Arc<TailBatch>>,
    /// Size of the documents of the batches published to the channel that are still held by the
    /// channel or by a session.
    retained_num_bytes: Arc<AtomicUsize>,
    /// Number of batches not published because `retained_num_bytes` would exceed the limit.
    num_dropped_batches: Arc<AtomicU64>,
}

/// Fans out the documents persisted by the ingester of this node to live tail sessions.
///
/// Contract: publishing never blocks and never fails ingestion. Subscribers that fall behind lose
/// batches and are notified via [`TailSubscription`].
///
/// The memory used by live tail is bounded: the batches of an index are not published while the
/// batches already published and still held by the channel or by a session exceed
/// `max_retained_num_bytes_per_index`. This happens when a session stops consuming, for instance
/// because its client does not read: all the sessions of the index then miss the batches.
#[derive(Debug, Clone)]
pub struct LiveTailHub {
    channels: Arc<Mutex<HashMap<IndexUid, IndexChannel>>>,
    channel_capacity: usize,
    max_retained_num_bytes_per_index: usize,
}

impl Default for LiveTailHub {
    fn default() -> Self {
        Self::new(
            DEFAULT_CHANNEL_CAPACITY,
            DEFAULT_MAX_RETAINED_NUM_BYTES_PER_INDEX,
        )
    }
}

impl LiveTailHub {
    pub fn new(channel_capacity: usize, max_retained_num_bytes_per_index: usize) -> Self {
        assert!(channel_capacity > 0, "channel capacity must be positive");
        Self {
            channels: Arc::default(),
            channel_capacity,
            max_retained_num_bytes_per_index,
        }
    }

    /// Returns whether at least one live tail session is subscribed to `index_uid`.
    pub fn is_active(&self, index_uid: &IndexUid) -> bool {
        let channels = self.channels.lock().expect("lock should not be poisoned");
        let Some(channel) = channels.get(index_uid) else {
            return false;
        };
        channel.sender.receiver_count() > 0
    }

    /// Publishes a batch of documents persisted for `index_uid`. Never blocks.
    ///
    /// When nobody is subscribed to the index, this is a lookup in a hash map: the batch is
    /// dropped, and the channel of the index, if any, is removed.
    ///
    /// Precondition: `doc_mapper` is the doc mapper of the shard that persisted the documents.
    pub fn publish(&self, index_uid: &IndexUid, doc_mapper: Arc<DocMapper>, doc_batch: DocBatchV2) {
        let mut channels = self.channels.lock().expect("lock should not be poisoned");
        let Some(channel) = channels.get(index_uid) else {
            return;
        };
        if channel.sender.receiver_count() == 0 {
            // All the sessions of the index ended.
            channels.remove(index_uid);
            return;
        }
        if doc_batch.is_empty() {
            return;
        }
        let retained_num_bytes = channel.retained_num_bytes.load(Ordering::Relaxed);

        if retained_num_bytes + doc_batch.num_bytes() > self.max_retained_num_bytes_per_index {
            debug!(%index_uid, retained_num_bytes, "dropping live tail batch: memory limit reached");
            channel.num_dropped_batches.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let num_docs = doc_batch.num_docs() as u64;
        debug!(%index_uid, num_docs, "publishing live tail batch");
        let tail_batch = TailBatch::with_retained_num_bytes(
            doc_mapper,
            doc_batch,
            channel.retained_num_bytes.clone(),
        );
        // An error means that the last session ended since the receiver count was checked.
        if channel.sender.send(Arc::new(tail_batch)).is_ok() {
            LIVE_TAIL_DOCS_PUBLISHED.inc_by(num_docs);
        }
    }

    /// Subscribes to the documents persisted for `index_uid` by the ingester of this node,
    /// including in the shards opened after the subscription.
    pub fn subscribe(&self, index_uid: &IndexUid) -> TailSubscription {
        let mut channels = self.channels.lock().expect("lock should not be poisoned");
        // Removes the channels of the indexes that nobody watches anymore, including the indexes
        // that are no longer ingested into, so that the number of channels is bounded by the
        // number of sessions.
        channels.retain(|_, channel| channel.sender.receiver_count() > 0);
        let channel = channels
            .entry(index_uid.clone())
            .or_insert_with(|| IndexChannel {
                sender: broadcast::channel(self.channel_capacity).0,
                retained_num_bytes: Arc::default(),
                num_dropped_batches: Arc::default(),
            });
        let num_seen_dropped_batches = channel.num_dropped_batches.load(Ordering::Relaxed);
        TailSubscription {
            batch_rx: channel.sender.subscribe(),
            num_dropped_batches: channel.num_dropped_batches.clone(),
            num_seen_dropped_batches,
        }
    }
}

/// Subscription of a live tail session to the batches of an index.
pub struct TailSubscription {
    batch_rx: broadcast::Receiver<Arc<TailBatch>>,
    num_dropped_batches: Arc<AtomicU64>,
    num_seen_dropped_batches: u64,
}

impl TailSubscription {
    /// Receives the next batch. Returns [`RecvError::Lagged`] when the subscriber fell behind and
    /// missed batches.
    pub async fn recv(&mut self) -> Result<Arc<TailBatch>, RecvError> {
        self.batch_rx.recv().await
    }

    pub fn try_recv(&mut self) -> Result<Arc<TailBatch>, TryRecvError> {
        self.batch_rx.try_recv()
    }

    /// Returns the number of batches that the hub did not publish because of the memory limit
    /// since the last call, or since the subscription for the first call.
    pub fn take_num_dropped_batches(&mut self) -> u64 {
        let num_dropped_batches = self.num_dropped_batches.load(Ordering::Relaxed);
        let num_new_dropped_batches = num_dropped_batches - self.num_seen_dropped_batches;
        self.num_seen_dropped_batches = num_dropped_batches;
        num_new_dropped_batches
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
    async fn test_live_tail_hub_removes_unwatched_channels() {
        let hub = LiveTailHub::default();
        let index_uid_a = IndexUid::for_test("test-index-a", 0);
        let index_uid_b = IndexUid::for_test("test-index-b", 0);

        drop(hub.subscribe(&index_uid_a));
        drop(hub.subscribe(&index_uid_b));
        assert_eq!(hub.channels.lock().unwrap().len(), 1);

        // Publishing to an index that nobody watches removes its channel.
        publish(&hub, &index_uid_b, &[json!({"body": "lost"})]);
        assert!(hub.channels.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_live_tail_hub_bounds_retained_bytes() {
        let doc = json!({"body": "x".repeat(100)});
        let batch_num_bytes = doc_batch_for_test(std::slice::from_ref(&doc)).num_bytes();
        let hub = LiveTailHub::new(64, 2 * batch_num_bytes);
        let index_uid = IndexUid::for_test("test-index", 0);
        let mut subscription = hub.subscribe(&index_uid);

        // The subscriber does not consume: the third batch exceeds the limit.
        for _ in 0..3 {
            publish(&hub, &index_uid, std::slice::from_ref(&doc));
        }
        assert_eq!(subscription.take_num_dropped_batches(), 1);
        assert_eq!(subscription.take_num_dropped_batches(), 0);

        // Once the batches are consumed and released, publishing resumes.
        drop(subscription.try_recv().unwrap());
        drop(subscription.try_recv().unwrap());
        publish(&hub, &index_uid, std::slice::from_ref(&doc));
        subscription.try_recv().unwrap();
        assert_eq!(subscription.take_num_dropped_batches(), 0);

        // A new subscriber only sees the batches dropped after it subscribed.
        let mut other_subscription = hub.subscribe(&index_uid);
        assert_eq!(other_subscription.take_num_dropped_batches(), 0);
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
        let hub = LiveTailHub::new(2, DEFAULT_MAX_RETAINED_NUM_BYTES_PER_INDEX);
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
