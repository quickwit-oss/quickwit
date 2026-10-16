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
use std::sync::{Arc, Mutex};

use quickwit_doc_mapper::{DocMapper, JsonObject};
use quickwit_proto::types::IndexUid;
use tokio::sync::broadcast;
use tracing::debug;

use crate::metrics::LIVE_TAIL_DOCS_PUBLISHED;

/// Number of batches buffered per index before slow subscribers start lagging.
const DEFAULT_CHANNEL_CAPACITY: usize = 64;

/// A batch of documents processed by an indexing pipeline, as seen by live tail subscribers.
///
/// The documents are JSON objects after the VRL transform (if any) and that were successfully
/// mapped by the doc mapper, i.e. exactly the documents that are about to be indexed.
#[derive(Debug)]
pub struct TailBatch {
    /// Doc mapper of the pipeline that processed the documents. Filtering with it, rather than
    /// with the doc mapping of the index at the time the session was opened, keeps live tail
    /// consistent with indexing across index updates.
    pub doc_mapper: Arc<DocMapper>,
    pub docs: Vec<TailDoc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TailDoc {
    pub json_obj: JsonObject,
    /// Size of the raw document, as passed to `DocMapper::doc_from_json_obj` by the indexing
    /// pipeline, so that `_doc_length` evaluates identically in live tail and search.
    pub num_bytes: u64,
}

/// Fans out the documents processed by the indexing pipelines of this node to live tail sessions.
///
/// Contract: publishing never blocks and never fails the indexing pipeline. Subscribers that
/// fall behind lose batches and are notified via [`broadcast::error::RecvError::Lagged`].
#[derive(Debug, Clone)]
pub struct LiveTailHub {
    senders: Arc<Mutex<HashMap<IndexUid, broadcast::Sender<Arc<TailBatch>>>>>,
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
            senders: Arc::default(),
            channel_capacity,
        }
    }

    /// Returns the tap that an indexing pipeline uses to publish its documents.
    pub fn tap(&self, index_uid: IndexUid) -> LiveTailTap {
        let sender = self.get_or_create_sender(&index_uid);
        LiveTailTap { index_uid, sender }
    }

    /// Subscribes to the documents of all the indexing pipelines of `index_uid` running on this
    /// node, including the pipelines spawned after the subscription.
    pub fn subscribe(&self, index_uid: &IndexUid) -> broadcast::Receiver<Arc<TailBatch>> {
        self.get_or_create_sender(index_uid).subscribe()
    }

    fn get_or_create_sender(&self, index_uid: &IndexUid) -> broadcast::Sender<Arc<TailBatch>> {
        let mut senders = self.senders.lock().expect("lock should not be poisoned");
        senders
            .entry(index_uid.clone())
            .or_insert_with(|| broadcast::channel(self.channel_capacity).0)
            .clone()
    }
}

/// Handle held by an indexing pipeline to publish its documents to the live tail sessions.
#[derive(Clone)]
pub struct LiveTailTap {
    index_uid: IndexUid,
    sender: broadcast::Sender<Arc<TailBatch>>,
}

impl LiveTailTap {
    /// Returns whether at least one live tail session is subscribed to this index.
    ///
    /// The pipeline should call this once per batch and skip collecting documents when it
    /// returns `false`, so that live tail costs nothing when nobody is watching.
    pub fn is_active(&self) -> bool {
        self.sender.receiver_count() > 0
    }

    /// Publishes a batch of documents. Never blocks.
    pub fn publish(&self, doc_mapper: Arc<DocMapper>, docs: Vec<TailDoc>) {
        if docs.is_empty() {
            return;
        }
        let num_docs = docs.len() as u64;
        debug!(index_uid=%self.index_uid, num_docs, "publishing live tail batch");
        let tail_batch = TailBatch { doc_mapper, docs };
        // An error means that all the subscribers left since `is_active` was called.
        if self.sender.send(Arc::new(tail_batch)).is_ok() {
            LIVE_TAIL_DOCS_PUBLISHED.inc_by(num_docs);
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use serde_json::json;
    use tokio::sync::broadcast::error::RecvError;

    use super::*;

    fn publish(tap: &LiveTailTap, docs: Vec<TailDoc>) {
        tap.publish(Arc::new(default_doc_mapper_for_test()), docs);
    }

    fn tail_doc(value: serde_json::Value) -> TailDoc {
        let serde_json::Value::Object(json_obj) = value else {
            panic!("expected a JSON object");
        };
        TailDoc {
            json_obj,
            num_bytes: 0,
        }
    }

    #[tokio::test]
    async fn test_live_tail_hub_tap_is_active_only_with_subscribers() {
        let hub = LiveTailHub::default();
        let index_uid = IndexUid::for_test("test-index", 0);
        let tap = hub.tap(index_uid.clone());
        assert!(!tap.is_active());

        // Publishing without subscriber is a no-op.
        publish(&tap, vec![tail_doc(json!({"body": "lost"}))]);

        let mut receiver = hub.subscribe(&index_uid);
        assert!(tap.is_active());

        publish(&tap, vec![tail_doc(json!({"body": "hello"}))]);
        let tail_batch = receiver.recv().await.unwrap();
        assert_eq!(tail_batch.docs, vec![tail_doc(json!({"body": "hello"}))]);

        drop(receiver);
        assert!(!tap.is_active());
    }

    #[tokio::test]
    async fn test_live_tail_hub_isolates_indexes() {
        let hub = LiveTailHub::default();
        let index_uid_a = IndexUid::for_test("test-index-a", 0);
        let index_uid_b = IndexUid::for_test("test-index-b", 0);
        let tap_a = hub.tap(index_uid_a.clone());
        let tap_b = hub.tap(index_uid_b.clone());

        let mut receiver_a = hub.subscribe(&index_uid_a);
        assert!(tap_a.is_active());
        assert!(!tap_b.is_active());

        publish(&tap_b, vec![tail_doc(json!({"body": "b"}))]);
        publish(&tap_a, vec![tail_doc(json!({"body": "a"}))]);

        let tail_batch = receiver_a.recv().await.unwrap();
        assert_eq!(tail_batch.docs, vec![tail_doc(json!({"body": "a"}))]);
        assert!(receiver_a.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_live_tail_hub_slow_subscriber_lags() {
        let hub = LiveTailHub::with_channel_capacity(2);
        let index_uid = IndexUid::for_test("test-index", 0);
        let tap = hub.tap(index_uid.clone());
        let mut receiver = hub.subscribe(&index_uid);

        for i in 0..5 {
            publish(&tap, vec![tail_doc(json!({"i": i}))]);
        }
        let RecvError::Lagged(num_lagged_batches) = receiver.recv().await.unwrap_err() else {
            panic!("expected lagged error");
        };
        assert_eq!(num_lagged_batches, 3);

        let tail_batch = receiver.recv().await.unwrap();
        assert_eq!(tail_batch.docs, vec![tail_doc(json!({"i": 3}))]);
    }
}
