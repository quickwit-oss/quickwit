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

use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use bytes::Bytes;
use quickwit_common::thread_pool::with_priority::ThreadPoolWithPriority;
use quickwit_proto::live_tail::{
    LiveTailDropReason, LiveTailDropped, LiveTailError, LiveTailHits, LiveTailMessage,
    LiveTailResult, live_tail_message,
};
use quickwit_query::query_ast::QueryAst;
use tokio::sync::{OwnedSemaphorePermit, broadcast, mpsc};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, warn};

use crate::filter::{FilteredDocs, filter_batches};
use crate::hub::TailBatch;
use crate::rate_limiter::DocRateLimiter;

/// Interval at which the received documents are filtered and sent.
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// Maximum number of documents accumulated before filtering early.
const MAX_PENDING_DOCS: usize = 10_000;

/// Target size of the NDJSON payload of a `LiveTailHits` message.
const TARGET_HITS_PAYLOAD_NUM_BYTES: usize = 1024 * 1024;

/// Upper bound of the size of a `LiveTailMessage` minus the size of its NDJSON payload.
const HITS_MESSAGE_OVERHEAD_NUM_BYTES: usize = 1024;

/// Returns the maximum size of the NDJSON payload of a `LiveTailHits` message, so that the
/// encoded message fits in a gRPC message of at most `grpc_max_message_num_bytes`.
pub(crate) fn max_hits_payload_num_bytes(grpc_max_message_num_bytes: usize) -> usize {
    grpc_max_message_num_bytes
        .saturating_sub(HITS_MESSAGE_OVERHEAD_NUM_BYTES)
        .min(TARGET_HITS_PAYLOAD_NUM_BYTES)
}

const NUM_FILTER_THREADS: usize = 2;

/// Filtering runs on a small dedicated thread pool rather than with `run_cpu_intensive`, so that
/// live tail sessions can never starve the CPU-intensive tasks of the ingest and search paths.
/// When the pool does not keep up, the sessions wait for their filtering, their subscription to
/// the hub lags, and the documents they miss are reported as `lagged`.
fn filter_thread_pool() -> &'static ThreadPoolWithPriority {
    static FILTER_THREAD_POOL: LazyLock<ThreadPoolWithPriority> =
        LazyLock::new(|| ThreadPoolWithPriority::new("live_tail", Some(NUM_FILTER_THREADS)));
    &FILTER_THREAD_POOL
}

pub(crate) struct LiveTailSession {
    session_id: String,
    query_ast: Arc<QueryAst>,
    batch_rx: broadcast::Receiver<Arc<TailBatch>>,
    message_tx: mpsc::Sender<LiveTailResult<LiveTailMessage>>,
    /// Shared with the other sessions opened on this node by the same client session.
    rate_limiter: Arc<Mutex<DocRateLimiter>>,
    /// Documents larger than this are dropped: they do not fit in a gRPC message.
    max_hits_payload_num_bytes: usize,
    pending_batches: Vec<Arc<TailBatch>>,
    num_pending_docs: usize,
    num_lagged_batches: u64,
    /// Released when the session ends.
    _session_permit: OwnedSemaphorePermit,
}

impl LiveTailSession {
    pub fn new(
        session_id: String,
        query_ast: QueryAst,
        batch_rx: broadcast::Receiver<Arc<TailBatch>>,
        message_tx: mpsc::Sender<LiveTailResult<LiveTailMessage>>,
        rate_limiter: Arc<Mutex<DocRateLimiter>>,
        max_hits_payload_num_bytes: usize,
        session_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            session_id,
            query_ast: Arc::new(query_ast),
            batch_rx,
            message_tx,
            rate_limiter,
            max_hits_payload_num_bytes,
            pending_batches: Vec::new(),
            num_pending_docs: 0,
            num_lagged_batches: 0,
            _session_permit: session_permit,
        }
    }

    /// Runs the session until the client drops the stream or an internal error occurs.
    pub async fn run(mut self) {
        debug!(session_id=%self.session_id, "live tail session started");
        let mut flush_interval = tokio::time::interval(FLUSH_INTERVAL);
        flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = self.message_tx.closed() => break,
                recv_result = self.batch_rx.recv() => {
                    match recv_result {
                        Ok(tail_batch) => {
                            self.num_pending_docs += tail_batch.num_docs();
                            self.pending_batches.push(tail_batch);
                        }
                        Err(broadcast::error::RecvError::Lagged(num_lagged_batches)) => {
                            self.num_lagged_batches += num_lagged_batches;
                        }
                        // The hub only drops the senders of the channels without receivers, so this
                        // only happens on shutdown.
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                    if self.num_pending_docs < MAX_PENDING_DOCS {
                        continue;
                    }
                }
                _ = flush_interval.tick() => {}
            }
            if let Err(error) = self.flush().await {
                warn!(session_id=%self.session_id, %error, "live tail session failed");
                let _ = self.message_tx.send(Err(error)).await;
                break;
            }
            if self.message_tx.is_closed() {
                break;
            }
        }
        debug!(session_id=%self.session_id, "live tail session ended");
    }

    async fn flush(&mut self) -> LiveTailResult<()> {
        if self.num_lagged_batches > 0 {
            let dropped = LiveTailDropped {
                reason: LiveTailDropReason::Lagged as i32,
                num_dropped: std::mem::take(&mut self.num_lagged_batches),
            };
            self.send(live_tail_message::Message::Dropped(dropped))
                .await;
        }
        if !self.pending_batches.is_empty() {
            let pending_batches = std::mem::take(&mut self.pending_batches);
            self.num_pending_docs = 0;
            self.filter_and_send(pending_batches).await?;
        }
        Ok(())
    }

    async fn filter_and_send(&mut self, tail_batches: Vec<Arc<TailBatch>>) -> LiveTailResult<()> {
        let query_ast = self.query_ast.clone();
        let num_available_docs = self
            .rate_limiter
            .lock()
            .expect("lock should not be poisoned")
            .num_available_docs(Instant::now());
        let FilteredDocs {
            num_matching_docs,
            docs_json,
        } = filter_thread_pool()
            .run_cpu_intensive(move || {
                filter_batches(&query_ast, &tail_batches, num_available_docs)
            })
            .await
            .map_err(|_| LiveTailError::Internal("live tail filter panicked".to_string()))?
            .map_err(|error| {
                LiveTailError::Internal(format!("failed to filter documents: {error:#}"))
            })?;
        let num_allowed_docs = docs_json.len();
        self.rate_limiter
            .lock()
            .expect("lock should not be poisoned")
            .consume(num_allowed_docs);
        debug!(session_id=%self.session_id, num_matching_docs, num_allowed_docs, "filtered live tail documents");

        let mut docs_ndjson: Vec<u8> = Vec::new();
        let mut num_too_large_docs: u64 = 0;

        for doc_json in docs_json {
            let doc_ndjson_num_bytes = doc_json.len() + 1;

            if doc_ndjson_num_bytes > self.max_hits_payload_num_bytes {
                num_too_large_docs += 1;
                continue;
            }
            if docs_ndjson.len() + doc_ndjson_num_bytes > self.max_hits_payload_num_bytes {
                self.send_hits(std::mem::take(&mut docs_ndjson)).await;
            }
            docs_ndjson.extend_from_slice(&doc_json);
            docs_ndjson.push(b'\n');
        }
        if !docs_ndjson.is_empty() {
            self.send_hits(docs_ndjson).await;
        }
        self.send_dropped(LiveTailDropReason::TooLarge, num_too_large_docs)
            .await;
        let num_rate_limited_docs = (num_matching_docs - num_allowed_docs) as u64;
        self.send_dropped(LiveTailDropReason::RateLimited, num_rate_limited_docs)
            .await;
        Ok(())
    }

    async fn send_dropped(&mut self, reason: LiveTailDropReason, num_dropped: u64) {
        if num_dropped == 0 {
            return;
        }
        let dropped = LiveTailDropped {
            reason: reason as i32,
            num_dropped,
        };
        self.send(live_tail_message::Message::Dropped(dropped))
            .await;
    }

    async fn send_hits(&mut self, docs_ndjson: Vec<u8>) {
        let hits = LiveTailHits {
            docs_ndjson: Bytes::from(docs_ndjson),
        };
        self.send(live_tail_message::Message::Hits(hits)).await;
    }

    /// Sends a message to the client. If the client is gone, the message is dropped and the
    /// session ends at the next iteration of the run loop.
    async fn send(&mut self, message: live_tail_message::Message) {
        let live_tail_message = LiveTailMessage {
            message: Some(message),
        };
        let _ = self.message_tx.send(Ok(live_tail_message)).await;
    }
}
