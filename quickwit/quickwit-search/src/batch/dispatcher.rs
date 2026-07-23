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

//! Async batching runtime: collects requests, dispatches batches.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use quickwit_metrics::{LazyCounter, LazyHistogram, lazy_counter, lazy_histogram};
use quickwit_proto::search::{SearchRequest, SearchResponse};
use tokio::sync::{mpsc, oneshot};
use tracing::*;

use super::combine::{build_combined_request, unbatch_response};
use crate::SearchError;
use crate::service::SearchService;

/// Maximum number of requests combined into one batch.
pub(super) const MAX_REQUESTS_PER_BATCH: usize = 10;

/// Distribution of batch sizes (requests per dispatch).
/// avg > 1 means batching is effective. count = number of dispatches.
/// sum = total requests processed.
static BATCH_SIZE: LazyHistogram = lazy_histogram!(
    name: "batch_size",
    description: "number of requests per batch dispatch",
    subsystem: "quickwit_search",
    buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 16.0],
);

/// Batches that fell back to individual calls due to incompatible requests.
static BATCH_FALLBACKS_TOTAL: LazyCounter = lazy_counter!(
    name: "batch_fallbacks_total",
    description: "batches that fell back to individual root_search calls",
    subsystem: "quickwit_search",
);

pub(super) struct BatchEntry {
    pub(super) request: SearchRequest,
    pub(super) result_tx: oneshot::Sender<crate::Result<SearchResponse>>,
    pub(super) batch_key: u64,
    pub(super) span: tracing::Span,
}

pub(super) async fn batch_dispatcher(
    search_service: Arc<dyn SearchService>,
    mut batch_rx: mpsc::UnboundedReceiver<BatchEntry>,
    window: Duration,
) {
    // map from grouping key → (PendingBatch of entries, timer deadline)
    let mut pending: HashMap<u64, (Vec<BatchEntry>, tokio::time::Instant)> = HashMap::new();

    loop {
        // compute the next deadline across all pending batches
        let next_deadline = pending.values().map(|(_, deadline)| *deadline).min();

        let sleep_until = next_deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600));

        tokio::select! {
            entry = batch_rx.recv() => {
                let Some(entry) = entry else {
                    // channel closed, dispatch remaining batches and exit
                    for (_, (entries, _)) in pending.drain() {
                        dispatch_batch(search_service.as_ref(), entries).await;
                    }
                    return;
                };

                let key = entry.batch_key;
                if let Some((mut batch_entries, deadline)) = pending.remove(&key) {
                    // Optimistic: the hash covers the common mismatch cases, and
                    // batch_execute falls back to individual calls if combining fails.
                    batch_entries.push(entry);
                    debug!(batch_key = key, batch_size = batch_entries.len(), "appending request to pending batch");

                    if batch_entries.len() >= MAX_REQUESTS_PER_BATCH {
                        debug!(
                            batch_key = key,
                            batch_size = batch_entries.len(),
                            "dispatching batch over request count limit"
                        );
                        let svc = search_service.clone();
                        tokio::spawn(async move {
                            dispatch_batch(svc.as_ref(), batch_entries).await;
                        });
                    } else {
                        pending.insert(key, (batch_entries, deadline));
                    }
                } else {
                    // new batch
                    debug!(batch_key = key, "starting new batch");
                    let deadline = tokio::time::Instant::now() + window;
                    pending.insert(key, (vec![entry], deadline));
                }
            }
            _ = tokio::time::sleep_until(sleep_until) => {
                // find and dispatch all expired batches
                let now = tokio::time::Instant::now();
                for (key, (entries, _)) in pending.extract_if(|_, (_, deadline)| *deadline <= now) {
                    debug!(batch_key = key, batch_size = entries.len(), "dispatching expired batch");
                    let svc = search_service.clone();
                    tokio::spawn(async move {
                        dispatch_batch(svc.as_ref(), entries).await;
                    });
                }
            }
        }
    }
}

pub(super) async fn dispatch_batch(
    search_service: &dyn SearchService,
    mut entries: Vec<BatchEntry>,
) {
    use tracing::Instrument;
    let batch_size = entries.len();
    let dispatch_span = tracing::info_span!("batch_dispatch", batch_size);
    for entry in &entries {
        dispatch_span.follows_from(entry.span.id());
    }

    // determinist sort so the combined aggregation is the same regardless of ordering (= better
    // partial cache usage)
    entries.sort_by_cached_key(|entry| entry.request.aggregation_request.clone());

    let (requests, result_txs): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .map(|e| (e.request, e.result_tx))
        .unzip();

    let results = batch_execute(search_service, requests)
        .instrument(dispatch_span)
        .await;

    for (tx, result) in result_txs.into_iter().zip(results) {
        let _ = tx.send(result);
    }
}

pub(super) async fn batch_execute(
    search_service: &dyn SearchService,
    requests: Vec<SearchRequest>,
) -> Vec<crate::Result<SearchResponse>> {
    if requests.is_empty() {
        return Vec::new();
    }
    let num_requests = requests.len();
    BATCH_SIZE.observe(num_requests as f64);

    if requests.len() == 1 {
        let result = search_service
            .root_search(requests.into_iter().next().unwrap())
            .await;
        return vec![result];
    }

    let Ok(combined_request) = build_combined_request(&requests) else {
        // Batching failed (e.g. hash collision, incompatible requests) — fall back to
        // individual root_search calls so no request is silently failed.
        debug!(
            num_requests,
            "batch combine failed, falling back to individual root_search calls"
        );
        BATCH_FALLBACKS_TOTAL.inc();
        let futures = requests
            .into_iter()
            .map(|req| search_service.root_search(req));
        return futures::future::join_all(futures).await;
    };

    match search_service.root_search(combined_request).await {
        Err(error) => {
            let error_msg = error.to_string();
            requests
                .into_iter()
                .map(|_| Err(SearchError::Internal(error_msg.clone())))
                .collect()
        }
        Ok(combined_response) => unbatch_response(combined_response, &requests),
    }
}
