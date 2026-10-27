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
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use feuer::Download;
use futures::FutureExt;
use futures::future::{BoxFuture, Shared, WeakShared};

use crate::{StorageErrorKind, StorageResult};

const MAX_GAP: usize = 1024 * 1024;
const MAX_MERGED_READ: usize = 8 * 1024 * 1024;
const BATCH_WINDOW: Duration = Duration::from_millis(1);
const CLEANUP_INTERVAL: usize = 1024;

// Feuer stores complete objects; retain the split offset alongside the shared download.
type DownloadFuture = Shared<BoxFuture<'static, StorageResult<(Range<usize>, Download)>>>;

#[derive(Default)]
pub(super) struct DownloadCoalescer {
    pending: Mutex<Pending>,
}

#[derive(Default)]
struct Pending {
    objects: HashMap<String, Vec<Batch>>,
    inserts_since_cleanup: usize,
}

struct Batch {
    state: Arc<Mutex<BatchState>>,
    // Only callers own the download future. Dropping the last caller cancels it.
    future: WeakShared<BoxFuture<'static, StorageResult<(Range<usize>, Download)>>>,
}

struct BatchState {
    range: Range<usize>,
    started: bool,
}

impl DownloadCoalescer {
    /// The key must identify an immutable object; callbacks for the same key must be
    /// interchangeable and return exactly the supplied range (including any gaps).
    /// Only pending batches can grow. In-flight reads can only serve covered ranges.
    pub(super) fn get_or_create<F, Fut>(
        &self,
        object_uri: String,
        range: Range<usize>,
        fetch: F,
    ) -> DownloadFuture
    where
        F: FnOnce(Range<usize>) -> Fut + Send + 'static,
        Fut: Future<Output = StorageResult<Download>> + Send + 'static,
    {
        let mut pending = self.pending.lock().unwrap();
        // Reclaim weak entries left behind by completed or cancelled requests,
        // including objects that are never requested again.
        if pending.inserts_since_cleanup >= CLEANUP_INTERVAL {
            pending.objects.retain(|_, batches| {
                batches.retain(|batch| batch.future.upgrade().is_some());
                !batches.is_empty()
            });
            pending.inserts_since_cleanup = 0;
        }
        let batches = pending.objects.entry(object_uri).or_default();
        batches.retain(|batch| batch.future.upgrade().is_some());
        for batch in batches.iter() {
            let Some(future) = batch.future.upgrade() else {
                continue;
            };
            let mut state = batch.state.lock().unwrap();
            if state.range.start <= range.start && state.range.end >= range.end {
                return future;
            }
            let merged = state.range.start.min(range.start)..state.range.end.max(range.end);
            let gap = range
                .start
                .saturating_sub(state.range.end)
                .max(state.range.start.saturating_sub(range.end));
            if !state.started && gap <= MAX_GAP && merged.len() <= MAX_MERGED_READ {
                state.range = merged;
                return future;
            }
        }
        let state = Arc::new(Mutex::new(BatchState {
            range,
            started: false,
        }));
        let download_state = state.clone();
        let future = async move {
            tokio::time::sleep(BATCH_WINDOW).await;
            let range = {
                let mut state = download_state.lock().unwrap();
                state.started = true;
                state.range.clone()
            };
            let download = fetch(range.clone()).await?;
            if download.bytes().len() != range.len() {
                return Err(StorageErrorKind::Internal.with_error(anyhow::anyhow!(
                    "range download returned {} bytes for {range:?}",
                    download.bytes().len()
                )));
            }
            Ok((range, download))
        }
        .boxed()
        .shared();
        batches.push(Batch {
            state,
            future: future.downgrade().unwrap(),
        });
        pending.inserts_since_cleanup += 1;
        future
    }
}
