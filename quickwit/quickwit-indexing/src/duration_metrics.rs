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

//! Elapsed durations for indexing/upload permit waits, upload-result waits, and upload tasks.
//! `quickwit_indexing_operation_duration_seconds{actor,operation}` uses the actor name captured
//! at mailbox creation. Spawned upload tasks use their owning uploader's name.
//! Permit and result waits overlap actor work. Upload tasks run separately and can overlap the
//! sequencer's result wait. These durations are not additive or CPU/isolated I/O measurements.
//! Independently computed histogram quantiles must not be subtracted from each other.

use std::future::Future;

use quickwit_metrics::{LazyHistogram, histogram, lazy_histogram};
use tokio::time::Instant;

static INDEXING_OPERATION_DURATION_SECONDS: LazyHistogram = lazy_histogram!(
    name: "operation_duration_seconds",
    description: "Elapsed seconds for indexing operations, by owning actor and operation",
    subsystem: "indexing",
    buckets: vec![
        0.000_001, 0.000_01, 0.000_1, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0,
        10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0,
    ],
);

#[derive(Clone, Copy, Debug)]
pub(crate) enum IndexingOperation {
    /// Wait for an indexing permit and create its cooperative indexing period.
    IndexingPermitWait,
    /// Wait for an upload permit.
    UploadPermitWait,
    /// Wait for an upload result or discard command in the sequencer.
    UploadResultWait,
    /// Run the entire spawned upload task, including preparation, staging, storage, and sending.
    UploadTask,
}

impl IndexingOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::IndexingPermitWait => "indexing_permit_wait",
            Self::UploadPermitWait => "upload_permit_wait",
            Self::UploadResultWait => "upload_result_wait",
            Self::UploadTask => "upload_task",
        }
    }
}

/// Observe a future's elapsed duration only on completion, including errors.
/// Cancellation and panic do not produce samples. Callers control opt-in and must use the
/// owning actor's captured mailbox name, never an instance identifier.
pub(crate) async fn measure_future_duration<F: Future>(
    actor_name: &str,
    operation: IndexingOperation,
    future: F,
) -> F::Output {
    let started_at = Instant::now();
    let output = future.await;
    let duration = started_at.elapsed();
    histogram!(parent: INDEXING_OPERATION_DURATION_SECONDS, "actor" => actor_name.to_owned(), "operation" => operation.as_str())
        .observe(duration.as_secs_f64());
    output
}
