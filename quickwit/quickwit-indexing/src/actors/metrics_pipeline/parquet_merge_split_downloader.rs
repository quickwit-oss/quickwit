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

//! Stub actor for downloading Parquet files prior to merge.
//!
//! The full implementation (PR 3c) will download each input split's Parquet
//! file from object storage to a local temp directory, then forward a
//! `ParquetMergeScratch` to the `ParquetMergeExecutor`.
//!
//! This stub exists so that `MergeSchedulerService` can reference the
//! `Mailbox<ParquetMergeSplitDownloader>` type and the `ParquetMergePlanner`
//! can be tested end-to-end through the scheduler.

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, QueueCapacity};
use tracing::debug;

use super::ParquetMergeTask;

/// Downloads Parquet split files from object storage for merge.
///
/// Stub implementation — accepts `ParquetMergeTask` messages but does not
/// perform real downloads. The full implementation comes in PR 3c.
pub struct ParquetMergeSplitDownloader;

#[async_trait]
impl Actor for ParquetMergeSplitDownloader {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn name(&self) -> String {
        "ParquetMergeSplitDownloader".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Unbounded
    }
}

#[async_trait]
impl Handler<ParquetMergeTask> for ParquetMergeSplitDownloader {
    type Reply = ();

    async fn handle(
        &mut self,
        task: ParquetMergeTask,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        debug!(
            merge_split_id = %task.merge_operation.merge_split_id,
            num_inputs = task.merge_operation.splits.len(),
            "received parquet merge task (stub — real download in PR 3c)"
        );
        Ok(())
    }
}
