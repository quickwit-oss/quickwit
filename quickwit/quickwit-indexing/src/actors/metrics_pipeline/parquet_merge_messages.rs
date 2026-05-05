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

//! Message types for the Parquet merge pipeline.
//!
//! These messages flow through the actor chain:
//!
//! ```text
//! ParquetMergePlanner ──► MergeSchedulerService ──► ParquetMergeSplitDownloader
//!                                                         │
//!                                                         ▼ (ParquetMergeScratch)
//!                                                   ParquetMergeExecutor
//! ```

use std::fmt;
use std::path::PathBuf;

use quickwit_common::temp_dir::TempDirectory;
use quickwit_parquet_engine::merge::policy::ParquetMergeOperation;
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use tantivy::TrackedObject;

use crate::actors::MergePermit;

/// Notification of newly created Parquet splits.
///
/// Sent to `ParquetMergePlanner` from:
/// - The publisher (feedback loop after a merge completes)
/// - The indexing service (initial seeding of immature splits on start)
#[derive(Debug)]
pub struct ParquetNewSplits {
    pub new_splits: Vec<ParquetSplitMetadata>,
}

/// A merge task dispatched by `MergeSchedulerService` to `ParquetMergeSplitDownloader`.
///
/// Carries the merge operation (tracked by the planner's inventory) and a
/// concurrency permit from the global merge semaphore.
pub struct ParquetMergeTask {
    pub merge_operation: TrackedObject<ParquetMergeOperation>,
    pub merge_permit: MergePermit,
}

impl fmt::Debug for ParquetMergeTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetMergeTask")
            .field("merge_split_id", &self.merge_operation.merge_split_id)
            .field("num_inputs", &self.merge_operation.splits.len())
            .finish()
    }
}

/// Downloaded Parquet files ready for merge execution.
///
/// Sent from `ParquetMergeSplitDownloader` to `ParquetMergeExecutor` after
/// all input files have been downloaded to local storage.
pub struct ParquetMergeScratch {
    /// The merge operation describing what to merge.
    pub merge_operation: TrackedObject<ParquetMergeOperation>,

    /// Local paths to the downloaded Parquet files, one per input split,
    /// in the same order as `merge_operation.splits`.
    pub downloaded_parquet_files: Vec<PathBuf>,

    /// Temp directory containing the downloaded files. Held to prevent cleanup
    /// until the merge executor is done with the files.
    pub scratch_directory: TempDirectory,

    /// Concurrency permit — held until the merge completes (including upload).
    pub merge_permit: MergePermit,
}

impl fmt::Debug for ParquetMergeScratch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetMergeScratch")
            .field("merge_split_id", &self.merge_operation.merge_split_id)
            .field("num_files", &self.downloaded_parquet_files.len())
            .finish()
    }
}
