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

//! Metrics indexing pipeline actors.
//!
//! This module contains the Parquet/DataFusion-based pipeline for time-series
//! metrics data. The pipeline bypasses Tantivy and instead produces Parquet
//! split files:
//!
//! ```text
//! Source → ParquetDocProcessor → ParquetIndexer → ParquetPackager → ParquetUploader → Publisher
//! ```

mod indexing_service_impl;
mod parquet_doc_processor;
mod parquet_indexer;
mod parquet_merge_executor;
pub(crate) mod parquet_merge_messages;
mod parquet_merge_pipeline;
mod parquet_merge_planner;
mod parquet_merge_split_downloader;
mod parquet_packager;
mod parquet_splits_update;
mod parquet_uploader;
mod pipeline;
mod processed_parquet_batch;
mod publisher_impl;

#[cfg(test)]
#[allow(
    clippy::disallowed_methods,
    clippy::needless_borrow,
    clippy::unnecessary_map_or
)]
mod parquet_e2e_test;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod parquet_merge_pipeline_test;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod parquet_merge_pipeline_crash_test;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod parquet_merge_pipeline_trace_conformance_test;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod parquet_merge_pipeline_sketch_test;

pub use parquet_doc_processor::{
    ParquetDocProcessor, ParquetDocProcessorCounters, ParquetDocProcessorError, is_arrow_ipc,
};
pub use parquet_indexer::{ParquetIndexer, ParquetIndexerCounters, ParquetSplitBatch};
pub use parquet_merge_executor::ParquetMergeExecutor;
pub use parquet_merge_messages::{ParquetMergeScratch, ParquetMergeTask, ParquetNewSplits};
pub use parquet_merge_pipeline::{ParquetMergePipeline, ParquetMergePipelineParams};
pub use parquet_merge_planner::ParquetMergePlanner;
pub use parquet_merge_split_downloader::ParquetMergeSplitDownloader;
pub use parquet_packager::{ParquetBatchForPackager, ParquetPackager, ParquetPackagerCounters};
pub use parquet_splits_update::ParquetSplitsUpdate;
pub use parquet_uploader::ParquetUploader;
pub use pipeline::{MetricsPipeline, MetricsPipelineParams};
pub use processed_parquet_batch::ProcessedParquetBatch;
pub(crate) use publisher_impl::METRICS_PUBLISHER_NAME;

#[cfg(test)]
/// Spawn a `Sequencer<Publisher>` in front of the given publisher mailbox.
pub(crate) fn spawn_sequencer_for_test(
    universe: &quickwit_actors::Universe,
    publisher_mailbox: quickwit_actors::Mailbox<crate::actors::Publisher>,
) -> quickwit_actors::Mailbox<crate::actors::Sequencer<crate::actors::Publisher>> {
    let sequencer = crate::actors::Sequencer::new(publisher_mailbox);
    let (sequencer_mailbox, _sequencer_handle) = universe.spawn_builder().spawn(sequencer);
    sequencer_mailbox
}
