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

mod indexing_service;
pub(crate) mod log_pipeline;
pub(crate) mod metrics_pipeline;
pub(crate) mod pipeline_shared;
mod publisher;
mod sequencer;
mod uploader;

pub use indexing_service::{INDEXING_DIR_NAME, IndexingService, IndexingServiceCounters};
pub use log_pipeline::{
    DocProcessor, DocProcessorCounters, FinishPendingMergesAndShutdownPipeline, IndexSerializer,
    Indexer, IndexerCounters, IndexingPipeline, IndexingPipelineParams, MergeExecutor,
    MergePipeline, MergeSchedulerService, MergeSplitDownloader, Packager, combine_partition_ids,
    merge_split_attrs, schedule_merge,
};
pub(crate) use log_pipeline::{MergePlanner, RunFinalizeMergePolicyAndQuit};
pub use log_pipeline::MergePermit;
pub use metrics_pipeline::{
    MetricsPipeline, MetricsPipelineParams, ParquetDocProcessor, ParquetDocProcessorCounters,
    ParquetDocProcessorError, ParquetIndexer, ParquetIndexerCounters, ParquetPackager,
    ParquetSplitBatch, ParquetUploader, is_arrow_ipc,
};
pub use publisher::{Publisher, PublisherCounters, PublisherType};
pub use quickwit_proto::indexing::IndexingError;
pub use sequencer::Sequencer;
pub use uploader::{SplitsUpdateMailbox, Uploader, UploaderCounters, UploaderType};
