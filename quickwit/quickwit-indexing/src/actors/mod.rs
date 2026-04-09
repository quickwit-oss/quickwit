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

mod cooperative_indexing;
mod doc_processor;
mod index_serializer;
mod indexer;
mod indexing_pipeline;
mod indexing_service;
mod log_publisher_impl;
mod merge_executor;
pub(crate) mod merge_pipeline;
mod merge_planner;
mod merge_scheduler_service;
mod merge_split_downloader;
#[cfg(feature = "metrics")]
pub(crate) mod metrics_pipeline;
mod packager;
pub(crate) mod pipeline_shared;
mod publisher;
mod sequencer;
mod uploader;
#[cfg(feature = "vrl")]
mod vrl_processing;

pub use doc_processor::{DocProcessor, DocProcessorCounters};
pub use index_serializer::IndexSerializer;
pub use indexer::{Indexer, IndexerCounters};
pub use indexing_pipeline::{IndexingPipeline, IndexingPipelineParams};
pub use indexing_service::{
    BoxPipelineHandle, INDEXING_DIR_NAME, IndexingService, IndexingServiceCounters,
};
pub(crate) use log_publisher_impl::{MERGE_PUBLISHER_NAME, PUBLISHER_NAME};
pub use merge_executor::{MergeExecutor, combine_partition_ids, merge_split_attrs};
pub use merge_pipeline::{
    FinishPendingMergesAndShutdownPipeline, MergePipeline, MergePipelineParams,
};
pub(crate) use merge_planner::MergePlanner;
#[cfg(test)]
pub(crate) use merge_planner::RunFinalizeMergePolicyAndQuit;
pub use merge_scheduler_service::{MergePermit, MergeSchedulerService, schedule_merge};
pub use merge_split_downloader::MergeSplitDownloader;
#[cfg(feature = "metrics")]
pub use metrics_pipeline::*;
pub use packager::Packager;
pub use publisher::{Publisher, PublisherCounters};
pub use quickwit_proto::indexing::IndexingError;
pub use sequencer::Sequencer;
pub use uploader::{SplitsUpdateMailbox, Uploader, UploaderCounters, UploaderType};
