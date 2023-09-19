// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

mod doc_processor;
mod index_serializer;
mod indexer;
mod indexing_pipeline;
mod indexing_service;
mod merge_executor;
mod merge_pipeline;
mod merge_planner;
mod merge_split_downloader;
mod packager;
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
    IndexingService, IndexingServiceCounters, MergePipelineId, INDEXING_DIR_NAME,
};
pub use merge_executor::{combine_partition_ids, merge_split_attrs, MergeExecutor};
pub use merge_pipeline::MergePipeline;
pub use merge_planner::MergePlanner;
pub use merge_split_downloader::MergeSplitDownloader;
pub use packager::Packager;
pub use publisher::{Publisher, PublisherCounters, PublisherType};
pub use quickwit_proto::indexing::IndexingError;
pub use sequencer::Sequencer;
pub use uploader::{SplitsUpdateMailbox, Uploader, UploaderCounters, UploaderType};
