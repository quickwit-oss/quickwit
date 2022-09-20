// Copyright (C) 2022 Quickwit, Inc.
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

mod indexed_split;
mod indexing_directory;
mod indexing_pipeline_id;
mod indexing_service_message;
mod indexing_statistics;
mod merge_planner_message;
mod merge_scratch;
mod merging_statistics;
mod packaged_split;
mod prepared_doc;
mod publish_lock;
mod publisher_message;
mod raw_doc_batch;
mod scratch_directory;
mod split_attrs;

pub use indexed_split::{
    CommitTrigger, IndexedSplit, IndexedSplitBatch, IndexedSplitBatchBuilder, IndexedSplitBuilder,
};
pub use indexing_directory::{IndexingDirectory, WeakIndexingDirectory, CACHE};
pub use indexing_pipeline_id::IndexingPipelineId;
pub use indexing_service_message::{
    DetachPipeline, ObservePipeline, ShutdownPipeline, ShutdownPipelines, SpawnMergePipeline,
    SpawnPipeline, SpawnPipelines,
};
pub use indexing_statistics::IndexingStatistics;
pub use merge_planner_message::NewSplits;
pub use merge_scratch::MergeScratch;
pub use merging_statistics::MergingStatistics;
pub use packaged_split::{PackagedSplit, PackagedSplitBatch};
pub use prepared_doc::{PreparedDoc, PreparedDocBatch};
pub use publish_lock::{NewPublishLock, PublishLock};
pub use publisher_message::SplitUpdate;
pub use raw_doc_batch::RawDocBatch;
pub use scratch_directory::ScratchDirectory;
pub use split_attrs::SplitAttrs;

#[derive(Clone, Copy, Debug)]
pub struct Observe;
