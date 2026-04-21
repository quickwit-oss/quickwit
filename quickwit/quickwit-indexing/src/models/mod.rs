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

#![allow(rustdoc::invalid_html_tags)]

mod indexed_split;
mod indexing_service_message;
mod indexing_statistics;
mod merge_planner_message;
mod merge_scratch;
mod merge_statistics;
mod packaged_split;
mod processed_doc;
mod publish_lock;
mod publisher_message;
mod raw_doc_batch;
mod shard_positions;
mod split_attrs;

pub use indexed_split::{
    CommitTrigger, EmptySplit, IndexedSplit, IndexedSplitBatch, IndexedSplitBatchBuilder,
    IndexedSplitBuilder,
};
pub use indexing_service_message::{
    DetachIndexingPipeline, DetachMergePipeline, ObservePipeline, SpawnPipeline,
};
pub use indexing_statistics::IndexingStatistics;
pub use merge_planner_message::NewSplits;
pub use merge_scratch::MergeScratch;
pub use merge_statistics::MergeStatistics;
pub use packaged_split::{PackagedSplit, PackagedSplitBatch};
pub use processed_doc::{ProcessedDoc, ProcessedDocBatch};
pub use publish_lock::{NewPublishLock, PublishLock};
pub use publisher_message::SplitsUpdate;
use quickwit_proto::types::PublishToken;
pub use raw_doc_batch::RawDocBatch;
pub(crate) use shard_positions::LocalShardPositionsUpdate;
pub use shard_positions::ShardPositionsService;
pub use split_attrs::{SplitAttrs, create_split_metadata};

#[derive(Debug)]
pub struct NewPublishToken(pub PublishToken);
