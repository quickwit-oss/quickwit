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
pub use split_attrs::{create_split_metadata, SplitAttrs};

#[derive(Debug)]
pub struct NewPublishToken(pub PublishToken);
