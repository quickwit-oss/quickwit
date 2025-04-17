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

use std::fmt;
use std::path::Path;

use quickwit_common::io::IoControls;
use quickwit_common::metrics::GaugeGuard;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_proto::types::{DocMappingUid, IndexUid, PublishToken};
use tantivy::IndexBuilder;
use tantivy::directory::MmapDirectory;
use tracing::{Span, instrument};

use crate::controlled_directory::ControlledDirectory;
use crate::merge_policy::MergeTask;
use crate::models::{PublishLock, SplitAttrs};
use crate::new_split_id;

pub struct IndexedSplitBuilder {
    pub split_attrs: SplitAttrs,
    pub index_writer: tantivy::SingleSegmentIndexWriter,
    pub split_scratch_directory: TempDirectory,
    pub controlled_directory_opt: Option<ControlledDirectory>,
}

pub struct IndexedSplit {
    pub split_attrs: SplitAttrs,
    pub index: tantivy::Index,
    pub split_scratch_directory: TempDirectory,
    pub controlled_directory_opt: Option<ControlledDirectory>,
}

impl IndexedSplit {
    pub fn split_id(&self) -> &str {
        &self.split_attrs.split_id
    }
}

impl fmt::Debug for IndexedSplit {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("IndexedSplit")
            .field("split_id", &self.split_attrs.split_id)
            .field("dir", &self.split_scratch_directory.path())
            .field("num_docs", &self.split_attrs.num_docs)
            .finish()
    }
}

impl fmt::Debug for IndexedSplitBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("IndexedSplitBuilder")
            .field("split_id", &self.split_attrs.split_id)
            .field("dir", &self.split_scratch_directory.path())
            .field("num_docs", &self.split_attrs.num_docs)
            .finish()
    }
}

impl IndexedSplitBuilder {
    pub fn new_in_dir(
        pipeline_id: IndexingPipelineId,
        partition_id: u64,
        last_delete_opstamp: u64,
        doc_mapping_uid: DocMappingUid,
        scratch_directory: TempDirectory,
        index_builder: IndexBuilder,
        io_controls: IoControls,
    ) -> anyhow::Result<Self> {
        // We avoid intermediary merge, and instead merge all segments in the packager.
        // The benefit is that we don't have to wait for potentially existing merges,
        // and avoid possible race conditions.
        let split_id = new_split_id();
        let split_scratch_directory_prefix = format!("split-{split_id}-");
        let split_scratch_directory =
            scratch_directory.named_temp_child(&split_scratch_directory_prefix)?;
        let mmap_directory = MmapDirectory::open(split_scratch_directory.path())?;
        let box_mmap_directory = Box::new(mmap_directory);

        let controlled_directory = ControlledDirectory::new(box_mmap_directory, io_controls);

        let index_writer =
            index_builder.single_segment_index_writer(controlled_directory.clone(), 15_000_000)?;
        Ok(Self {
            split_attrs: SplitAttrs {
                node_id: pipeline_id.node_id,
                index_uid: pipeline_id.index_uid,
                source_id: pipeline_id.source_id,
                doc_mapping_uid,
                partition_id,
                split_id,
                num_docs: 0,
                replaced_split_ids: Vec::new(),
                uncompressed_docs_size_in_bytes: 0,
                time_range: None,
                delete_opstamp: last_delete_opstamp,
                num_merge_ops: 0,
            },
            index_writer,
            split_scratch_directory,
            controlled_directory_opt: Some(controlled_directory),
        })
    }

    #[instrument(name="serialize_split",
        skip_all,
        fields(
            node_id=%self.split_attrs.node_id,
            index_uid=%self.split_attrs.index_uid,
            source_id=%self.split_attrs.source_id,
            split_id=%self.split_attrs.split_id,
            partition_id=%self.split_attrs.partition_id,
            num_docs=%self.split_attrs.num_docs,
            uncompressed_docs_size_in_bytes=%self.split_attrs.uncompressed_docs_size_in_bytes,
            delete_opstamp=%self.split_attrs.delete_opstamp,
            num_merge_ops=%self.split_attrs.num_merge_ops,
        )
    )]
    pub fn finalize(self) -> anyhow::Result<IndexedSplit> {
        let index = self.index_writer.finalize()?;
        Ok(IndexedSplit {
            split_attrs: self.split_attrs,
            index,
            split_scratch_directory: self.split_scratch_directory,
            controlled_directory_opt: self.controlled_directory_opt,
        })
    }

    pub fn path(&self) -> &Path {
        self.split_scratch_directory.path()
    }

    pub fn split_id(&self) -> &str {
        &self.split_attrs.split_id
    }
}

#[derive(Debug)]
pub struct IndexedSplitBatch {
    pub splits: Vec<IndexedSplit>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub publish_token_opt: Option<PublishToken>,
    /// A [`MergeTask`] tracked by either the `MergePlanner` or the `DeleteTaskPlanner`
    /// in the `MergePipeline` or `DeleteTaskPipeline`.
    /// See planners docs to understand the usage.
    /// If `None`, the split batch was built in the `IndexingPipeline`.
    pub merge_task_opt: Option<MergeTask>,
    pub batch_parent_span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitTrigger {
    Drained,
    ForceCommit,
    MemoryLimit,
    NoMoreDocs,
    NumDocsLimit,
    Timeout,
}

#[derive(Debug)]
pub struct IndexedSplitBatchBuilder {
    pub splits: Vec<IndexedSplitBuilder>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub publish_token_opt: Option<PublishToken>,
    pub commit_trigger: CommitTrigger,
    pub batch_parent_span: Span,
    pub memory_usage: GaugeGuard<'static>,
    pub _split_builders_guard: GaugeGuard<'static>,
}

/// Sends notifications to the Publisher that the last batch of splits was empty.
#[derive(Debug)]
pub struct EmptySplit {
    pub index_uid: IndexUid,
    pub checkpoint_delta: IndexCheckpointDelta,
    pub publish_lock: PublishLock,
    pub publish_token_opt: Option<PublishToken>,
    pub batch_parent_span: Span,
}
