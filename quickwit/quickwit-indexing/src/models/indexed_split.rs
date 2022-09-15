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

use std::fmt;
use std::path::Path;
use std::time::Instant;

use quickwit_actors::{KillSwitch, Progress};
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use tantivy::directory::MmapDirectory;
use tantivy::IndexBuilder;

use crate::controlled_directory::ControlledDirectory;
use crate::models::{IndexingPipelineId, PublishLock, ScratchDirectory, SplitAttrs};
use crate::new_split_id;

pub struct IndexedSplitBuilder {
    pub split_attrs: SplitAttrs,
    pub index_writer: tantivy::SingleSegmentIndexWriter,
    pub split_scratch_directory: ScratchDirectory,
    pub controlled_directory_opt: Option<ControlledDirectory>,
}

pub struct IndexedSplit {
    pub split_attrs: SplitAttrs,
    pub index: tantivy::Index,
    pub split_scratch_directory: ScratchDirectory,
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
            .field("id", &self.split_attrs.split_id)
            .field("dir", &self.split_scratch_directory.path())
            .field("num_docs", &self.split_attrs.num_docs)
            .finish()
    }
}

impl fmt::Debug for IndexedSplitBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("IndexedSplitBuilder")
            .field("id", &self.split_attrs.split_id)
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
        scratch_directory: ScratchDirectory,
        index_builder: IndexBuilder,
        progress: Progress,
        kill_switch: KillSwitch,
    ) -> anyhow::Result<Self> {
        // We avoid intermediary merge, and instead merge all segments in the packager.
        // The benefit is that we don't have to wait for potentially existing merges,
        // and avoid possible race conditions.
        let split_id = new_split_id();
        let split_scratch_directory_prefix = format!("split-{}-", split_id);
        let split_scratch_directory =
            scratch_directory.named_temp_child(split_scratch_directory_prefix)?;
        let mmap_directory = MmapDirectory::open(split_scratch_directory.path())?;
        let box_mmap_directory = Box::new(mmap_directory);
        let controlled_directory =
            ControlledDirectory::new(box_mmap_directory, progress, kill_switch);
        let index_writer =
            index_builder.single_segment_index_writer(controlled_directory.clone(), 10_000_000)?;
        Ok(Self {
            split_attrs: SplitAttrs {
                pipeline_id,
                partition_id,
                split_id,
                num_docs: 0,
                replaced_split_ids: Vec::new(),
                uncompressed_docs_size_in_bytes: 0,
                time_range: None,
                delete_opstamp: last_delete_opstamp,
            },
            index_writer,
            split_scratch_directory,
            controlled_directory_opt: Some(controlled_directory),
        })
    }

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
    pub checkpoint_delta: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub date_of_birth: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitTrigger {
    Timeout,
    NoMoreDocs,
    NumDocsLimit,
    MemoryLimit,
}

#[derive(Debug)]
pub struct IndexedSplitBatchBuilder {
    pub splits: Vec<IndexedSplitBuilder>,
    pub checkpoint_delta: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub date_of_birth: Instant,
    pub commit_trigger: CommitTrigger,
}
