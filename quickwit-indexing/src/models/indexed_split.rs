// Copyright (C) 2021 Quickwit, Inc.
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
use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Instant;

use quickwit_actors::{KillSwitch, Progress};
use quickwit_config::IndexingResources;
use quickwit_metastore::checkpoint::CheckpointDelta;
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::IndexBuilder;
use tokio::sync::SemaphorePermit;

use crate::controlled_directory::ControlledDirectory;
use crate::models::ScratchDirectory;
use crate::new_split_id;

pub struct IndexedSplit {
    pub index_id: String,
    pub split_id: String,
    pub replaced_split_ids: Vec<String>,

    pub time_range: Option<RangeInclusive<i64>>,

    /// Number of valid documents in the split.
    pub num_docs: u64,

    // Sum of the size of the document that were sent to the indexed.
    // This includes both documents that are valid or documents that are
    // invalid.
    pub docs_size_in_bytes: u64,

    /// Instant of reception of the first document in the indexer.
    ///
    /// This is mostly useful to understand part of the time to search.
    /// However, note that the document may have been waiting for a long time in the source
    /// before actually reaching the indexer.
    pub split_date_of_birth: Instant,

    /// Number of demux operations this split has undergone.
    pub demux_num_ops: usize,

    pub checkpoint_delta: CheckpointDelta,

    pub index: tantivy::Index,
    pub index_writer: tantivy::IndexWriter,
    pub split_scratch_directory: ScratchDirectory,

    pub controlled_directory_opt: Option<ControlledDirectory>,
    pub permit: SemaphorePermit<'static>,
}

impl fmt::Debug for IndexedSplit {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("IndexedSplit")
            .field("id", &self.split_id)
            .field("dir", &self.split_scratch_directory.path())
            .field("num_docs", &self.num_docs)
            .finish()
    }
}

impl IndexedSplit {
    pub fn new_in_dir(
        index_id: String,
        scratch_directory: ScratchDirectory,
        indexing_resources: IndexingResources,
        index_builder: IndexBuilder,
        progress: Progress,
        kill_switch: KillSwitch,
        permit: SemaphorePermit<'static>,
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
        let index = index_builder.open_or_create(controlled_directory.clone())?;
        let index_writer = index.writer_with_num_threads(
            indexing_resources.num_threads,
            indexing_resources.heap_size.get_bytes() as usize,
        )?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        Ok(IndexedSplit {
            index_id,
            split_id,
            replaced_split_ids: Vec::new(),
            time_range: None,
            demux_num_ops: 0,
            docs_size_in_bytes: 0,
            num_docs: 0,
            split_date_of_birth: Instant::now(),
            index,
            index_writer,
            split_scratch_directory,
            checkpoint_delta: CheckpointDelta::default(),
            controlled_directory_opt: Some(controlled_directory),
            permit,
        })
    }

    pub fn path(&self) -> &Path {
        self.split_scratch_directory.path()
    }
}

#[derive(Debug)]
pub struct IndexedSplitBatch {
    pub splits: Vec<IndexedSplit>,
}
