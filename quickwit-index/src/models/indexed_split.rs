// Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::fmt;
use std::ops::RangeInclusive;
use std::path::Path;
use std::time::Instant;

use quickwit_metastore::checkpoint::CheckpointDelta;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::{Field, Schema};

use crate::models::ScratchDirectory;

const MEM_BUDGET_IN_BYTES: usize = 1_000_000_000;

pub struct IndexedSplit {
    pub split_id: String,
    pub index_id: String,
    pub time_range: Option<RangeInclusive<i64>>,

    /// Number of valid documents in the split.
    pub num_docs: u64,

    // Sum of the size of the document that were sent to the indexed.
    // This includes both documents that are valid or documents that are
    // invalid.
    pub docs_size_in_bytes: u64,

    /// Instant of the instant of the first document in the split.
    pub start_time: Instant,

    pub checkpoint_delta: CheckpointDelta,

    pub index: tantivy::Index,
    pub index_writer: tantivy::IndexWriter,
    pub split_scratch_directory: ScratchDirectory,
    /// The special field for extracting tags.
    pub tags_field: Field,
}

impl fmt::Debug for IndexedSplit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexedSplit")
            .field("id", &self.split_id)
            .field("index", &self.index_id)
            .field("dir", &self.split_scratch_directory.path())
            .finish()
    }
}

fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}

impl IndexedSplit {
    pub fn new_in_dir(
        index_id: String,
        index_scratch_directory: &ScratchDirectory,
        schema: Schema,
        tags_field: Field,
    ) -> anyhow::Result<Self> {
        // We avoid intermediary merge, and instead merge all segments in the packager.
        // The benefit is that we don't have to wait for potentially existing merges,
        // and avoid possible race conditions.
        let split_scratch_directory = index_scratch_directory.temp_child()?;
        let index = tantivy::Index::create_in_dir(split_scratch_directory.path(), schema)?;
        // TODO make mem budget configurable.
        let index_writer = index.writer_with_num_threads(1, MEM_BUDGET_IN_BYTES)?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let split_id = new_split_id();
        Ok(IndexedSplit {
            split_id,
            index_id,
            time_range: None,
            docs_size_in_bytes: 0,
            num_docs: 0,
            start_time: Instant::now(),
            index,
            index_writer,
            split_scratch_directory,
            checkpoint_delta: CheckpointDelta::default(),
            tags_field,
        })
    }

    pub fn path(&self) -> &Path {
        self.split_scratch_directory.path()
    }
}
