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

use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::Schema;
use tempfile::TempDir;

const MEM_BUDGET_IN_BYTES: usize = 1_000_000_000;

pub struct IndexedSplit {
    pub split_id: String,
    pub index_id: String,
    pub time_range: Option<RangeInclusive<i64>>,

    pub num_docs: u64,
    pub size_in_bytes: u64,
    pub start_time: Instant,

    pub index: tantivy::Index,
    pub index_writer: tantivy::IndexWriter,
    pub temp_dir: TempDir,
}

fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}

impl IndexedSplit {
    pub fn new_in_dir(index_id: String, root_path: &Path, schema: Schema) -> anyhow::Result<Self> {
        // TODO make mem budget configurable.
        let index = tantivy::Index::create_in_dir(root_path, schema)?;
        let index_writer = index.writer_with_num_threads(1, MEM_BUDGET_IN_BYTES)?;
        // We avoid intermediary merge, and instead merge all segments in the packager.
        // The benefit is that we don't have ot wait for potentially existing merges,
        // and avoid possible race conditions.
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        let temp_dir = tempfile::tempdir_in(root_path)?;
        let split_id = new_split_id();
        Ok(IndexedSplit {
            split_id,
            index_id,
            time_range: None,
            size_in_bytes: 0,
            num_docs: 0,
            start_time: Instant::now(),

            index,
            index_writer,
            temp_dir,
        })
    }
}

impl fmt::Debug for IndexedSplit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IndexedSplit(id={}, path={})",
            self.split_id,
            self.temp_dir.path().display()
        )
    }
}
