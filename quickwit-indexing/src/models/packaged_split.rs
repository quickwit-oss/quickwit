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

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::ops::RangeInclusive;
use std::time::Instant;

use quickwit_metastore::checkpoint::IndexCheckpointDelta;

use crate::models::{IndexingPipelineId, PublishLock, ScratchDirectory};

pub struct PackagedSplit {
    pub split_id: String,
    pub partition_id: u64,
    pub pipeline_id: IndexingPipelineId,

    pub replaced_split_ids: Vec<String>,
    pub time_range: Option<RangeInclusive<i64>>,
    pub size_in_bytes: u64,
    pub split_scratch_directory: ScratchDirectory,
    pub num_docs: u64,
    pub tags: BTreeSet<String>,
    pub split_files: Vec<std::path::PathBuf>,
    pub hotcache_bytes: Vec<u8>,
}

impl fmt::Debug for PackagedSplit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PackagedSplit")
            .field("split_id", &self.split_id)
            .field("partition_id", &self.partition_id)
            .field("replaced_split_ids", &self.replaced_split_ids)
            .field("time_range", &self.time_range)
            .field("size_in_bytes", &self.size_in_bytes)
            .field("split_scratch_directory", &self.split_scratch_directory)
            .field("num_docs", &self.num_docs)
            .field("tags", &self.tags)
            .field("split_files", &self.split_files)
            .finish()
    }
}

#[derive(Debug)]
pub struct PackagedSplitBatch {
    pub splits: Vec<PackagedSplit>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub date_of_birth: Instant,
}

impl PackagedSplitBatch {
    /// Instantiate a consistent [`PackagedSplitBatch`] that
    /// satisfies two constraints:
    /// - a batch must have at least one split
    /// - all splits must be on the same `index_id`.
    pub fn new(
        splits: Vec<PackagedSplit>,
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
        publish_lock: PublishLock,
        date_of_birth: Instant,
    ) -> Self {
        assert!(!splits.is_empty());
        assert_eq!(
            splits
                .iter()
                .map(|split| split.pipeline_id.index_id.clone())
                .collect::<HashSet<_>>()
                .len(),
            1,
            "All splits must be on the same `index_id`."
        );
        Self {
            splits,
            checkpoint_delta_opt,
            publish_lock,
            date_of_birth,
        }
    }

    pub fn index_id(&self) -> String {
        self.splits
            .get(0)
            .map(|split| split.pipeline_id.index_id.clone())
            .unwrap()
    }

    pub fn split_ids(&self) -> Vec<String> {
        self.splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect::<Vec<_>>()
    }
}
