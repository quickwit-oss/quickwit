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

use std::collections::HashSet;
use std::ops::{Range, RangeInclusive};

use quickwit_metastore::checkpoint::CheckpointDelta;
use tantivy::SegmentId;

use crate::models::ScratchDirectory;

#[derive(Debug)]
pub struct PackagedSplit {
    pub split_id: String,
    pub replaced_split_ids: Vec<String>,
    pub index_id: String,
    pub checkpoint_delta: CheckpointDelta,
    pub time_range: Option<RangeInclusive<i64>>,
    pub size_in_bytes: u64,
    pub footer_offsets: Range<u64>,
    pub segment_ids: Vec<SegmentId>,
    pub split_scratch_directory: ScratchDirectory,
    pub num_docs: u64,
    pub tags: HashSet<String>,
}
