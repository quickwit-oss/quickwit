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

use itertools::Itertools;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_metastore::SplitMetadata;
use tracing::Span;

use crate::models::PublishLock;

pub struct SplitsUpdate {
    pub index_id: String,
    pub new_splits: Vec<SplitMetadata>,
    pub replaced_split_ids: Vec<String>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub parent_span: Span,
}

impl fmt::Debug for SplitsUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let new_split_ids: String = self
            .new_splits
            .iter()
            .map(|split| split.split_id())
            .join(",");
        f.debug_struct("SplitsUpdate")
            .field("index_id", &self.index_id)
            .field("new_splits", &new_split_ids)
            .field("checkpoint_delta", &self.checkpoint_delta_opt)
            .finish()
    }
}
