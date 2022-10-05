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

use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use tracing::Span;

use crate::models::{PublishLock, ScratchDirectory, SplitAttrs};

pub struct PackagedSplit {
    pub split_attrs: SplitAttrs,
    pub split_scratch_directory: ScratchDirectory,
    pub tags: BTreeSet<String>,
    pub split_files: Vec<std::path::PathBuf>,
    pub hotcache_bytes: Vec<u8>,
}

impl PackagedSplit {
    pub fn split_id(&self) -> &str {
        &self.split_attrs.split_id
    }
}

impl fmt::Debug for PackagedSplit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PackagedSplit")
            .field("split_attrs", &self.split_attrs)
            .field("split_scratch_directory", &self.split_scratch_directory)
            .field("tags", &self.tags)
            .field("split_files", &self.split_files)
            .finish()
    }
}

#[derive(Debug)]
pub struct PackagedSplitBatch {
    pub parent_span: Span,
    pub splits: Vec<PackagedSplit>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
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
        span: Span,
    ) -> Self {
        assert!(!splits.is_empty());
        assert_eq!(
            splits
                .iter()
                .map(|split| split.split_attrs.pipeline_id.index_id.clone())
                .collect::<HashSet<_>>()
                .len(),
            1,
            "All splits must be on the same `index_id`."
        );
        Self {
            parent_span: span,
            splits,
            checkpoint_delta_opt,
            publish_lock,
        }
    }

    pub fn index_id(&self) -> String {
        self.splits
            .get(0)
            .map(|split| split.split_attrs.pipeline_id.index_id.clone())
            .unwrap()
    }

    pub fn split_ids(&self) -> Vec<String> {
        self.splits
            .iter()
            .map(|split| split.split_attrs.split_id.clone())
            .collect::<Vec<_>>()
    }
}
