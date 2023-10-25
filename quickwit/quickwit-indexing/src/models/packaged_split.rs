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

use std::collections::BTreeSet;
use std::fmt;

use itertools::Itertools;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_proto::types::{IndexUid, PublishToken, SplitId};
use tantivy::TrackedObject;
use tracing::Span;

use crate::merge_policy::MergeOperation;
use crate::models::{PublishLock, SplitAttrs};

pub struct PackagedSplit {
    pub split_attrs: SplitAttrs,
    pub split_scratch_directory: TempDirectory,
    pub tags: BTreeSet<String>,
    pub split_files: Vec<std::path::PathBuf>,
    pub hotcache_bytes: Vec<u8>,
}

impl PackagedSplit {
    pub fn index_uid(&self) -> &IndexUid {
        &self.split_attrs.pipeline_id.index_uid
    }

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
    pub splits: Vec<PackagedSplit>,
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    pub publish_lock: PublishLock,
    pub publish_token_opt: Option<PublishToken>,
    /// A [`MergeOperation`] tracked by either the `MergePlanner` or the `DeleteTaskPlanner`
    /// in the `MergePipeline` or `DeleteTaskPipeline`.
    /// See planners docs to understand the usage.
    /// If `None`, the split batch was built in the `IndexingPipeline`.
    pub merge_operation_opt: Option<TrackedObject<MergeOperation>>,
    pub batch_parent_span: Span,
}

impl PackagedSplitBatch {
    /// Instantiate a consistent [`PackagedSplitBatch`] that
    /// satisfies two constraints:
    /// - a batch must have at least one split
    /// - all splits must belong to the same `index_uid`.
    pub fn new(
        splits: Vec<PackagedSplit>,
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
        publish_lock: PublishLock,
        publish_token_opt: Option<PublishToken>,
        merge_operation_opt: Option<TrackedObject<MergeOperation>>,
        batch_parent_span: Span,
    ) -> Self {
        assert!(!splits.is_empty());
        assert!(
            splits
                .iter()
                .tuple_windows()
                .all(|(left_split, right_split)| left_split.index_uid() == right_split.index_uid()),
            "All splits must belong to the same `index_uid`."
        );
        Self {
            splits,
            checkpoint_delta_opt,
            publish_lock,
            publish_token_opt,
            merge_operation_opt,
            batch_parent_span,
        }
    }

    pub fn index_uid(&self) -> IndexUid {
        self.splits[0].split_attrs.pipeline_id.index_uid.clone()
    }

    pub fn split_ids(&self) -> Vec<SplitId> {
        self.splits
            .iter()
            .map(|split| split.split_attrs.split_id.clone())
            .collect::<Vec<_>>()
    }
}
