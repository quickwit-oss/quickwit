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

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use itertools::Itertools;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_proto::types::{IndexUid, PublishToken, SplitId};
use tracing::Span;

use crate::merge_policy::MergeTask;
use crate::models::{PublishLock, SplitAttrs};

pub struct PackagedSplit {
    pub serialized_split_fields: Vec<u8>,
    pub split_attrs: SplitAttrs,
    pub split_scratch_directory: TempDirectory,
    pub tags: BTreeSet<String>,
    pub split_files: Vec<PathBuf>,
    pub hotcache_bytes: Vec<u8>,
}

impl PackagedSplit {
    pub fn index_uid(&self) -> &IndexUid {
        &self.split_attrs.index_uid
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
    /// A [`MergeTask`] tracked by either the `MergePlanner` or the `DeleteTaskPlanner`
    /// in the `MergePipeline` or `DeleteTaskPipeline`.
    /// See planners docs to understand the usage.
    /// If `None`, the split batch was built in the `IndexingPipeline`.
    pub merge_task_opt: Option<MergeTask>,
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
        merge_task_opt: Option<MergeTask>,
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
            merge_task_opt,
            batch_parent_span,
        }
    }

    pub fn index_uid(&self) -> IndexUid {
        self.splits[0].split_attrs.index_uid.clone()
    }

    pub fn split_ids(&self) -> Vec<SplitId> {
        self.splits
            .iter()
            .map(|split| split.split_attrs.split_id.clone())
            .collect::<Vec<_>>()
    }
}
