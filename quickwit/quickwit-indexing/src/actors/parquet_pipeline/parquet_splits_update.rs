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

//! Message type for metrics split updates sent by ParquetUploader to downstream actors.

use std::fmt;

use itertools::Itertools;
use quickwit_metastore::checkpoint::IndexCheckpointDelta;
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use quickwit_proto::types::{IndexUid, PublishToken};
use tracing::Span;

use super::parquet_merge_messages::ParquetMergeTask;
use crate::models::PublishLock;

/// Message sent by ParquetUploader to downstream actors after staging and uploading.
///
/// This is analogous to `SplitsUpdate` but uses `ParquetSplitMetadata` to support
/// both metrics and sketch splits (distinguished by the `kind` field).
pub struct ParquetSplitsUpdate {
    /// Index unique identifier.
    pub index_uid: IndexUid,
    /// The staged and uploaded splits (metrics or sketches).
    pub new_splits: Vec<ParquetSplitMetadata>,
    /// Split IDs being replaced (for merges, typically empty for ingest).
    pub replaced_split_ids: Vec<String>,
    /// Checkpoint delta covering the data in these splits.
    pub checkpoint_delta_opt: Option<IndexCheckpointDelta>,
    /// Publish lock for coordination.
    pub publish_lock: PublishLock,
    /// Optional publish token.
    pub publish_token_opt: Option<PublishToken>,
    /// Parent span for tracing.
    pub parent_span: Span,
    /// Merge task — held until the publisher drops this message, ensuring the
    /// planner inventory guard and semaphore permit stay alive while the merge
    /// output is in flight. `None` for the ingest path.
    pub _merge_task_opt: Option<ParquetMergeTask>,
}

impl fmt::Debug for ParquetSplitsUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let new_split_ids: String = self
            .new_splits
            .iter()
            .map(|split| split.split_id_str())
            .join(",");
        f.debug_struct("ParquetSplitsUpdate")
            .field("index_uid", &self.index_uid)
            .field("new_splits", &new_split_ids)
            .field("checkpoint_delta", &self.checkpoint_delta_opt)
            .finish()
    }
}
