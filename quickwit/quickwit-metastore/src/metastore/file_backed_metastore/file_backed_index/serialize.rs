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

use std::collections::HashMap;

use itertools::Itertools;
use quickwit_proto::types::SourceId;
use serde::{Deserialize, Serialize};

use super::shards::{SerdeShards, Shards};
use crate::file_backed_metastore::file_backed_index::FileBackedIndex;
use crate::metastore::DeleteTask;
use crate::{IndexMetadata, Split};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedFileBackedIndex {
    #[serde(rename = "0.6")]
    // Retro compatibility.
    #[serde(alias = "0.5")]
    #[serde(alias = "0.4")]
    V0_6(FileBackedIndexV0_6),
}

impl From<FileBackedIndex> for VersionedFileBackedIndex {
    fn from(index: FileBackedIndex) -> Self {
        VersionedFileBackedIndex::V0_6(index.into())
    }
}

impl From<VersionedFileBackedIndex> for FileBackedIndex {
    fn from(index: VersionedFileBackedIndex) -> Self {
        match index {
            VersionedFileBackedIndex::V0_6(v0_6) => v0_6.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileBackedIndexV0_6 {
    #[serde(rename = "index")]
    metadata: IndexMetadata,
    splits: Vec<Split>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    shards: HashMap<SourceId, SerdeShards>,
    #[serde(default)]
    delete_tasks: Vec<DeleteTask>,
}

impl From<FileBackedIndex> for FileBackedIndexV0_6 {
    fn from(index: FileBackedIndex) -> Self {
        let splits = index
            .splits
            .into_values()
            .sorted_by_key(|split| split.update_timestamp)
            .collect();
        let shards = index
            .per_source_shards
            .into_iter()
            .map(|(source_id, shards)| (source_id, SerdeShards::from(shards)))
            .collect();
        let delete_tasks = index
            .delete_tasks
            .into_iter()
            .sorted_by_key(|delete_task| delete_task.opstamp)
            .collect();
        Self {
            metadata: index.metadata,
            splits,
            shards,
            delete_tasks,
        }
    }
}

impl From<FileBackedIndexV0_6> for FileBackedIndex {
    fn from(mut index: FileBackedIndexV0_6) -> Self {
        // Override split index_id to support old SplitMetadata format.
        for split in index.splits.iter_mut() {
            if split.split_metadata.index_uid.is_empty() {
                split.split_metadata.index_uid = index.metadata.index_uid.clone();
            }
        }
        let shards = index
            .shards
            .into_iter()
            .map(|(source_id, serde_shards)| {
                let index_uid = index.metadata.index_uid.clone();
                (
                    source_id.clone(),
                    Shards::from_serde_shards(index_uid, source_id, serde_shards),
                )
            })
            .collect();
        Self::new(index.metadata, index.splits, shards, index.delete_tasks)
    }
}
