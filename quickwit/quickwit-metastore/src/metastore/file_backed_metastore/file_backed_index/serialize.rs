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

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::file_backed_metastore::file_backed_index::FileBackedIndex;
use crate::metastore::DeleteTask;
use crate::{IndexMetadata, Split};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedFileBackedIndex {
    #[serde(rename = "0.4")]
    V0_4(FileBackedIndexV0_4),
}

impl From<FileBackedIndex> for VersionedFileBackedIndex {
    fn from(index: FileBackedIndex) -> Self {
        VersionedFileBackedIndex::V0_4(index.into())
    }
}

impl From<VersionedFileBackedIndex> for FileBackedIndex {
    fn from(index: VersionedFileBackedIndex) -> Self {
        match index {
            VersionedFileBackedIndex::V0_4(v0_4) => v0_4.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileBackedIndexV0_4 {
    #[serde(rename = "index")]
    metadata: IndexMetadata,
    splits: Vec<Split>,
    #[serde(default)]
    delete_tasks: Vec<DeleteTask>,
}

impl From<FileBackedIndex> for FileBackedIndexV0_4 {
    fn from(index: FileBackedIndex) -> Self {
        Self {
            metadata: index.metadata,
            splits: index
                .splits
                .into_values()
                .sorted_by_key(|split| split.update_timestamp)
                .collect(),
            delete_tasks: index
                .delete_tasks
                .into_iter()
                .sorted_by_key(|delete_task| delete_task.opstamp)
                .collect(),
        }
    }
}

impl From<FileBackedIndexV0_4> for FileBackedIndex {
    fn from(mut index: FileBackedIndexV0_4) -> Self {
        // Override split index_id to support old SplitMetadata format.
        for split in index.splits.iter_mut() {
            if split.split_metadata.index_id.is_empty() {
                split.split_metadata.index_id = index.metadata.index_id().to_string();
            }
        }
        Self::new(index.metadata, index.splits, index.delete_tasks)
    }
}
