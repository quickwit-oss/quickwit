// Copyright (C) 2024 Quickwit, Inc.
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
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::SourceId;
use serde::{Deserialize, Serialize};

use super::shards::Shards;
use crate::file_backed::file_backed_index::FileBackedIndex;
use crate::metastore::DeleteTask;
use crate::{IndexMetadata, Split};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedFileBackedIndex {
    #[serde(rename = "0.9")]
    // Retro compatibility.
    #[serde(alias = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(FileBackedIndexV0_8),
}

impl From<FileBackedIndex> for VersionedFileBackedIndex {
    fn from(index: FileBackedIndex) -> Self {
        VersionedFileBackedIndex::V0_8(index.into())
    }
}

impl From<VersionedFileBackedIndex> for FileBackedIndex {
    fn from(index: VersionedFileBackedIndex) -> Self {
        match index {
            VersionedFileBackedIndex::V0_8(v0_8) => v0_8.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileBackedIndexV0_8 {
    #[serde(rename = "index")]
    metadata: IndexMetadata,
    splits: Vec<Split>,
    // TODO: Remove `skip_serializing_if` when we release ingest v2.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    shards: HashMap<SourceId, Vec<Shard>>,
    #[serde(default)]
    delete_tasks: Vec<DeleteTask>,
}

impl From<FileBackedIndex> for FileBackedIndexV0_8 {
    fn from(index: FileBackedIndex) -> Self {
        let splits = index
            .splits
            .into_values()
            .sorted_by_key(|split| split.update_timestamp)
            .collect();
        let shards = index
            .per_source_shards
            .into_iter()
            .filter_map(|(source_id, shards)| {
                // TODO: Remove this filter when we release ingest v2.
                // Skip serializing empty shards since the feature is hidden and disabled by
                // default. This way, we can still modify the serialization format without worrying
                // about backward compatibility post `0.7`.
                if !shards.is_empty() {
                    Some((source_id, shards.into_shards_vec()))
                } else {
                    None
                }
            })
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

impl From<FileBackedIndexV0_8> for FileBackedIndex {
    fn from(index: FileBackedIndexV0_8) -> Self {
        let mut per_source_shards: HashMap<SourceId, Shards> = index
            .shards
            .into_iter()
            .map(|(source_id, shards_vec)| {
                let index_uid = index.metadata.index_uid.clone();
                (
                    source_id.clone(),
                    Shards::from_shards_vec(index_uid, source_id, shards_vec),
                )
            })
            .collect();
        // TODO: Remove this when we release ingest v2.
        for source in index.metadata.sources.values() {
            if source.source_type() == SourceType::IngestV2
                && !per_source_shards.contains_key(&source.source_id)
            {
                let index_uid = index.metadata.index_uid.clone();
                let source_id = source.source_id.clone();
                per_source_shards.insert(source_id.clone(), Shards::empty(index_uid, source_id));
            }
        }
        Self::new(
            index.metadata,
            index.splits,
            per_source_shards,
            index.delete_tasks,
        )
    }
}
