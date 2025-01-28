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

use std::collections::HashMap;

use itertools::Itertools;
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::{DocMappingUid, SourceId};
use serde::{Deserialize, Serialize};

use super::shards::Shards;
use crate::file_backed::file_backed_index::FileBackedIndex;
use crate::metastore::DeleteTask;
use crate::{IndexMetadata, Split};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedFileBackedIndex {
    #[serde(rename = "0.9")]
    V0_9(FileBackedIndexV0_8),
    // Retro compatibility.
    #[serde(alias = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(FileBackedIndexV0_8),
}

impl From<FileBackedIndex> for VersionedFileBackedIndex {
    fn from(index: FileBackedIndex) -> Self {
        VersionedFileBackedIndex::V0_9(index.into())
    }
}

impl From<VersionedFileBackedIndex> for FileBackedIndex {
    fn from(index: VersionedFileBackedIndex) -> Self {
        match index {
            VersionedFileBackedIndex::V0_8(mut v0_8) => {
                for shards in v0_8.shards.values_mut() {
                    for shard in shards {
                        shard.doc_mapping_uid = Some(DocMappingUid::default());
                    }
                }
                v0_8.into()
            }
            VersionedFileBackedIndex::V0_9(v0_8) => v0_8.into(),
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
