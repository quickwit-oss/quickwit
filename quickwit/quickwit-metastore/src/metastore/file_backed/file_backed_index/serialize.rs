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
use quickwit_proto::types::{DocMappingUid, SourceId};
use serde::{Deserialize, Serialize};

use super::StoredParquetSplit;
use super::shards::Shards;
use crate::file_backed::file_backed_index::FileBackedIndex;
use crate::metastore::{DeleteTask, use_shard_api};
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
    /// Metrics splits (for metrics pipeline).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    metrics_splits: Vec<StoredParquetSplit>,
    /// Sketch splits (for DDSketch pipeline).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    sketch_splits: Vec<StoredParquetSplit>,
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
                if !shards.is_empty() {
                    return Some((source_id, shards.into_shards_vec()));
                }
                // A source whose checkpoint is stored in the shard table must keep its entry even
                // when it holds no shard, so that a node still running an affected version can
                // serve the shard API for it. The other sources carry no information and are
                // skipped, as they were before ingest v2 was released.
                let source = index.metadata.sources.get(&source_id)?;
                if !use_shard_api(&source.source_params) {
                    return None;
                }
                Some((source_id, shards.into_shards_vec()))
            })
            .collect();
        let delete_tasks = index
            .delete_tasks
            .into_iter()
            .sorted_by_key(|delete_task| delete_task.opstamp)
            .collect();
        let metrics_splits = index
            .metrics_splits
            .into_values()
            .sorted_by_key(|split| split.update_timestamp)
            .collect();
        let sketch_splits = index
            .sketch_splits
            .into_values()
            .sorted_by_key(|split| split.update_timestamp)
            .collect();
        Self {
            metadata: index.metadata,
            splits,
            shards,
            delete_tasks,
            metrics_splits,
            sketch_splits,
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
        // Restore the entries of the sources that store their checkpoint in the shard table but
        // hold no shard. Versions prior to this one dropped them on serialization, so this also
        // repairs indexes that were persisted by those versions.
        for source in index.metadata.sources.values() {
            if use_shard_api(&source.source_params)
                && !per_source_shards.contains_key(&source.source_id)
            {
                let index_uid = index.metadata.index_uid.clone();
                let source_id = source.source_id.clone();
                per_source_shards.insert(source_id.clone(), Shards::empty(index_uid, source_id));
            }
        }
        Self::new_with_metrics_splits(
            index.metadata,
            index.splits,
            per_source_shards,
            index.delete_tasks,
            index.metrics_splits,
            index.sketch_splits,
        )
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::{
        FileSourceMessageType, FileSourceNotification, FileSourceParams, FileSourceSqs,
        SourceConfig, SourceParams,
    };
    use quickwit_proto::metastore::ListShardsSubrequest;

    use super::*;

    /// Builds an index holding one source that uses the shard API and one that does not, neither
    /// of them holding a shard yet.
    fn index_with_shardless_sources() -> FileBackedIndex {
        let sqs_params = FileSourceSqs {
            queue_url: "https://sqs.us-east-1.amazonaws.com/000000000000/queue".to_string(),
            message_type: FileSourceMessageType::S3Notification,
            deduplication_window_duration_secs: 100,
            deduplication_window_max_messages: 100,
            deduplication_cleanup_interval_secs: 60,
        };
        let source_params = SourceParams::File(FileSourceParams::Notifications(
            FileSourceNotification::Sqs(sqs_params),
        ));
        let index_metadata = IndexMetadata::for_test("test-index", "ram://indexes/test-index");
        let mut index = FileBackedIndex::from(index_metadata);
        index
            .add_source(SourceConfig::for_test("sqs-source", source_params))
            .unwrap();
        index
            .add_source(SourceConfig::for_test("void-source", SourceParams::void()))
            .unwrap();
        index
    }

    /// A source that stores its checkpoint in the shard table holds no shard until its first one
    /// is opened. Its entry must still be serialized, otherwise a node running an affected
    /// version cannot serve the shard API for it.
    #[test]
    fn test_serialize_keeps_shardless_shard_api_source() {
        let index = index_with_shardless_sources();

        let serialized = serde_json::to_value(&index).unwrap();

        assert_eq!(serialized["shards"]["sqs-source"], serde_json::json!([]));
        // The sources that do not use the shard API carry no information and are still skipped.
        assert!(serialized["shards"].get("void-source").is_none());
    }

    /// Versions prior to this one dropped that entry, which left the index permanently unable to
    /// serve the shard API for the source. Deserialization restores it.
    #[test]
    fn test_deserialize_restores_dropped_shardless_shard_api_source() {
        let index = index_with_shardless_sources();
        let mut serialized = serde_json::to_value(&index).unwrap();
        // Simulate an index persisted by an affected version.
        serialized.as_object_mut().unwrap().remove("shards");

        let deserialized: FileBackedIndex = serde_json::from_value(serialized).unwrap();

        let subresponse = deserialized
            .list_shards(ListShardsSubrequest {
                source_id: "sqs-source".to_string(),
                ..Default::default()
            })
            .unwrap();
        assert!(subresponse.shards.is_empty());
    }
}
