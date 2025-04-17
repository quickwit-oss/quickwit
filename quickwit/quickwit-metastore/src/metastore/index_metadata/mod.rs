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

pub(crate) mod serialize;

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use quickwit_common::uri::Uri;
use quickwit_config::{
    DocMapping, IndexConfig, IndexingSettings, RetentionPolicy, SearchSettings, SourceConfig,
};
use quickwit_proto::metastore::{EntityKind, MetastoreError, MetastoreResult};
use quickwit_proto::types::{IndexUid, SourceId};
use serde::{Deserialize, Serialize};
use serialize::VersionedIndexMetadata;
use time::OffsetDateTime;

use crate::checkpoint::IndexCheckpoint;

/// An index metadata carries all meta data about an index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(into = "VersionedIndexMetadata")]
#[serde(try_from = "VersionedIndexMetadata")]
pub struct IndexMetadata {
    /// Index incarnation id
    pub index_uid: IndexUid,
    /// Index configuration
    pub index_config: IndexConfig,
    /// Per-source map of checkpoint for the given index.
    pub checkpoint: IndexCheckpoint,
    /// Time at which the index was created.
    pub create_timestamp: i64,
    /// Sources
    pub sources: HashMap<SourceId, SourceConfig>,
}

impl IndexMetadata {
    /// Panics if `index_config` is missing `index_uri`.
    pub fn new(index_config: IndexConfig) -> Self {
        let index_uid = IndexUid::new_with_random_ulid(&index_config.index_id);
        IndexMetadata::new_with_index_uid(index_uid, index_config)
    }

    /// Panics if `index_config` is missing `index_uri`.
    pub fn new_with_index_uid(index_uid: IndexUid, index_config: IndexConfig) -> Self {
        IndexMetadata {
            index_uid,
            index_config,
            checkpoint: Default::default(),
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            sources: HashMap::default(),
        }
    }

    /// Returns an [`IndexMetadata`] object with multiple hard coded values for tests.
    ///
    /// An incarnation id of `0` will be used to complete the index id into a index uuid.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(index_id: &str, index_uri: &str) -> Self {
        let index_uid = IndexUid::for_test(index_id, 0);
        let mut index_metadata = IndexMetadata::new(IndexConfig::for_test(index_id, index_uri));
        index_metadata.index_uid = index_uid;
        index_metadata
    }

    /// Extracts the index config from the index metadata object.
    pub fn into_index_config(self) -> IndexConfig {
        self.index_config
    }

    /// Accessor to the index config.
    pub fn index_config(&self) -> &IndexConfig {
        &self.index_config
    }

    /// Accessor to the index config's index id for convenience.
    pub fn index_id(&self) -> &str {
        &self.index_config.index_id
    }

    /// Accessor to the index config's index uri for convenience.
    pub fn index_uri(&self) -> &Uri {
        &self.index_config().index_uri
    }

    /// Replaces or removes the current retention policy, returning whether a mutation occurred.
    pub fn set_retention_policy(&mut self, retention_policy_opt: Option<RetentionPolicy>) -> bool {
        if self.index_config.retention_policy_opt != retention_policy_opt {
            self.index_config.retention_policy_opt = retention_policy_opt;
            true
        } else {
            false
        }
    }

    /// Replaces the current search settings, returning whether a mutation occurred.
    pub fn set_search_settings(&mut self, search_settings: SearchSettings) -> bool {
        if self.index_config.search_settings != search_settings {
            self.index_config.search_settings = search_settings;
            true
        } else {
            false
        }
    }

    /// Replaces the current indexing settings, returning whether a mutation occurred.
    pub fn set_indexing_settings(&mut self, indexing_settings: IndexingSettings) -> bool {
        if self.index_config.indexing_settings != indexing_settings {
            self.index_config.indexing_settings = indexing_settings;
            true
        } else {
            false
        }
    }

    /// Replaces the current doc mapping, returning whether a mutation occurred.
    pub fn set_doc_mapping(&mut self, doc_mapping: DocMapping) -> bool {
        if self.index_config.doc_mapping != doc_mapping {
            self.index_config.doc_mapping = doc_mapping;
            true
        } else {
            false
        }
    }

    /// Adds a source to the index. Returns an error if the source already exists.
    pub fn add_source(&mut self, source_config: SourceConfig) -> MetastoreResult<()> {
        match self.sources.entry(source_config.source_id.clone()) {
            Entry::Occupied(_) => Err(MetastoreError::AlreadyExists(EntityKind::Source {
                index_id: self.index_id().to_string(),
                source_id: source_config.source_id,
            })),
            Entry::Vacant(entry) => {
                self.checkpoint.add_source(&source_config.source_id);
                entry.insert(source_config);
                Ok(())
            }
        }
    }

    /// Adds a source to the index. Returns whether a mutation occurred and an
    /// error if the source doesn't exist.
    pub fn update_source(&mut self, source_config: SourceConfig) -> MetastoreResult<bool> {
        match self.sources.entry(source_config.source_id.clone()) {
            Entry::Occupied(mut entry) => {
                if entry.get() == &source_config {
                    return Ok(false);
                }
                entry.insert(source_config);
                Ok(true)
            }
            Entry::Vacant(_) => Err(MetastoreError::NotFound(EntityKind::Source {
                index_id: self.index_id().to_string(),
                source_id: source_config.source_id,
            })),
        }
    }

    pub(crate) fn toggle_source(&mut self, source_id: &str, enable: bool) -> MetastoreResult<bool> {
        let Some(source_config) = self.sources.get_mut(source_id) else {
            return Err(MetastoreError::NotFound(EntityKind::Source {
                index_id: self.index_id().to_string(),
                source_id: source_id.to_string(),
            }));
        };
        let mutation_occurred = source_config.enabled != enable;
        source_config.enabled = enable;
        Ok(mutation_occurred)
    }

    /// Deletes a source from the index.
    pub(crate) fn delete_source(&mut self, source_id: &str) -> MetastoreResult<()> {
        self.sources.remove(source_id).ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Source {
                index_id: self.index_id().to_string(),
                source_id: source_id.to_string(),
            })
        })?;
        self.checkpoint.remove_source(source_id);
        Ok(())
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl quickwit_config::TestableForRegression for IndexMetadata {
    fn sample_for_regression() -> IndexMetadata {
        use std::collections::BTreeMap;

        use quickwit_proto::types::Position;

        use crate::checkpoint::{PartitionId, SourceCheckpoint, SourceCheckpointDelta};

        let index_config = IndexConfig::sample_for_regression();

        let mut source_checkpoint = SourceCheckpoint::default();
        let delta = SourceCheckpointDelta::from_partition_delta(
            PartitionId::from(0i64),
            Position::Beginning,
            Position::offset(42u64),
        )
        .unwrap();
        source_checkpoint.try_apply_delta(delta).unwrap();

        let per_source_checkpoint: BTreeMap<String, SourceCheckpoint> =
            BTreeMap::from_iter([("kafka-source".to_string(), source_checkpoint)]);
        let checkpoint = IndexCheckpoint::from(per_source_checkpoint);

        let mut index_metadata = IndexMetadata {
            index_uid: IndexUid::for_test(&index_config.index_id, 1),
            index_config,
            checkpoint,
            create_timestamp: 1789,
            sources: Default::default(),
        };
        index_metadata
            .add_source(SourceConfig::sample_for_regression())
            .unwrap();
        index_metadata
    }

    fn assert_equality(&self, other: &Self) {
        self.index_config().assert_equality(other.index_config());
        assert_eq!(self.checkpoint, other.checkpoint);
        assert_eq!(self.create_timestamp, other.create_timestamp);
        assert_eq!(self.sources, other.sources);
    }
}
