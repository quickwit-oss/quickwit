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

mod serialize;

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};

use quickwit_common::uri::Uri;
use quickwit_config::{IndexConfig, SourceConfig, TestableForRegression};
use serde::{Deserialize, Serialize};
use serialize::VersionedIndexMetadata;
use time::OffsetDateTime;

use crate::checkpoint::{
    IndexCheckpoint, PartitionId, Position, SourceCheckpoint, SourceCheckpointDelta,
};
use crate::{MetastoreError, MetastoreResult};

/// An index metadata carries all meta data about an index.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(into = "VersionedIndexMetadata")]
#[serde(try_from = "VersionedIndexMetadata")]
pub struct IndexMetadata {
    /// Index configuration
    pub index_config: IndexConfig,
    /// Per-source map of checkpoint for the given index.
    pub checkpoint: IndexCheckpoint,
    /// Time at which the index was created.
    pub create_timestamp: i64,
    /// Sources
    pub sources: HashMap<String, SourceConfig>,
}

impl IndexMetadata {
    /// Panics if `index_config` is missing `index_uri`.
    pub fn new(index_config: IndexConfig) -> Self {
        IndexMetadata {
            index_config,
            checkpoint: Default::default(),
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            sources: HashMap::default(),
        }
    }

    /// Returns an [`IndexMetadata`] object with multiple hard coded values for tests.
    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(index_id: &str, index_uri: &str) -> Self {
        IndexMetadata::new(IndexConfig::for_test(index_id, index_uri))
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

    /// Adds a source to the index. Returns an error if the source_id already exists.
    pub fn add_source(&mut self, source: SourceConfig) -> MetastoreResult<()> {
        let entry = self.sources.entry(source.source_id.clone());
        let source_id = source.source_id.clone();
        if let Entry::Occupied(_) = entry {
            return Err(MetastoreError::SourceAlreadyExists {
                source_id: source_id.clone(),
                source_type: source.source_type().to_string(),
            });
        }
        entry.or_insert(source);
        self.checkpoint.add_source(&source_id);
        Ok(())
    }

    pub(crate) fn toggle_source(&mut self, source_id: &str, enable: bool) -> MetastoreResult<bool> {
        let source =
            self.sources
                .get_mut(source_id)
                .ok_or_else(|| MetastoreError::SourceDoesNotExist {
                    source_id: source_id.to_string(),
                })?;
        let mutation_occurred = source.enabled != enable;
        source.enabled = enable;
        Ok(mutation_occurred)
    }

    /// Deletes a source from the index. Returns whether the index was modified (true).
    pub(crate) fn delete_source(&mut self, source_id: &str) -> MetastoreResult<bool> {
        self.sources
            .remove(source_id)
            .ok_or_else(|| MetastoreError::SourceDoesNotExist {
                source_id: source_id.to_string(),
            })?;
        self.checkpoint.remove_source(source_id);
        Ok(true)
    }
}

impl TestableForRegression for IndexMetadata {
    fn sample_for_regression() -> IndexMetadata {
        let mut source_checkpoint = SourceCheckpoint::default();
        let delta = SourceCheckpointDelta::from_partition_delta(
            PartitionId::from(0i64),
            Position::Beginning,
            Position::from(42u64),
        );
        source_checkpoint.try_apply_delta(delta).unwrap();
        let mut per_source_checkpoint: BTreeMap<String, SourceCheckpoint> = BTreeMap::default();
        per_source_checkpoint.insert("kafka-source".to_string(), source_checkpoint);
        let checkpoint = IndexCheckpoint::from(per_source_checkpoint);
        let index_config = IndexConfig::sample_for_regression();
        let mut index_metadata = IndexMetadata {
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

    fn test_equality(&self, other: &Self) {
        self.index_config().test_equality(other.index_config());
        assert_eq!(self.checkpoint, other.checkpoint);
        assert_eq!(self.create_timestamp, other.create_timestamp);
        assert_eq!(self.sources, other.sources);
    }
}
