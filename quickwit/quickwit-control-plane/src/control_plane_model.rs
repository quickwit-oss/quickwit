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

use std::collections::hash_map::Entry;
use std::time::Instant;

use fnv::{FnvHashMap, FnvHashSet};
#[cfg(test)]
use itertools::Itertools;
use quickwit_common::Progress;
use quickwit_config::{SourceConfig, INGEST_SOURCE_ID};
use quickwit_metastore::{
    IndexMetadata, ListIndexesMetadataRequestExt, ListIndexesMetadataResponseExt,
};
use quickwit_proto::control_plane::ControlPlaneResult;
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::metastore::{
    EntityKind, ListIndexesMetadataRequest, ListShardsSubrequest, MetastoreError, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_proto::types::IndexId;
use quickwit_proto::{metastore, IndexUid, NodeId, NodeIdRef, ShardId, SourceId};
use serde::Serialize;
use tracing::{error, info};

use crate::SourceUid;

type NextShardId = ShardId;
#[derive(Debug, Eq, PartialEq)]
struct ShardTableEntry {
    shards: FnvHashMap<ShardId, Shard>,
    next_shard_id: NextShardId,
}

impl Default for ShardTableEntry {
    fn default() -> Self {
        Self {
            shards: Default::default(),
            next_shard_id: Self::DEFAULT_NEXT_SHARD_ID,
        }
    }
}

impl ShardTableEntry {
    const DEFAULT_NEXT_SHARD_ID: NextShardId = 1; // `1` matches the PostgreSQL sequence min value.

    fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    fn is_default(&self) -> bool {
        self.is_empty() && self.next_shard_id == Self::DEFAULT_NEXT_SHARD_ID
    }

    #[cfg(test)]
    fn shards(&self) -> Vec<Shard> {
        self.shards
            .values()
            .cloned()
            .sorted_unstable_by_key(|shard| shard.shard_id)
            .collect()
    }
}

/// The control plane maintains a model in sync with the metastore.
///
/// The model stays consistent with the metastore, because all
/// of the mutations go through the control plane.
///
/// If a mutation yields an error, the control plane is killed
/// and restarted.
///
/// Upon starts, it loads its entire state from the metastore.
#[derive(Default, Debug)]
pub struct ControlPlaneModel {
    index_uid_table: FnvHashMap<IndexId, IndexUid>,
    index_table: FnvHashMap<IndexUid, IndexMetadata>,
    shard_table: ShardTable,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ControlPlaneModelMetrics {
    pub num_shards: usize,
}

impl ControlPlaneModel {
    /// Clears the entire state of the model.
    pub fn clear(&mut self) {
        *self = Default::default();
    }

    pub fn observable_state(&self) -> ControlPlaneModelMetrics {
        ControlPlaneModelMetrics {
            num_shards: self.shard_table.table_entries.len(),
        }
    }

    pub async fn load_from_metastore(
        &mut self,
        metastore: &mut MetastoreServiceClient,
        progress: &Progress,
    ) -> ControlPlaneResult<()> {
        let now = Instant::now();
        self.clear();
        let index_metadatas = progress
            .protect_future(metastore.list_indexes_metadata(ListIndexesMetadataRequest::all()))
            .await?
            .deserialize_indexes_metadata()?;
        let num_indexes = index_metadatas.len();
        self.index_table.reserve(num_indexes);

        let mut num_sources = 0;
        let mut num_shards = 0;

        let mut subrequests = Vec::with_capacity(index_metadatas.len());

        for index_metadata in index_metadatas {
            self.add_index(index_metadata);
        }

        for index_metadata in self.index_table.values() {
            for source_id in index_metadata.sources.keys() {
                if source_id != INGEST_SOURCE_ID {
                    continue;
                }
                let request = ListShardsSubrequest {
                    index_uid: index_metadata.index_uid.clone().into(),
                    source_id: source_id.to_string(),
                    shard_state: Some(ShardState::Open as i32),
                };
                num_sources += 1;
                subrequests.push(request);
            }
        }
        if !subrequests.is_empty() {
            let list_shards_request = metastore::ListShardsRequest { subrequests };
            let list_shard_response = progress
                .protect_future(metastore.list_shards(list_shards_request))
                .await?;

            self.shard_table
                .table_entries
                .reserve(list_shard_response.subresponses.len());

            for list_shards_subresponse in list_shard_response.subresponses {
                num_shards += list_shards_subresponse.shards.len();

                let source_uid = SourceUid {
                    index_uid: list_shards_subresponse.index_uid.into(),
                    source_id: list_shards_subresponse.source_id,
                };
                let shards: FnvHashMap<ShardId, Shard> = list_shards_subresponse
                    .shards
                    .into_iter()
                    .map(|shard| (shard.shard_id, shard))
                    .collect();
                let table_entry = ShardTableEntry {
                    shards,
                    next_shard_id: list_shards_subresponse.next_shard_id,
                };
                self.shard_table
                    .table_entries
                    .insert(source_uid, table_entry);
            }
        }
        info!(
            "synced internal state with metastore in {} seconds ({} indexes, {} sources, {} \
             shards)",
            now.elapsed().as_secs(),
            num_indexes,
            num_sources,
            num_shards,
        );
        Ok(())
    }

    pub fn list_shards(&self, source_uid: &SourceUid) -> Vec<ShardId> {
        self.shard_table.list_shards(source_uid)
    }

    pub(crate) fn get_source_configs(
        &self,
    ) -> impl Iterator<Item = (SourceUid, &SourceConfig)> + '_ {
        self.index_table.values().flat_map(|index_metadata| {
            index_metadata
                .sources
                .iter()
                .map(move |(source_id, source_config)| {
                    (
                        SourceUid {
                            index_uid: index_metadata.index_uid.clone(),
                            source_id: source_id.clone(),
                        },
                        source_config,
                    )
                })
        })
    }

    pub(crate) fn add_index(&mut self, index_metadata: IndexMetadata) {
        let index_uid = index_metadata.index_uid.clone();
        self.index_uid_table
            .insert(index_metadata.index_id().to_string(), index_uid.clone());
        self.index_table.insert(index_uid, index_metadata);
    }

    pub(crate) fn delete_index(&mut self, index_uid: &IndexUid) {
        // TODO: We need to let the routers and ingesters know.
        self.index_table.remove(index_uid);
        self.shard_table.delete_index(index_uid.index_id());
    }

    /// Adds a source to a given index. Returns an error if a source with the same source_id already
    /// exists.
    pub(crate) fn add_source(
        &mut self,
        index_uid: &IndexUid,
        source_config: SourceConfig,
    ) -> ControlPlaneResult<()> {
        self.shard_table
            .add_source(index_uid, &source_config.source_id);
        let index_metadata = self.index_table.get_mut(index_uid).ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.to_string(),
            })
        })?;
        index_metadata.add_source(source_config)?;
        Ok(())
    }

    pub(crate) fn delete_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        self.shard_table.delete_source(index_uid, source_id);
    }

    /// Removes the shards identified by their index UID, source ID, and shard IDs.
    pub fn delete_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shard_ids: &[ShardId],
    ) {
        self.shard_table
            .delete_shards(index_uid, source_id, shard_ids);
    }

    /// Sets the state of the shards identified by their index UID, source ID, and shard IDs to
    /// `Closed`.
    pub fn close_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shard_ids: &[ShardId],
    ) {
        self.shard_table
            .close_shards(index_uid, source_id, shard_ids);
    }

    pub fn index_uid(&self, index_id: &str) -> Option<IndexUid> {
        self.index_uid_table.get(index_id).cloned()
    }

    /// Updates the shard table.
    pub fn update_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shards: &[Shard],
        next_shard_id: NextShardId,
    ) {
        self.shard_table
            .update_shards(index_uid, source_id, shards, next_shard_id);
    }

    /// Finds open shards for a given index and source and whose leaders are not in the set of
    /// unavailable ingesters.
    pub fn find_open_shards(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        unavailable_ingesters: &FnvHashSet<NodeId>,
    ) -> Option<(Vec<Shard>, NextShardId)> {
        self.shard_table
            .find_open_shards(index_uid, source_id, unavailable_ingesters)
    }
}

// A table that keeps track of the existing shards for each index and source.
#[derive(Debug, Default)]
struct ShardTable {
    table_entries: FnvHashMap<SourceUid, ShardTableEntry>,
}

impl ShardTable {
    /// Adds a new empty entry for the given index and source.
    ///
    /// TODO check and document the behavior on error (if the source was already here).
    fn add_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = ShardTableEntry::default();
        let previous_table_entry_opt = self.table_entries.insert(source_uid, table_entry);
        if let Some(previous_table_entry) = previous_table_entry_opt {
            if !previous_table_entry.is_default() {
                error!(
                    "shard table entry for index `{}` and source `{}` already exists",
                    index_uid.index_id(),
                    source_id
                );
            }
        }
    }

    fn delete_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        self.table_entries.remove(&source_uid);
    }

    /// Removes all the entries that match the target index ID.
    fn delete_index(&mut self, index_id: &str) {
        self.table_entries
            .retain(|source_uid, _| source_uid.index_uid.index_id() != index_id);
    }

    fn list_shards(&self, source_uid: &SourceUid) -> Vec<ShardId> {
        let Some(shard_table_entry) = self.table_entries.get(source_uid) else {
            return Vec::new();
        };
        shard_table_entry
            .shards
            .values()
            .map(|shard| shard.shard_id)
            .collect()
    }

    /// Finds open shards for a given index and source and whose leaders are not in the set of
    /// unavailable ingesters.
    fn find_open_shards(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        unavailable_ingesters: &FnvHashSet<NodeId>,
    ) -> Option<(Vec<Shard>, NextShardId)> {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = self.table_entries.get(&source_uid)?;
        let open_shards: Vec<Shard> = table_entry
            .shards
            .values()
            .filter(|shard| {
                shard.is_open()
                    && !unavailable_ingesters.contains(NodeIdRef::from_str(&shard.leader_id))
            })
            .cloned()
            .collect();

        #[cfg(test)]
        let open_shards = open_shards
            .into_iter()
            .sorted_by_key(|shard| shard.shard_id)
            .collect();

        Some((open_shards, table_entry.next_shard_id))
    }

    /// Updates the shard table.
    pub fn update_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shards: &[Shard],
        next_shard_id: NextShardId,
    ) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        match self.table_entries.entry(source_uid) {
            Entry::Occupied(mut entry) => {
                for shard in shards {
                    let table_entry = entry.get_mut();
                    table_entry.shards.insert(shard.shard_id, shard.clone());
                    table_entry.next_shard_id = next_shard_id;
                }
            }
            // This should never happen if the control plane view is consistent with the state of
            // the metastore, so should we panic here? Warnings are most likely going to go
            // unnoticed.
            Entry::Vacant(entry) => {
                let shards: FnvHashMap<ShardId, Shard> = shards
                    .iter()
                    .cloned()
                    .map(|shard| (shard.shard_id, shard))
                    .collect();
                let table_entry = ShardTableEntry {
                    shards,
                    next_shard_id,
                };
                entry.insert(table_entry);
            }
        }
    }

    /// Sets the state of the shards identified by their index UID, source ID, and shard IDs to
    /// `Closed`.
    pub fn close_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shard_ids: &[ShardId],
    ) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        if let Some(table_entry) = self.table_entries.get_mut(&source_uid) {
            for shard_id in shard_ids {
                if let Some(shard) = table_entry.shards.get_mut(shard_id) {
                    shard.shard_state = ShardState::Closed as i32;
                }
            }
        }
    }

    /// Removes the shards identified by their index UID, source ID, and shard IDs.
    pub fn delete_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shard_ids: &[ShardId],
    ) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        if let Some(table_entry) = self.table_entries.get_mut(&source_uid) {
            for shard_id in shard_ids {
                table_entry.shards.remove(shard_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::SourceConfig;
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::metastore::ListIndexesMetadataResponse;

    use super::*;

    #[test]
    fn test_shard_table_add_source() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();
        let mut shard_table = ShardTable::default();
        shard_table.add_source(&index_uid, &source_id);
        assert_eq!(shard_table.table_entries.len(), 1);
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = shard_table.table_entries.get(&source_uid).unwrap();
        assert!(table_entry.shards.is_empty());
        assert_eq!(table_entry.next_shard_id, 1);
    }

    #[test]
    fn test_shard_table_find_open_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();
        shard_table.add_source(&index_uid, &source_id);

        let mut unavailable_ingesters = FnvHashSet::default();

        let (open_shards, next_shard_id) = shard_table
            .find_open_shards(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 0);
        assert_eq!(next_shard_id, 1);

        let shard_01 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 2,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closing as i32,
            ..Default::default()
        };
        let shard_03 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 3,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_04 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 4,
            leader_id: "test-leader-1".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.update_shards(
            &index_uid,
            &source_id,
            &[shard_01, shard_02, shard_03.clone(), shard_04.clone()],
            5,
        );
        let (open_shards, next_shard_id) = shard_table
            .find_open_shards(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 2);
        assert_eq!(open_shards[0], shard_03);
        assert_eq!(open_shards[1], shard_04);
        assert_eq!(next_shard_id, 5);

        unavailable_ingesters.insert("test-leader-0".into());

        let (open_shards, next_shard_id) = shard_table
            .find_open_shards(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 1);
        assert_eq!(open_shards[0], shard_04);
        assert_eq!(next_shard_id, 5);
    }

    #[test]
    fn test_shard_table_update_shards() {
        let index_uid_0: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let mut shard_01 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.update_shards(&index_uid_0, &source_id, &[shard_01.clone()], 2);

        assert_eq!(shard_table.table_entries.len(), 1);

        let source_uid = SourceUid {
            index_uid: index_uid_0.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = shard_table.table_entries.get(&source_uid).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], shard_01);
        assert_eq!(table_entry.next_shard_id, 2);

        shard_01.shard_state = ShardState::Closed as i32;

        let shard_02 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 2,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };

        shard_table.update_shards(
            &index_uid_0,
            &source_id,
            &[shard_01.clone(), shard_02.clone()],
            3,
        );

        assert_eq!(shard_table.table_entries.len(), 1);

        let source_uid = SourceUid {
            index_uid: index_uid_0.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = shard_table.table_entries.get(&source_uid).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0], shard_01);
        assert_eq!(shards[1], shard_02);
        assert_eq!(table_entry.next_shard_id, 3);
    }

    #[test]
    fn test_shard_table_delete_shards() {
        let index_uid_0: IndexUid = "test-index:0".into();
        let index_uid_1: IndexUid = "test-index:1".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let shard_01 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 2,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_11 = Shard {
            index_uid: index_uid_1.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.update_shards(&index_uid_0, &source_id, &[shard_01.clone(), shard_02], 3);
        shard_table.update_shards(&index_uid_1, &source_id, &[shard_11], 2);
        shard_table.delete_shards(&index_uid_0, &source_id, &[2]);
        shard_table.delete_shards(&index_uid_1, &source_id, &[1]);

        assert_eq!(shard_table.table_entries.len(), 2);

        let source_uid_0 = SourceUid {
            index_uid: index_uid_0.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = shard_table.table_entries.get(&source_uid_0).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], shard_01);
        assert_eq!(table_entry.next_shard_id, 3);

        let source_uid_1 = SourceUid {
            index_uid: index_uid_1.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = shard_table.table_entries.get(&source_uid_1).unwrap();
        assert!(table_entry.is_empty());
        assert_eq!(table_entry.next_shard_id, 2);
    }

    #[tokio::test]
    async fn test_control_plane_model_load_shard_table() {
        let progress = Progress::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|request| {
                assert_eq!(request, ListIndexesMetadataRequest::all());

                let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
                let source = SourceConfig::ingest_default();
                index_0.add_source(source.clone()).unwrap();

                let mut index_1 = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
                index_1.add_source(source).unwrap();

                let indexes = vec![index_0, index_1];
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(indexes).unwrap())
            });
        mock_metastore.expect_list_shards().returning(|request| {
            assert_eq!(request.subrequests.len(), 2);

            assert_eq!(request.subrequests[0].index_uid, "test-index-0:0");
            assert_eq!(request.subrequests[0].source_id, INGEST_SOURCE_ID);
            assert_eq!(request.subrequests[0].shard_state(), ShardState::Open);

            assert_eq!(request.subrequests[1].index_uid, "test-index-1:0");
            assert_eq!(request.subrequests[1].source_id, INGEST_SOURCE_ID);
            assert_eq!(request.subrequests[1].shard_state(), ShardState::Open);

            let subresponses = vec![
                metastore::ListShardsSubresponse {
                    index_uid: "test-index-0:0".to_string(),
                    source_id: INGEST_SOURCE_ID.to_string(),
                    shards: vec![Shard {
                        shard_id: 42,
                        ..Default::default()
                    }],
                    next_shard_id: 43,
                },
                metastore::ListShardsSubresponse {
                    index_uid: "test-index-1:0".to_string(),
                    source_id: INGEST_SOURCE_ID.to_string(),
                    shards: Vec::new(),
                    next_shard_id: 1,
                },
            ];
            let response = metastore::ListShardsResponse { subresponses };
            Ok(response)
        });
        let mut model = ControlPlaneModel::default();
        let mut metastore_client = MetastoreServiceClient::from(mock_metastore);
        model
            .load_from_metastore(&mut metastore_client, &progress)
            .await
            .unwrap();

        assert_eq!(model.index_table.len(), 2);
        assert_eq!(
            model.index_uid("test-index-0").unwrap(),
            IndexUid::from("test-index-0:0".to_string())
        );
        assert_eq!(
            model.index_uid("test-index-1").unwrap(),
            IndexUid::from("test-index-1:0".to_string())
        );

        assert_eq!(model.shard_table.table_entries.len(), 2);

        let source_uid_0 = SourceUid {
            index_uid: "test-index-0:0".into(),
            source_id: INGEST_SOURCE_ID.to_string(),
        };
        let table_entry = model.shard_table.table_entries.get(&source_uid_0).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, 42);
        assert_eq!(table_entry.next_shard_id, 43);

        let source_uid_1 = SourceUid {
            index_uid: "test-index-1:0".into(),
            source_id: INGEST_SOURCE_ID.to_string(),
        };
        let table_entry = model.shard_table.table_entries.get(&source_uid_1).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 0);
        assert_eq!(table_entry.next_shard_id, 1);
    }
    #[tokio::test]
    async fn test_ingest_controller_close_shards() {
        // TODO: Write test when the RPC is actually called by ingesters.
    }
}
