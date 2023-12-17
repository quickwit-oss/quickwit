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

mod shard_table;

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::ops::Deref;
use std::time::Instant;

use anyhow::bail;
use fnv::{FnvHashMap, FnvHashSet};
use quickwit_common::Progress;
use quickwit_config::SourceConfig;
use quickwit_ingest::ShardInfos;
use quickwit_metastore::{IndexMetadata, ListIndexesMetadataResponseExt};
use quickwit_proto::control_plane::{ControlPlaneError, ControlPlaneResult};
use quickwit_proto::ingest::Shard;
use quickwit_proto::metastore::{
    self, EntityKind, ListIndexesMetadataRequest, ListShardsSubrequest, ListShardsSubresponse,
    MetastoreError, MetastoreService, MetastoreServiceClient, SourceType,
};
use quickwit_proto::types::{IndexId, IndexUid, NodeId, ShardId, SourceId, SourceUid};
use serde::Serialize;
pub(super) use shard_table::{NextShardId, ScalingMode, ShardEntry, ShardStats, ShardTable};
use tracing::{info, instrument, warn};

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
pub(crate) struct ControlPlaneModel {
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
            num_shards: self.shard_table.num_shards(),
        }
    }

    #[instrument(skip_all)]
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
            for source_config in index_metadata.sources.values() {
                num_sources += 1;

                if source_config.source_type() != SourceType::IngestV2 || !source_config.enabled {
                    continue;
                }
                let request = ListShardsSubrequest {
                    index_uid: index_metadata.index_uid.clone().into(),
                    source_id: source_config.source_id.clone(),
                    shard_state: None,
                };
                subrequests.push(request);
            }
        }
        if !subrequests.is_empty() {
            let list_shards_request = metastore::ListShardsRequest { subrequests };
            let list_shard_response = progress
                .protect_future(metastore.list_shards(list_shards_request))
                .await?;

            for list_shards_subresponse in list_shard_response.subresponses {
                num_shards += list_shards_subresponse.shards.len();
                let ListShardsSubresponse {
                    index_uid,
                    source_id,
                    shards,
                    next_shard_id,
                } = list_shards_subresponse;
                let source_uid = SourceUid {
                    index_uid: IndexUid::parse(&index_uid).map_err(|invalid_index_uri| {
                        ControlPlaneError::Internal(format!(
                            "invalid index uid received from the metastore: {invalid_index_uri:?}"
                        ))
                    })?,
                    source_id,
                };
                self.shard_table
                    .initialize_source_shards(source_uid, shards, next_shard_id);
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

    pub fn index_uid(&self, index_id: &str) -> Option<IndexUid> {
        self.index_uid_table.get(index_id).cloned()
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
        let index_metadata = self.index_table.get_mut(index_uid).ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.to_string(),
            })
        })?;
        let source_id = source_config.source_id.clone();
        index_metadata.add_source(source_config)?;
        self.shard_table.add_source(index_uid, &source_id);
        Ok(())
    }

    pub(crate) fn delete_source(&mut self, source_uid: &SourceUid) {
        // Removing shards from shard table.
        self.shard_table
            .delete_source(&source_uid.index_uid, &source_uid.source_id);
        // Remove source from index config.
        let Some(index_model) = self.index_table.get_mut(&source_uid.index_uid) else {
            warn!(index_uid=%source_uid.index_uid, source_id=%source_uid.source_id, "delete source: index not found");
            return;
        };
        if index_model.sources.remove(&source_uid.source_id).is_none() {
            warn!(index_uid=%source_uid.index_uid, source_id=%source_uid.source_id, "delete source: source not found");
        };
    }

    /// Returns `true` if the source status has changed, `false` otherwise.
    /// Returns an error if the source could not be found.
    pub(crate) fn toggle_source(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        enable: bool,
    ) -> anyhow::Result<bool> {
        let Some(index_model) = self.index_table.get_mut(index_uid) else {
            bail!("index `{}` not found", index_uid.index_id());
        };
        let Some(source_config) = index_model.sources.get_mut(source_id) else {
            bail!("source `{source_id}` not found");
        };
        let has_changed = source_config.enabled != enable;
        source_config.enabled = enable;
        Ok(has_changed)
    }

    pub(crate) fn all_shards_mut(&mut self) -> impl Iterator<Item = &mut ShardEntry> + '_ {
        self.shard_table.all_shards_mut()
    }

    #[cfg(test)]
    pub(crate) fn all_shards(&self) -> impl Iterator<Item = &ShardEntry> + '_ {
        self.shard_table.all_shards()
    }

    pub fn list_shards_for_node(
        &self,
        ingester: &NodeId,
    ) -> impl Deref<Target = FnvHashMap<SourceUid, BTreeSet<ShardId>>> + '_ {
        if let Some(shards_for_node) = self.shard_table.list_shards_for_node(ingester) {
            Cow::Borrowed(shards_for_node)
        } else {
            Cow::Owned(FnvHashMap::default())
        }
    }

    pub fn list_shards_for_index<'a>(
        &'a self,
        index_uid: &'a IndexUid,
    ) -> impl Iterator<Item = &'a ShardEntry> + 'a {
        self.shard_table.list_shards_for_index(index_uid)
    }

    /// Lists the shards of a given source. Returns `None` if the source does not exist.
    pub fn list_shards_for_source(
        &self,
        source_uid: &SourceUid,
    ) -> Option<impl Iterator<Item = &ShardEntry>> {
        self.shard_table.list_shards(source_uid)
    }

    pub fn next_shard_id(&self, source_uid: &SourceUid) -> Option<ShardId> {
        self.shard_table.next_shard_id(source_uid)
    }

    /// Inserts the shards that have just been opened by calling `open_shards` on the metastore.
    pub fn insert_newly_opened_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shards: Vec<Shard>,
        next_shard_id: NextShardId,
    ) {
        self.shard_table
            .insert_newly_opened_shards(index_uid, source_id, shards, next_shard_id);
    }

    /// Finds open shards for a given index and source and whose leaders are not in the set of
    /// unavailable ingesters.
    pub fn find_open_shards(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        unavailable_leaders: &FnvHashSet<NodeId>,
    ) -> Option<(Vec<ShardEntry>, NextShardId)> {
        self.shard_table
            .find_open_shards(index_uid, source_id, unavailable_leaders)
    }

    /// Updates the state and ingestion rate of the shards according to the given shard infos.
    pub fn update_shards(
        &mut self,
        source_uid: &SourceUid,
        shard_infos: &ShardInfos,
    ) -> ShardStats {
        self.shard_table.update_shards(source_uid, shard_infos)
    }

    /// Sets the state of the shards identified by their index UID, source ID, and shard IDs to
    /// `Closed`.
    pub fn close_shards(&mut self, source_uid: &SourceUid, shard_ids: &[ShardId]) -> Vec<ShardId> {
        self.shard_table.close_shards(source_uid, shard_ids)
    }

    /// Removes the shards identified by their index UID, source ID, and shard IDs.
    pub fn delete_shards(&mut self, source_uid: &SourceUid, shard_ids: &[ShardId]) {
        self.shard_table.delete_shards(source_uid, shard_ids);
    }

    pub fn acquire_scaling_permits(
        &mut self,
        source_uid: &SourceUid,
        scaling_mode: ScalingMode,
        num_permits: u64,
    ) -> Option<bool> {
        self.shard_table
            .acquire_scaling_permits(source_uid, scaling_mode, num_permits)
    }

    pub fn release_scaling_permits(
        &mut self,
        source_uid: &SourceUid,
        scaling_mode: ScalingMode,
        num_permits: u64,
    ) {
        self.shard_table
            .release_scaling_permits(source_uid, scaling_mode, num_permits)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::{SourceConfig, SourceParams, INGEST_SOURCE_ID};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::metastore::ListIndexesMetadataResponse;

    use super::*;

    #[tokio::test]
    async fn test_control_plane_model_load_shard_table() {
        let progress = Progress::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|request| {
                assert_eq!(request, ListIndexesMetadataRequest::all());

                let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
                let mut source_config = SourceConfig::ingest_v2_default();
                source_config.enabled = true;
                index_0.add_source(source_config.clone()).unwrap();

                let mut index_1 = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
                index_1.add_source(source_config.clone()).unwrap();

                let mut index_2 = IndexMetadata::for_test("test-index-2", "ram:///test-index-2");
                source_config.enabled = false;
                index_2.add_source(source_config.clone()).unwrap();

                let indexes = vec![index_0, index_1, index_2];
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(indexes).unwrap())
            });
        mock_metastore.expect_list_shards().returning(|request| {
            assert_eq!(request.subrequests.len(), 2);

            assert_eq!(request.subrequests[0].index_uid, "test-index-0:0");
            assert_eq!(request.subrequests[0].source_id, INGEST_SOURCE_ID);
            assert_eq!(
                request.subrequests[0].shard_state(),
                ShardState::Unspecified
            );

            assert_eq!(request.subrequests[1].index_uid, "test-index-1:0");
            assert_eq!(request.subrequests[1].source_id, INGEST_SOURCE_ID);
            assert_eq!(
                request.subrequests[1].shard_state(),
                ShardState::Unspecified
            );

            let subresponses = vec![
                metastore::ListShardsSubresponse {
                    index_uid: "test-index-0:0".to_string(),
                    source_id: INGEST_SOURCE_ID.to_string(),
                    shards: vec![Shard {
                        shard_id: 42,
                        index_uid: "test-index-0:0".to_string(),
                        source_id: INGEST_SOURCE_ID.to_string(),
                        shard_state: ShardState::Open as i32,
                        leader_id: "node1".to_string(),
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

        assert_eq!(model.index_table.len(), 3);
        assert_eq!(
            model.index_uid("test-index-0").unwrap().as_str(),
            "test-index-0:0"
        );
        assert_eq!(
            model.index_uid("test-index-1").unwrap().as_str(),
            "test-index-1:0"
        );
        assert_eq!(
            model.index_uid("test-index-2").unwrap().as_str(),
            "test-index-2:0"
        );

        assert_eq!(model.shard_table.num_shards(), 1);

        let source_uid_0 = SourceUid {
            index_uid: "test-index-0:0".into(),
            source_id: INGEST_SOURCE_ID.to_string(),
        };
        let shards: Vec<&ShardEntry> = model
            .shard_table
            .list_shards(&source_uid_0)
            .unwrap()
            .collect();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, 42);

        let next_shard_id = model.next_shard_id(&source_uid_0).unwrap();
        assert_eq!(next_shard_id, 43);

        let source_uid_1 = SourceUid {
            index_uid: "test-index-1:0".into(),
            source_id: INGEST_SOURCE_ID.to_string(),
        };
        let shards: Vec<&ShardEntry> = model
            .shard_table
            .list_shards(&source_uid_1)
            .unwrap()
            .collect();
        assert_eq!(shards.len(), 0);

        let next_shard_id = model.next_shard_id(&source_uid_1).unwrap();
        assert_eq!(next_shard_id, 1);
    }

    #[test]
    fn test_control_plane_model_toggle_source() {
        let mut model = ControlPlaneModel::default();
        let index_metadata = IndexMetadata::for_test("test-index", "ram://");
        let index_uid = index_metadata.index_uid.clone();
        model.add_index(index_metadata);
        let source_config = SourceConfig::for_test("test-source", SourceParams::void());
        model.add_source(&index_uid, source_config).unwrap();
        {
            let has_changed = model
                .toggle_source(&index_uid, &"test-source".to_string(), true)
                .unwrap();
            assert!(!has_changed);
        }
        {
            let has_changed = model
                .toggle_source(&index_uid, &"test-source".to_string(), true)
                .unwrap();
            assert!(!has_changed);
        }
        {
            let has_changed = model
                .toggle_source(&index_uid, &"test-source".to_string(), false)
                .unwrap();
            assert!(has_changed);
        }
        {
            let has_changed = model
                .toggle_source(&index_uid, &"test-source".to_string(), false)
                .unwrap();
            assert!(!has_changed);
        }
        {
            let has_changed = model
                .toggle_source(&index_uid, &"test-source".to_string(), true)
                .unwrap();
            assert!(has_changed);
        }
        {
            let has_changed = model
                .toggle_source(&index_uid, &"test-source".to_string(), true)
                .unwrap();
            assert!(!has_changed);
        }
    }
}
