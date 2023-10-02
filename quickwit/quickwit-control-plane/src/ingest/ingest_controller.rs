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
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fnv::FnvHashMap;
use itertools::Itertools;
use quickwit_common::Progress;
use quickwit_config::INGEST_SOURCE_ID;
use quickwit_ingest::IngesterPool;
use quickwit_metastore::{ListIndexesQuery, Metastore};
use quickwit_proto::control_plane::{
    ControlPlaneError, ControlPlaneResult, GetOpenShardsSubresponse, GetOrCreateOpenShardsRequest,
    GetOrCreateOpenShardsResponse,
};
use quickwit_proto::ingest::ingester::{IngesterService, PingRequest};
use quickwit_proto::ingest::{IngestV2Error, Shard, ShardState};
use quickwit_proto::metastore::{
    CloseShardsRequest, DeleteShardsRequest, EmptyResponse, EntityKind, MetastoreError,
};
use quickwit_proto::types::{IndexId, NodeId, SourceId};
use quickwit_proto::{metastore, IndexUid, NodeIdRef, ShardId};
use rand::seq::SliceRandom;
use serde::Serialize;
use tokio::time::timeout;
use tracing::{error, info};

const PING_LEADER_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(2)
};

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

/// A table that keeps track of the existing shards for each index and source.
#[derive(Debug, Default)]
struct ShardTable {
    table_entries: HashMap<(IndexUid, SourceId), ShardTableEntry>,
}

impl ShardTable {
    fn len(&self) -> usize {
        self.table_entries.len()
    }

    /// Removes all entries that match the target index ID.
    fn delete_index(&mut self, index_id: &str) {
        self.table_entries
            .retain(|(index_uid, _), _| index_uid.index_id() != index_id);
    }

    /// Adds a new empty entry for the given index and source.
    ///
    /// TODO check and document the behavior on error (if the source was already here).
    fn add_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let key = (index_uid.clone(), source_id.clone());
        let table_entry = ShardTableEntry::default();
        let previous_table_entry_opt = self.table_entries.insert(key, table_entry);

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

    /// Removes any entry that matches the target index UID and source ID.
    fn delete_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let key = (index_uid.clone(), source_id.clone());
        self.table_entries.remove(&key);
    }

    /// Clears the shard table.
    fn clear(&mut self) {
        self.table_entries.clear();
    }

    /// Finds open shards for a given index and source and whose leaders are not in the set of
    /// unavailable ingesters.
    fn find_open_shards(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        unavailable_ingesters: &HashSet<NodeId>,
    ) -> Option<(Vec<Shard>, NextShardId)> {
        let key = (index_uid.clone(), source_id.clone());
        let table_entry = self.table_entries.get(&key)?;
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

    /// Updates the shard table with the shards returned by the metastore.
    fn update_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        shards: &[Shard],
        next_shard_id: NextShardId,
    ) {
        let key = (index_uid.clone(), source_id.clone());
        match self.table_entries.entry(key) {
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
    fn close_shards(&mut self, index_uid: &IndexUid, source_id: &SourceId, shard_ids: &[ShardId]) {
        let key = (index_uid.clone(), source_id.clone());

        if let Some(table_entry) = self.table_entries.get_mut(&key) {
            for shard_id in shard_ids {
                if let Some(shard) = table_entry.shards.get_mut(shard_id) {
                    shard.shard_state = ShardState::Closed as i32;
                }
            }
        }
    }

    /// Removes the shards identified by their index UID, source ID, and shard IDs.
    fn delete_shards(&mut self, index_uid: &IndexUid, source_id: &SourceId, shard_ids: &[ShardId]) {
        let key = (index_uid.clone(), source_id.clone());
        if let Some(table_entry) = self.table_entries.get_mut(&key) {
            for shard_id in shard_ids {
                table_entry.shards.remove(shard_id);
            }
        }
    }
}

pub struct IngestController {
    metastore: Arc<dyn Metastore>,
    ingester_pool: IngesterPool,
    index_table: HashMap<IndexId, IndexUid>,
    shard_table: ShardTable,
    replication_factor: usize,
}

impl fmt::Debug for IngestController {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IngestController")
            .field("replication", &self.metastore)
            .field("ingester_pool", &self.ingester_pool)
            .field("index_table.len", &self.index_table.len())
            .field("shard_table.len", &self.shard_table.len())
            .field("replication_factor", &self.replication_factor)
            .finish()
    }
}

impl IngestController {
    pub fn new(
        metastore: Arc<dyn Metastore>,
        ingester_pool: IngesterPool,
        replication_factor: usize,
    ) -> Self {
        Self {
            metastore,
            ingester_pool,
            index_table: HashMap::new(),
            shard_table: ShardTable::default(),
            replication_factor,
        }
    }

    pub(crate) async fn load_state(&mut self, progress: &Progress) -> ControlPlaneResult<()> {
        info!("syncing internal state with metastore");
        let now = Instant::now();

        self.index_table.clear();
        self.shard_table.clear();

        let indexes = progress
            .protect_future(self.metastore.list_indexes_metadatas(ListIndexesQuery::All))
            .await?;

        self.index_table.reserve(indexes.len());

        let num_indexes = indexes.len();
        let mut num_sources = 0;
        let mut num_shards = 0;

        let mut subrequests = Vec::with_capacity(indexes.len());

        for index in indexes {
            for source_id in index.sources.into_keys() {
                if source_id != INGEST_SOURCE_ID {
                    continue;
                }
                let request = metastore::ListShardsSubrequest {
                    index_uid: index.index_uid.clone().into(),
                    source_id,
                    shard_state: Some(ShardState::Open as i32),
                };
                num_sources += 1;
                subrequests.push(request);
            }
            self.index_table
                .insert(index.index_config.index_id, index.index_uid);
        }
        if !subrequests.is_empty() {
            let list_shards_request = metastore::ListShardsRequest { subrequests };
            let list_shard_response = progress
                .protect_future(self.metastore.list_shards(list_shards_request))
                .await?;

            self.shard_table
                .table_entries
                .reserve(list_shard_response.subresponses.len());

            for list_shards_subresponse in list_shard_response.subresponses {
                num_shards += list_shards_subresponse.shards.len();

                let key = (
                    list_shards_subresponse.index_uid.into(),
                    list_shards_subresponse.source_id,
                );
                let shards: FnvHashMap<ShardId, Shard> = list_shards_subresponse
                    .shards
                    .into_iter()
                    .map(|shard| (shard.shard_id, shard))
                    .collect();
                let table_entry = ShardTableEntry {
                    shards,
                    next_shard_id: list_shards_subresponse.next_shard_id,
                };
                self.shard_table.table_entries.insert(key, table_entry);
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

    /// Pings an ingester to determine whether it is available for hosting a shard. If a follower ID
    /// is provided, the leader candidate is in charge of pinging the follower candidate as
    /// well.
    async fn ping_leader_and_follower(
        &mut self,
        leader_id: &NodeId,
        follower_id_opt: Option<&NodeId>,
        progress: &Progress,
    ) -> Result<(), PingError> {
        let mut leader_ingester = self
            .ingester_pool
            .get(leader_id)
            .ok_or(PingError::LeaderUnavailable)?;

        let ping_request = PingRequest {
            leader_id: leader_id.clone().into(),
            follower_id: follower_id_opt
                .cloned()
                .map(|follower_id| follower_id.into()),
        };
        progress.protect_future(timeout(
            PING_LEADER_TIMEOUT,
            leader_ingester.ping(ping_request),
        ))
        .await
        .map_err(|_| PingError::LeaderUnavailable)? // The leader timed out.
        .map_err(|error| {
            if let Some(follower_id) = follower_id_opt {
                if matches!(error, IngestV2Error::IngesterUnavailable { ingester_id } if ingester_id == *follower_id) {
                    return PingError::FollowerUnavailable;
                }
            }
            PingError::LeaderUnavailable
        })?;
        Ok(())
    }

    /// Finds an available leader-follower pair to host a shard. If the replication factor is set to
    /// 1, only a leader is returned. If no nodes are available, `None` is returned.
    async fn find_leader_and_follower(
        &mut self,
        unavailable_ingesters: &mut HashSet<NodeId>,
        progress: &Progress,
    ) -> Option<(NodeId, Option<NodeId>)> {
        let mut candidates: Vec<NodeId> = self
            .ingester_pool
            .keys()
            .into_iter()
            .filter(|node_id| !unavailable_ingesters.contains(node_id))
            .collect();
        candidates.shuffle(&mut rand::thread_rng());

        #[cfg(test)]
        candidates.sort();

        if self.replication_factor == 1 {
            for leader_id in candidates {
                if unavailable_ingesters.contains(&leader_id) {
                    continue;
                }
                if self
                    .ping_leader_and_follower(&leader_id, None, progress)
                    .await
                    .is_ok()
                {
                    return Some((leader_id, None));
                }
            }
        } else {
            for (leader_id, follower_id) in candidates.into_iter().tuple_combinations() {
                // We must perform this check here since the `unavailable_ingesters` set can grow as
                // we go through the loop.
                if unavailable_ingesters.contains(&leader_id)
                    || unavailable_ingesters.contains(&follower_id)
                {
                    continue;
                }
                match self
                    .ping_leader_and_follower(&leader_id, Some(&follower_id), progress)
                    .await
                {
                    Ok(_) => return Some((leader_id, Some(follower_id))),
                    Err(PingError::LeaderUnavailable) => {
                        unavailable_ingesters.insert(leader_id);
                    }
                    Err(PingError::FollowerUnavailable) => {
                        // We do not mark the follower as unavailable here. The issue could be
                        // specific to the link between the leader and follower. We define
                        // unavailability as being unavailable from the point of view of the control
                        // plane.
                    }
                }
            }
        }
        None
    }

    /// Finds the open shards that satisfies the [`GetOrCreateOpenShardsRequest`] request sent by an
    /// ingest router. First, the control plane checks its internal shard table to find
    /// candidates. If it does not contain any, the control plane will ask
    /// the metastore to open new shards.
    pub(crate) async fn get_or_create_open_shards(
        &mut self,
        get_open_shards_request: GetOrCreateOpenShardsRequest,
        progress: &Progress,
    ) -> ControlPlaneResult<GetOrCreateOpenShardsResponse> {
        let mut get_open_shards_subresponses =
            Vec::with_capacity(get_open_shards_request.subrequests.len());

        let mut unavailable_ingesters: HashSet<NodeId> = get_open_shards_request
            .unavailable_ingesters
            .into_iter()
            .map(|ingester_id| ingester_id.into())
            .collect();

        let mut open_shards_subrequests = Vec::new();

        for get_open_shards_subrequest in get_open_shards_request.subrequests {
            let index_uid = self
                .index_table
                .get(&get_open_shards_subrequest.index_id)
                .cloned()
                .ok_or_else(|| {
                    MetastoreError::NotFound(EntityKind::Index {
                        index_id: get_open_shards_subrequest.index_id.clone(),
                    })
                })?;
            // if !get_open_shards_subrequest.closed_shards.is_empty() {
            //     self.shard_table.delete_shards(
            //         &index_uid,
            //         &get_open_shards_subrequest.source_id,
            //         &get_open_shards_subrequest.closed_shards,
            //     );
            // }
            let (open_shards, next_shard_id) = self
                .shard_table
                .find_open_shards(
                    &index_uid,
                    &get_open_shards_subrequest.source_id,
                    &unavailable_ingesters,
                )
                .ok_or_else(|| {
                    MetastoreError::NotFound(EntityKind::Source {
                        index_id: get_open_shards_subrequest.index_id.clone(),
                        source_id: get_open_shards_subrequest.source_id.clone(),
                    })
                })?;
            if !open_shards.is_empty() {
                let get_open_shards_subresponse = GetOpenShardsSubresponse {
                    index_uid: index_uid.into(),
                    source_id: get_open_shards_subrequest.source_id,
                    open_shards,
                };
                get_open_shards_subresponses.push(get_open_shards_subresponse);
            } else {
                // TODO: Find leaders in batches.
                // TODO: Round-robin leader-follower pairs or choose according to load.
                let (leader_id, follower_id) = self
                    .find_leader_and_follower(&mut unavailable_ingesters, progress)
                    .await
                    .ok_or_else(|| {
                        ControlPlaneError::Unavailable("no available ingester".to_string())
                    })?;
                let open_shards_subrequest = metastore::OpenShardsSubrequest {
                    index_uid: index_uid.into(),
                    source_id: get_open_shards_subrequest.source_id,
                    leader_id: leader_id.into(),
                    follower_id: follower_id.map(|follower_id| follower_id.into()),
                    next_shard_id,
                };
                open_shards_subrequests.push(open_shards_subrequest);
            }
        }
        if !open_shards_subrequests.is_empty() {
            let open_shards_request = metastore::OpenShardsRequest {
                subrequests: open_shards_subrequests,
            };
            let open_shards_response = progress
                .protect_future(self.metastore.open_shards(open_shards_request))
                .await?;
            for open_shards_subresponse in &open_shards_response.subresponses {
                let index_uid: IndexUid = open_shards_subresponse.index_uid.clone().into();
                self.shard_table.update_shards(
                    &index_uid,
                    &open_shards_subresponse.source_id,
                    &open_shards_subresponse.open_shards,
                    open_shards_subresponse.next_shard_id,
                );
            }
            for open_shards_subresponse in open_shards_response.subresponses {
                let get_open_shards_subresponse = GetOpenShardsSubresponse {
                    index_uid: open_shards_subresponse.index_uid,
                    source_id: open_shards_subresponse.source_id,
                    open_shards: open_shards_subresponse.open_shards,
                };
                get_open_shards_subresponses.push(get_open_shards_subresponse);
            }
        }
        let get_open_shards_response = GetOrCreateOpenShardsResponse {
            subresponses: get_open_shards_subresponses,
        };
        Ok(get_open_shards_response)
    }

    pub(crate) async fn close_shards(
        &mut self,
        close_shards_request: CloseShardsRequest,
    ) -> ControlPlaneResult<EmptyResponse> {
        for close_shards_subrequest in close_shards_request.subrequests {
            let index_uid: IndexUid = close_shards_subrequest.index_uid.into();
            let source_id = close_shards_subrequest.source_id;
            // TODO: Group by (index_uid, source_id) first, or change schema of
            // `CloseShardsSubrequest`.
            let shard_ids = [close_shards_subrequest.shard_id];

            self.shard_table
                .close_shards(&index_uid, &source_id, &shard_ids)
        }
        Ok(EmptyResponse {})
    }

    pub(crate) async fn delete_shards(
        &mut self,
        delete_shards_request: DeleteShardsRequest,
    ) -> ControlPlaneResult<EmptyResponse> {
        for delete_shards_subrequest in delete_shards_request.subrequests {
            let index_uid: IndexUid = delete_shards_subrequest.index_uid.into();
            let source_id = delete_shards_subrequest.source_id;
            let shard_ids = delete_shards_subrequest.shard_ids;

            self.shard_table
                .delete_shards(&index_uid, &source_id, &shard_ids)
        }
        Ok(EmptyResponse {})
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PingError {
    LeaderUnavailable,
    FollowerUnavailable,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct IngestControllerState {
    pub num_indexes: usize,
}

impl IngestController {
    pub fn observable_state(&self) -> IngestControllerState {
        IngestControllerState {
            num_indexes: self.index_table.len(),
        }
    }

    pub(crate) fn create_index(&mut self, index_uid: IndexUid) {
        let index_uid = index_uid;
        let index_id = index_uid.index_id().to_string();
        self.index_table.insert(index_id, index_uid);
    }

    pub(crate) fn delete_index(&mut self, index_uid: &IndexUid) {
        // TODO: We need to let the routers and ingesters know.
        self.index_table.remove(index_uid.index_id());
        self.shard_table.delete_index(index_uid.index_id());
    }

    pub(crate) fn add_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        self.shard_table.add_source(index_uid, source_id);
    }

    pub(crate) fn delete_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        self.shard_table.delete_source(index_uid, source_id);
    }
}

#[cfg(test)]
mod tests {

    use quickwit_config::SourceConfig;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::control_plane::GetOrCreateOpenShardsSubrequest;
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, MockIngesterService, PingResponse,
    };
    use quickwit_proto::ingest::{IngestV2Error, Shard};

    use super::*;

    #[test]
    fn test_shard_table_add_source() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        shard_table.add_source(&index_uid, &source_id);
        assert_eq!(shard_table.table_entries.len(), 1);

        let key = (index_uid.clone(), source_id.clone());
        let table_entry = shard_table.table_entries.get(&key).unwrap();
        assert!(table_entry.shards.is_empty());
        assert_eq!(table_entry.next_shard_id, 1);
    }

    #[test]
    fn test_shard_table_find_open_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();
        shard_table.add_source(&index_uid, &source_id);

        let mut unavailable_ingesters = HashSet::new();

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

        let key = (index_uid_0.clone(), source_id.clone());
        let table_entry = shard_table.table_entries.get(&key).unwrap();
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

        let key = (index_uid_0.clone(), source_id.clone());
        let table_entry = shard_table.table_entries.get(&key).unwrap();
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

        let key = (index_uid_0.clone(), source_id.clone());
        let table_entry = shard_table.table_entries.get(&key).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], shard_01);
        assert_eq!(table_entry.next_shard_id, 3);

        let key = (index_uid_1.clone(), source_id.clone());
        let table_entry = shard_table.table_entries.get(&key).unwrap();
        assert_eq!(table_entry.shards.len(), 0);
        assert_eq!(table_entry.next_shard_id, 2);
    }

    #[tokio::test]
    async fn test_ingest_controller_load_shard_table() {
        let progress = Progress::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_list_indexes_metadatas()
            .returning(|query| {
                assert!(matches!(query, ListIndexesQuery::All));

                let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
                let source = SourceConfig::ingest_default();
                index_0.add_source(source.clone()).unwrap();

                let mut index_1 = IndexMetadata::for_test("test-index-1", "ram:///test-index-1");
                index_1.add_source(source).unwrap();

                let indexes = vec![index_0, index_1];
                Ok(indexes)
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
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool, replication_factor);

        ingest_controller.load_state(&progress).await.unwrap();

        assert_eq!(ingest_controller.index_table.len(), 2);
        assert_eq!(
            ingest_controller.index_table["test-index-0"],
            "test-index-0:0"
        );
        assert_eq!(
            ingest_controller.index_table["test-index-1"],
            "test-index-1:0"
        );

        assert_eq!(ingest_controller.shard_table.table_entries.len(), 2);

        let key = ("test-index-0:0".into(), INGEST_SOURCE_ID.to_string());
        let table_entry = ingest_controller
            .shard_table
            .table_entries
            .get(&key)
            .unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, 42);
        assert_eq!(table_entry.next_shard_id, 43);

        let key = ("test-index-1:0".into(), INGEST_SOURCE_ID.to_string());
        let table_entry = ingest_controller
            .shard_table
            .table_entries
            .get(&key)
            .unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 0);
        assert_eq!(table_entry.next_shard_id, 1);
    }

    #[tokio::test]
    async fn test_ingest_controller_ping_leader() {
        let progress = Progress::default();

        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let leader_id: NodeId = "test-ingester-0".into();
        let error = ingest_controller
            .ping_leader_and_follower(&leader_id, None, &progress)
            .await
            .unwrap_err();
        assert!(matches!(error, PingError::LeaderUnavailable));

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert!(request.follower_id.is_none());

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        ingest_controller
            .ping_leader_and_follower(&leader_id, None, &progress)
            .await
            .unwrap();

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert!(request.follower_id.is_none());

            let leader_id: NodeId = "test-ingester-0".into();
            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: leader_id,
            })
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let error = ingest_controller
            .ping_leader_and_follower(&leader_id, None, &progress)
            .await
            .unwrap_err();
        assert!(matches!(error, PingError::LeaderUnavailable));

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-1");

            let follower_id: NodeId = "test-ingester-1".into();
            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: follower_id,
            })
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let follower_id: NodeId = "test-ingester-1".into();
        let error = ingest_controller
            .ping_leader_and_follower(&leader_id, Some(&follower_id), &progress)
            .await
            .unwrap_err();
        assert!(matches!(error, PingError::FollowerUnavailable));
    }

    #[tokio::test]
    async fn test_ingest_controller_find_leader_replication_factor_1() {
        let progress = Progress::default();

        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut HashSet::new(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().times(2).returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert!(request.follower_id.is_none());

            Err(IngestV2Error::Internal("Io error".to_string()))
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut HashSet::new(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-1");
            assert!(request.follower_id.is_none());

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-1".into(), ingester);

        let (leader_id, follower_id) = ingest_controller
            .find_leader_and_follower(&mut HashSet::new(), &progress)
            .await
            .unwrap();
        assert_eq!(leader_id.as_str(), "test-ingester-1");
        assert!(follower_id.is_none());
    }

    #[tokio::test]
    async fn test_ingest_controller_find_leader_replication_factor_2() {
        let progress = Progress::default();

        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut HashSet::new(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-1");

            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: "test-ingester-1".into(),
            })
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().returning(|_request| {
            panic!("`test-ingester-1` should not be pinged.");
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-1".into(), ingester.clone());

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&mut HashSet::new(), &progress)
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-1");

            Err(IngestV2Error::IngesterUnavailable {
                ingester_id: "test-ingester-1".into(),
            })
        });
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-2");

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-0".into(), ingester.clone());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().returning(|_request| {
            panic!("`test-ingester-2` should not be pinged.");
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-2".into(), ingester.clone());

        let (leader_id, follower_id) = ingest_controller
            .find_leader_and_follower(&mut HashSet::new(), &progress)
            .await
            .unwrap();
        assert_eq!(leader_id.as_str(), "test-ingester-0");
        assert_eq!(follower_id.unwrap().as_str(), "test-ingester-2");
    }

    #[tokio::test]
    async fn test_ingest_controller_get_open_shards() {
        let index_id_0 = "test-index-0".to_string();
        let index_uid_0: IndexUid = "test-index-0:0".into();

        let index_id_1 = "test-index-1".to_string();
        let index_uid_1: IndexUid = "test-index-1:0".into();

        let source_id = "test-source".to_string();

        let progress = Progress::default();

        let mut mock_metastore = MockMetastore::default();
        mock_metastore
            .expect_open_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);
                assert_eq!(request.subrequests[0].index_uid, "test-index-1:0");
                assert_eq!(request.subrequests[0].source_id, "test-source");

                let subresponses = vec![metastore::OpenShardsSubresponse {
                    index_uid: "test-index-1:0".to_string(),
                    source_id: "test-source".to_string(),
                    open_shards: vec![Shard {
                        shard_id: 1,
                        leader_id: "test-ingester-2".to_string(),
                        ..Default::default()
                    }],
                    next_shard_id: 2,
                }];
                let response = metastore::OpenShardsResponse { subresponses };
                Ok(response)
            });
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-1");
            assert_eq!(request.follower_id.unwrap(), "test-ingester-2");

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-1".into(), ingester.clone());

        let mock_ingester = MockIngesterService::default();
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool.insert("test-ingester-2".into(), ingester.clone());

        let replication_factor = 2;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        ingest_controller
            .index_table
            .insert(index_id_0.clone(), index_uid_0.clone());
        ingest_controller
            .index_table
            .insert(index_id_1.clone(), index_uid_1.clone());
        ingest_controller
            .shard_table
            .add_source(&index_uid_0, &source_id);
        ingest_controller
            .shard_table
            .add_source(&index_uid_1, &source_id);

        let shards = vec![
            Shard {
                shard_id: 1,
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
            Shard {
                shard_id: 2,
                leader_id: "test-ingester-1".to_string(),
                shard_state: ShardState::Open as i32,
                ..Default::default()
            },
        ];
        ingest_controller
            .shard_table
            .update_shards(&index_uid_0, &source_id, &shards, 3);

        let request = GetOrCreateOpenShardsRequest {
            subrequests: Vec::new(),
            unavailable_ingesters: Vec::new(),
        };
        let response = ingest_controller
            .get_or_create_open_shards(request, &progress)
            .await
            .unwrap();
        assert_eq!(response.subresponses.len(), 0);

        let subrequests = vec![
            GetOrCreateOpenShardsSubrequest {
                index_id: "test-index-0".to_string(),
                source_id: source_id.clone(),
                closed_shards: Vec::new(),
            },
            GetOrCreateOpenShardsSubrequest {
                index_id: "test-index-1".to_string(),
                source_id: source_id.clone(),
                closed_shards: Vec::new(),
            },
        ];
        let unavailable_ingesters = vec!["test-ingester-0".to_string()];
        let request = GetOrCreateOpenShardsRequest {
            subrequests,
            unavailable_ingesters,
        };
        let response = ingest_controller
            .get_or_create_open_shards(request, &progress)
            .await
            .unwrap();
        assert_eq!(response.subresponses.len(), 2);

        assert_eq!(response.subresponses[0].index_uid, "test-index-0:0");
        assert_eq!(response.subresponses[0].source_id, source_id);
        assert_eq!(response.subresponses[0].open_shards.len(), 1);
        assert_eq!(response.subresponses[0].open_shards[0].shard_id, 2);
        assert_eq!(
            response.subresponses[0].open_shards[0].leader_id,
            "test-ingester-1"
        );

        assert_eq!(response.subresponses[1].index_uid, "test-index-1:0");
        assert_eq!(response.subresponses[1].source_id, source_id);
        assert_eq!(response.subresponses[1].open_shards.len(), 1);
        assert_eq!(response.subresponses[1].open_shards[0].shard_id, 1);
        assert_eq!(
            response.subresponses[1].open_shards[0].leader_id,
            "test-ingester-2"
        );

        assert_eq!(ingest_controller.shard_table.table_entries.len(), 2);
    }

    #[tokio::test]
    async fn test_ingest_controller_close_shards() {
        // TODO: Write test when the RPC is actually called by ingesters.
    }
}
