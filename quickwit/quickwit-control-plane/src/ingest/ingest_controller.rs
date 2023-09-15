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
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler};
use quickwit_config::INGEST_SOURCE_ID;
use quickwit_ingest::IngesterPool;
use quickwit_metastore::{ListIndexesQuery, Metastore};
use quickwit_proto::control_plane::{
    CloseShardsRequest, CloseShardsResponse, ControlPlaneError, ControlPlaneResult,
    GetOpenShardsRequest, GetOpenShardsResponse, GetOpenShardsSubresponse,
};
use quickwit_proto::ingest::ingester::{IngesterService, PingRequest};
use quickwit_proto::ingest::{IngestV2Error, Shard, ShardState};
use quickwit_proto::metastore::events::{
    AddSourceEvent, CreateIndexEvent, DeleteIndexEvent, DeleteSourceEvent,
};
use quickwit_proto::metastore::{EntityKind, MetastoreError};
use quickwit_proto::types::{IndexId, NodeId, SourceId};
use quickwit_proto::{metastore, IndexUid, NodeIdRef, ShardId};
use rand::seq::SliceRandom;
use serde_json::{json, Value as JsonValue};
use tokio::time::timeout;
use tracing::{error, info};

const PING_LEADER_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(2)
};

type NextShardId = ShardId;

/// A wrapper around `Shard` that implements `Hash` to allow insertion of shards into a `HashSet`.
struct ShardEntry(Shard);

impl fmt::Debug for ShardEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardEntry")
            .field("shard_id", &self.0.shard_id)
            .field("leader_id", &self.0.leader_id)
            .field("follower_id", &self.0.follower_id)
            .field("shard_state", &self.0.shard_state)
            .finish()
    }
}

impl Deref for ShardEntry {
    type Target = Shard;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for ShardEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.shard_id.hash(state);
    }
}

impl PartialEq for ShardEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.shard_id == other.0.shard_id
    }
}

impl Eq for ShardEntry {}

#[derive(Debug, Eq, PartialEq)]
struct ShardTableEntry {
    shard_entries: HashSet<ShardEntry>,
    next_shard_id: NextShardId,
}

impl Default for ShardTableEntry {
    fn default() -> Self {
        Self {
            shard_entries: HashSet::new(),
            next_shard_id: 1, // `1` matches the PostgreSQL sequence min value.
        }
    }
}

impl ShardTableEntry {
    #[cfg(test)]
    fn shards(&self) -> Vec<Shard> {
        self.shard_entries
            .iter()
            .map(|shard_entry| shard_entry.0.clone())
            .sorted_by_key(|shard| shard.shard_id)
            .collect()
    }
}

/// A table that keeps track of the existing shards for each index and source.
#[derive(Debug, Default)]
struct ShardTable {
    table_entries: HashMap<(IndexUid, SourceId), ShardTableEntry>,
}

impl ShardTable {
    /// Adds a new empty entry for the given index and source.
    fn add_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let key = (index_uid.clone(), source_id.clone());
        let table_entry = ShardTableEntry::default();
        let previous_table_entry_opt = self.table_entries.insert(key, table_entry);

        if let Some(previous_table_entry) = previous_table_entry_opt {
            if previous_table_entry != ShardTableEntry::default() {
                error!(
                    "shard table entry for index `{}` and source `{}` already exists",
                    index_uid.index_id(),
                    source_id
                );
            }
        }
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
            .shard_entries
            .iter()
            .filter(|shard_entry| {
                shard_entry.is_open()
                    && !unavailable_ingesters.contains(NodeIdRef::from_str(&shard_entry.leader_id))
            })
            .map(|shard_entry| shard_entry.0.clone())
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
                    let shard_entry = ShardEntry(shard.clone());
                    table_entry.shard_entries.replace(shard_entry);
                    table_entry.next_shard_id = next_shard_id;
                }
            }
            // This should never happen if the control plane view is consistent with the state of
            // the metastore, so should we panic here? Warnings are most likely going to go
            // unnoticed.
            Entry::Vacant(entry) => {
                let shard_entries: HashSet<ShardEntry> =
                    shards.iter().cloned().map(ShardEntry).collect();
                let table_entry = ShardTableEntry {
                    shard_entries,
                    next_shard_id,
                };
                entry.insert(table_entry);
            }
        }
    }

    /// Evicts the shards identified by their index UID, source ID, and shard IDs from the shard
    /// table.
    fn remove_shards(&mut self, index_uid: &IndexUid, source_id: &SourceId, shard_ids: &[ShardId]) {
        let key = (index_uid.clone(), source_id.clone());

        if let Some(table_entry) = self.table_entries.get_mut(&key) {
            table_entry
                .shard_entries
                .retain(|shard_entry| !shard_ids.contains(&shard_entry.shard_id));
        }
    }

    /// Removes all the entries that match the target index ID.
    fn remove_index(&mut self, index_id: &str) {
        self.table_entries
            .retain(|(index_uid, _), _| index_uid.index_id() != index_id);
    }

    fn remove_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let key = (index_uid.clone(), source_id.clone());
        self.table_entries.remove(&key);
    }
}

pub struct IngestController {
    metastore: Arc<dyn Metastore>,
    ingester_pool: IngesterPool,
    index_table: HashMap<IndexId, IndexUid>,
    shard_table: ShardTable,
    replication_factor: usize,
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

    async fn load_state(&mut self, ctx: &ActorContext<Self>) -> ControlPlaneResult<()> {
        info!("syncing internal state with metastore");
        let now = Instant::now();

        self.index_table.clear();
        self.shard_table.clear();

        let indexes = ctx
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
            let list_shard_response = ctx
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
                let shard_entries: HashSet<ShardEntry> = list_shards_subresponse
                    .shards
                    .into_iter()
                    .map(ShardEntry)
                    .collect();
                let table_entry = ShardTableEntry {
                    shard_entries,
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
        ctx: &ActorContext<Self>,
        leader_id: &NodeId,
        follower_id_opt: Option<&NodeId>,
    ) -> Result<(), PingError> {
        let mut leader_ingester = self
            .ingester_pool
            .get(leader_id)
            .await
            .ok_or(PingError::LeaderUnavailable)?;

        let ping_request = PingRequest {
            leader_id: leader_id.clone().into(),
            follower_id: follower_id_opt
                .cloned()
                .map(|follower_id| follower_id.into()),
        };
        ctx.protect_future(timeout(
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
        ctx: &ActorContext<Self>,
        unavailable_ingesters: &mut HashSet<NodeId>,
    ) -> Option<(NodeId, Option<NodeId>)> {
        let mut candidates: Vec<NodeId> = self.ingester_pool.keys().await;
        candidates.retain(|node_id| !unavailable_ingesters.contains(node_id));
        candidates.shuffle(&mut rand::thread_rng());

        #[cfg(test)]
        candidates.sort();

        if self.replication_factor == 1 {
            for leader_id in candidates {
                if unavailable_ingesters.contains(&leader_id) {
                    continue;
                }
                if self
                    .ping_leader_and_follower(ctx, &leader_id, None)
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
                    .ping_leader_and_follower(ctx, &leader_id, Some(&follower_id))
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

    /// Finds the open shards that satisfies the [`GetOpenShardsRequest`] request sent by an
    /// ingest router. First, the control plane checks its internal shard table to find
    /// candidates. If it does not contain any, the control plane will ask
    /// the metastore to open new shards.
    async fn get_open_shards(
        &mut self,
        ctx: &ActorContext<Self>,
        get_open_shards_request: GetOpenShardsRequest,
    ) -> ControlPlaneResult<GetOpenShardsResponse> {
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
                    .find_leader_and_follower(ctx, &mut unavailable_ingesters)
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
            let open_shards_response = ctx
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
        let get_open_shards_response = GetOpenShardsResponse {
            subresponses: get_open_shards_subresponses,
        };
        Ok(get_open_shards_response)
    }

    async fn close_shards(
        &mut self,
        ctx: &ActorContext<Self>,
        close_shards_request: CloseShardsRequest,
    ) -> ControlPlaneResult<CloseShardsResponse> {
        let mut close_shards_subrequests =
            Vec::with_capacity(close_shards_request.subrequests.len());
        for close_shards_subrequest in close_shards_request.subrequests {
            let close_shards_subrequest = metastore::CloseShardsSubrequest {
                index_uid: close_shards_subrequest.index_uid,
                source_id: close_shards_subrequest.source_id,
                shard_id: close_shards_subrequest.shard_id,
                shard_state: close_shards_subrequest.shard_state,
                replication_position_inclusive: close_shards_subrequest
                    .replication_position_inclusive,
            };
            close_shards_subrequests.push(close_shards_subrequest);
        }
        let metastore_close_shards_request = metastore::CloseShardsRequest {
            subrequests: close_shards_subrequests,
        };
        let close_shards_response = ctx
            .protect_future(self.metastore.close_shards(metastore_close_shards_request))
            .await?;
        for close_shards_success in close_shards_response.successes {
            let index_uid: IndexUid = close_shards_success.index_uid.into();
            self.shard_table.remove_shards(
                &index_uid,
                &close_shards_success.source_id,
                &[close_shards_success.shard_id],
            );
        }
        let close_shards_response = CloseShardsResponse {};
        Ok(close_shards_response)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PingError {
    LeaderUnavailable,
    FollowerUnavailable,
}

#[async_trait]
impl Actor for IngestController {
    type ObservableState = JsonValue;

    fn observable_state(&self) -> Self::ObservableState {
        json!({
            "num_indexes": self.index_table.len(),
        })
    }

    fn name(&self) -> String {
        "IngestController".to_string()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.load_state(ctx)
            .await
            .context("failed to initialize ingest controller")?;
        Ok(())
    }
}

#[async_trait]
impl Handler<CreateIndexEvent> for IngestController {
    type Reply = ControlPlaneResult<()>;

    async fn handle(
        &mut self,
        event: CreateIndexEvent,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid = event.index_uid;
        let index_id = index_uid.index_id().to_string();

        self.index_table.insert(index_id, index_uid);
        Ok(Ok(()))
    }
}

#[async_trait]
impl Handler<DeleteIndexEvent> for IngestController {
    type Reply = ControlPlaneResult<()>;

    async fn handle(
        &mut self,
        event: DeleteIndexEvent,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        // TODO: We need to let the routers and ingesters know.
        self.index_table.remove(event.index_uid.index_id());
        self.shard_table.remove_index(event.index_uid.index_id());
        Ok(Ok(()))
    }
}

#[async_trait]
impl Handler<AddSourceEvent> for IngestController {
    type Reply = ControlPlaneResult<()>;

    async fn handle(
        &mut self,
        event: AddSourceEvent,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.shard_table
            .add_source(&event.index_uid, &event.source_id);
        Ok(Ok(()))
    }
}

#[async_trait]
impl Handler<DeleteSourceEvent> for IngestController {
    type Reply = ControlPlaneResult<()>;

    async fn handle(
        &mut self,
        event: DeleteSourceEvent,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        self.shard_table
            .remove_source(&event.index_uid, &event.source_id);
        Ok(Ok(()))
    }
}

#[async_trait]
impl Handler<GetOpenShardsRequest> for IngestController {
    type Reply = ControlPlaneResult<GetOpenShardsResponse>;

    async fn handle(
        &mut self,
        request: GetOpenShardsRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response_res = self.get_open_shards(ctx, request).await;
        Ok(response_res)
    }
}

#[async_trait]
impl Handler<CloseShardsRequest> for IngestController {
    type Reply = ControlPlaneResult<CloseShardsResponse>;

    async fn handle(
        &mut self,
        request: CloseShardsRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let response_res = self.close_shards(ctx, request).await;
        Ok(response_res)
    }
}

#[cfg(test)]
mod tests {

    use quickwit_actors::Universe;
    use quickwit_config::SourceConfig;
    use quickwit_metastore::{IndexMetadata, MockMetastore};
    use quickwit_proto::control_plane::GetOpenShardsSubrequest;
    use quickwit_proto::ingest::ingester::{
        IngesterServiceClient, MockIngesterService, PingResponse,
    };
    use quickwit_proto::ingest::{IngestV2Error, Shard};
    use tokio::sync::watch;

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
        assert!(table_entry.shard_entries.is_empty());
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
    fn test_shard_table_remove_shards() {
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
        shard_table.remove_shards(&index_uid_0, &source_id, &[2]);
        shard_table.remove_shards(&index_uid_1, &source_id, &[1]);

        assert_eq!(shard_table.table_entries.len(), 2);

        let key = (index_uid_0.clone(), source_id.clone());
        let table_entry = shard_table.table_entries.get(&key).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], shard_01);
        assert_eq!(table_entry.next_shard_id, 3);

        let key = (index_uid_1.clone(), source_id.clone());
        let table_entry = shard_table.table_entries.get(&key).unwrap();
        assert_eq!(table_entry.shard_entries.len(), 0);
        assert_eq!(table_entry.next_shard_id, 2);
    }

    #[tokio::test]
    async fn test_ingest_controller_load_shard_table() {
        let universe = Universe::with_accelerated_time();
        let (mailbox, _inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx = ActorContext::for_test(&universe, mailbox, observable_state_tx);

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

        ingest_controller.load_state(&ctx).await.unwrap();

        assert_eq!(ingest_controller.index_table.len(), 2);
        assert_eq!(
            ingest_controller.index_table["test-index-0"],
            "test-index-0:0".into()
        );
        assert_eq!(
            ingest_controller.index_table["test-index-1"],
            "test-index-1:0".into()
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
        let universe = Universe::new();
        let (mailbox, _inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx = ActorContext::for_test(&universe, mailbox, observable_state_tx);

        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let leader_id: NodeId = "test-ingester-0".into();
        let error = ingest_controller
            .ping_leader_and_follower(&ctx, &leader_id, None)
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
        ingester_pool
            .insert("test-ingester-0".into(), ingester.clone())
            .await;

        ingest_controller
            .ping_leader_and_follower(&ctx, &leader_id, None)
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
        ingester_pool
            .insert("test-ingester-0".into(), ingester.clone())
            .await;

        let error = ingest_controller
            .ping_leader_and_follower(&ctx, &leader_id, None)
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
        ingester_pool
            .insert("test-ingester-0".into(), ingester.clone())
            .await;

        let follower_id: NodeId = "test-ingester-1".into();
        let error = ingest_controller
            .ping_leader_and_follower(&ctx, &leader_id, Some(&follower_id))
            .await
            .unwrap_err();
        assert!(matches!(error, PingError::FollowerUnavailable));
    }

    #[tokio::test]
    async fn test_ingest_controller_find_leader_replication_factor_1() {
        let universe = Universe::with_accelerated_time();
        let (mailbox, _inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx = ActorContext::for_test(&universe, mailbox, observable_state_tx);

        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 1;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&ctx, &mut HashSet::new())
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().times(2).returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-0");
            assert!(request.follower_id.is_none());

            Err(IngestV2Error::Internal("Io error".to_string()))
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool
            .insert("test-ingester-0".into(), ingester.clone())
            .await;

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&ctx, &mut HashSet::new())
            .await;
        assert!(leader_follower_pair.is_none());

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().once().returning(|request| {
            assert_eq!(request.leader_id, "test-ingester-1");
            assert!(request.follower_id.is_none());

            Ok(PingResponse {})
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool
            .insert("test-ingester-1".into(), ingester)
            .await;

        let (leader_id, follower_id) = ingest_controller
            .find_leader_and_follower(&ctx, &mut HashSet::new())
            .await
            .unwrap();
        assert_eq!(leader_id.as_str(), "test-ingester-1");
        assert!(follower_id.is_none());
    }

    #[tokio::test]
    async fn test_ingest_controller_find_leader_replication_factor_2() {
        let universe = Universe::with_accelerated_time();
        let (mailbox, _inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx = ActorContext::for_test(&universe, mailbox, observable_state_tx);

        let mock_metastore = MockMetastore::default();
        let metastore = Arc::new(mock_metastore);
        let ingester_pool = IngesterPool::default();
        let replication_factor = 2;
        let mut ingest_controller =
            IngestController::new(metastore, ingester_pool.clone(), replication_factor);

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&ctx, &mut HashSet::new())
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
        ingester_pool
            .insert("test-ingester-0".into(), ingester.clone())
            .await;

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().returning(|_request| {
            panic!("`test-ingester-1` should not be pinged.");
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool
            .insert("test-ingester-1".into(), ingester.clone())
            .await;

        let leader_follower_pair = ingest_controller
            .find_leader_and_follower(&ctx, &mut HashSet::new())
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
        ingester_pool
            .insert("test-ingester-0".into(), ingester.clone())
            .await;

        let mut mock_ingester = MockIngesterService::default();
        mock_ingester.expect_ping().returning(|_request| {
            panic!("`test-ingester-2` should not be pinged.");
        });
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool
            .insert("test-ingester-2".into(), ingester.clone())
            .await;

        let (leader_id, follower_id) = ingest_controller
            .find_leader_and_follower(&ctx, &mut HashSet::new())
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

        let universe = Universe::with_accelerated_time();
        let (mailbox, _inbox) = universe.create_test_mailbox();
        let (observable_state_tx, _observable_state_rx) = watch::channel(json!({}));
        let ctx = ActorContext::for_test(&universe, mailbox, observable_state_tx);

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
        ingester_pool
            .insert("test-ingester-1".into(), ingester.clone())
            .await;

        let mock_ingester = MockIngesterService::default();
        let ingester: IngesterServiceClient = mock_ingester.into();
        ingester_pool
            .insert("test-ingester-2".into(), ingester.clone())
            .await;

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

        let request = GetOpenShardsRequest {
            subrequests: Vec::new(),
            unavailable_ingesters: Vec::new(),
        };
        let response = ingest_controller
            .get_open_shards(&ctx, request)
            .await
            .unwrap();
        assert_eq!(response.subresponses.len(), 0);

        let subrequests = vec![
            GetOpenShardsSubrequest {
                index_id: "test-index-0".to_string(),
                source_id: source_id.clone(),
            },
            GetOpenShardsSubrequest {
                index_id: "test-index-1".to_string(),
                source_id: source_id.clone(),
            },
        ];
        let unavailable_ingesters = vec!["test-ingester-0".to_string()];
        let request = GetOpenShardsRequest {
            subrequests,
            unavailable_ingesters,
        };
        let response = ingest_controller
            .get_open_shards(&ctx, request)
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
