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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use quickwit_proto::ingest::{Shard, ShardIds, ShardState};
use quickwit_proto::types::{IndexId, IndexUid, NodeId, ShardId, SourceId};
use tracing::{info, warn};

use crate::IngesterPool;

#[derive(Debug)]
pub(super) struct RoutingEntry {
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub shard_id: ShardId,
    pub shard_state: ShardState,
    pub leader_id: NodeId,
}

impl From<Shard> for RoutingEntry {
    fn from(shard: Shard) -> Self {
        let shard_id = shard.shard_id().clone();
        let shard_state = shard.shard_state();
        Self {
            index_uid: shard.index_uid.into(),
            source_id: shard.source_id,
            shard_id,
            shard_state,
            leader_id: shard.leader_id.into(),
        }
    }
}

/// The set of shards the router is aware of for the given index and source.
#[derive(Debug, Default)]
pub(super) struct RoutingTableEntry {
    /// The index UID of the shards.
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub local_shards: Vec<RoutingEntry>,
    pub local_round_robin_idx: AtomicUsize,
    pub remote_shards: Vec<RoutingEntry>,
    pub remote_round_robin_idx: AtomicUsize,
}

impl RoutingTableEntry {
    /// Creates a new entry and ensures that the shards are open, unique, and sorted by shard ID.
    fn new(
        self_node_id: &NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        mut shards: Vec<Shard>,
    ) -> Self {
        let num_shards = shards.len();

        shards.sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));
        shards.dedup_by(|left, right| left.shard_id == right.shard_id);

        let (local_shards, remote_shards): (Vec<_>, Vec<_>) = shards
            .into_iter()
            .filter(|shard| shard.is_open())
            .map(RoutingEntry::from)
            .partition(|shard| *self_node_id == shard.leader_id);

        if num_shards > local_shards.len() + remote_shards.len() {
            warn!("input shards should not contain closed shards or duplicates");
        }

        Self {
            index_uid,
            source_id,
            local_shards,
            // local_shard_ids_range_opt,
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards,
            // remote_shard_ids_range_opt,
            remote_round_robin_idx: AtomicUsize::default(),
        }
    }

    fn empty(index_uid: IndexUid, source_id: SourceId) -> Self {
        Self {
            index_uid,
            source_id,
            local_shards: Vec::new(),
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        }
    }

    /// Returns `true` if at least one shard in the table entry is open and has a leader available.
    /// As it goes through the list of shards in the entry, it populates `closed_shard_ids` and
    /// `unavailable_leaders` with the shard IDs of the closed shards and the node ID of the
    /// unavailable ingesters encountered along the way.
    pub fn has_open_shards(
        &self,
        ingester_pool: &IngesterPool,
        closed_shard_ids: &mut Vec<ShardId>,
        unavailable_leaders: &mut HashSet<NodeId>,
    ) -> bool {
        let shards = self.local_shards.iter().chain(self.remote_shards.iter());

        for shard in shards {
            match shard.shard_state {
                ShardState::Closed => {
                    closed_shard_ids.push(shard.shard_id.clone());
                    continue;
                }
                ShardState::Unavailable | ShardState::Unspecified => {
                    continue;
                }
                ShardState::Open => {
                    if unavailable_leaders.contains(&shard.leader_id) {
                        continue;
                    }
                    if ingester_pool.contains_key(&shard.leader_id) {
                        return true;
                    } else {
                        let leader_id: NodeId = shard.leader_id.clone();
                        unavailable_leaders.insert(leader_id);
                    }
                }
            }
        }
        false
    }

    /// Returns the next open and available shard in the table entry in a round-robin fashion.
    pub fn next_open_shard_round_robin(
        &self,
        ingester_pool: &IngesterPool,
    ) -> Option<&RoutingEntry> {
        for (shards, round_robin_idx) in [
            (&self.local_shards, &self.local_round_robin_idx),
            (&self.remote_shards, &self.remote_round_robin_idx),
        ] {
            if shards.is_empty() {
                continue;
            }
            for _attempt in 0..shards.len() {
                let shard_idx = round_robin_idx.fetch_add(1, Ordering::Relaxed);
                let shard = &shards[shard_idx % shards.len()];

                if shard.shard_state.is_open() && ingester_pool.contains_key(&shard.leader_id) {
                    return Some(shard);
                }
            }
        }
        None
    }

    /// Inserts the open shards the routing table is not aware of.
    fn insert_open_shards(
        &mut self,
        self_node_id: &NodeId,
        leader_id: &NodeId,
        index_uid: &IndexUid,
        shard_ids: &[ShardId],
    ) {
        match self.index_uid.cmp(index_uid) {
            // If we receive an update for a new incarnation of the index, then we clear the entry
            // and insert all the shards.
            std::cmp::Ordering::Less => {
                self.index_uid = index_uid.clone();
                self.clear_shards();
            }
            // If we receive an update for a previous incarnation of the index, then we ignore it.
            std::cmp::Ordering::Greater => {
                return;
            }
            std::cmp::Ordering::Equal => {}
        };
        let target_shards = if self_node_id == leader_id {
            &mut self.local_shards
        } else {
            &mut self.remote_shards
        };
        let mut num_inserted_shards = 0;
        let num_target_shards = target_shards.len();

        if num_target_shards == 0 {
            target_shards.reserve(num_target_shards);
            target_shards.extend(shard_ids.iter().map(|shard_id| RoutingEntry {
                index_uid: self.index_uid.clone(),
                source_id: self.source_id.clone(),
                shard_id: shard_id.clone(),
                shard_state: ShardState::Open,
                leader_id: leader_id.clone(),
            }));
            num_inserted_shards = target_shards.len();
        } else {
            let shard_ids_range = target_shards[0].shard_id.clone()
                ..=target_shards[num_target_shards - 1].shard_id.clone();

            for shard_id in shard_ids {
                // If we can't find the shard, then we insert it.
                if shard_ids_range.contains(shard_id) {
                    continue;
                }
                if target_shards[..num_target_shards]
                    .binary_search_by(|shard| shard.shard_id.cmp(shard_id))
                    .is_err()
                {
                    target_shards.push(RoutingEntry {
                        index_uid: self.index_uid.clone(),
                        source_id: self.source_id.clone(),
                        shard_id: shard_id.clone(),
                        shard_state: ShardState::Open,
                        leader_id: leader_id.clone(),
                    });
                    num_inserted_shards += 1;
                }
            }
        }
        if num_inserted_shards > 0 {
            target_shards.sort_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id));

            info!(
                index_id=%self.index_uid.index_id(),
                source_id=%self.source_id,
                "inserted {num_inserted_shards} shards into routing table"
            );
        }
    }

    /// Clears local and remote shards.
    fn clear_shards(&mut self) {
        self.local_shards.clear();
        self.local_round_robin_idx = AtomicUsize::default();
        self.remote_shards.clear();
        self.remote_round_robin_idx = AtomicUsize::default();
    }

    /// Closes the shards identified by their shard IDs.
    fn close_shards(&mut self, index_uid: &IndexUid, shard_ids: &[ShardId]) {
        // If the shard table was just recently updated with shards for a new index UID, then we can
        // safely discard this request.
        if self.index_uid != *index_uid {
            return;
        }
        for shards in [&mut self.local_shards, &mut self.remote_shards] {
            if shards.is_empty() {
                continue;
            }
            let num_shards = shards.len();
            let shard_ids_range =
                shards[0].shard_id.clone()..=shards[num_shards - 1].shard_id.clone();

            for shard_id in shard_ids {
                if !shard_ids_range.contains(shard_id) {
                    continue;
                }
                if let Ok(shard_idx) = shards.binary_search_by(|shard| shard.shard_id.cmp(shard_id))
                {
                    shards[shard_idx].shard_state = ShardState::Closed;
                }
            }
        }
    }

    /// Shards the shards identified by their shard IDs.
    fn delete_shards(&mut self, index_uid: &IndexUid, shard_ids: &[ShardId]) {
        // If the shard table was just recently updated with shards for a new index UID, then we can
        // safely discard this request.
        if self.index_uid != *index_uid {
            return;
        }
        for shards in [&mut self.local_shards, &mut self.remote_shards] {
            if shards.is_empty() {
                continue;
            }
            let num_shards = shards.len();
            let shard_ids_range =
                shards[0].shard_id.clone()..=shards[num_shards - 1].shard_id.clone();
            let mut deleted_any = false;

            for shard_id in shard_ids {
                if !shard_ids_range.contains(shard_id) {
                    continue;
                }
                if let Ok(shard_idx) = shards.binary_search_by(|shard| shard.shard_id.cmp(shard_id))
                {
                    // We use `Unspecified` as a tombstone.
                    shards[shard_idx].shard_state = ShardState::Unspecified;
                    deleted_any = true;
                }
            }
            if deleted_any {
                shards.retain(|shard| shard.shard_state != ShardState::Unspecified);
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.local_shards.len() + self.remote_shards.len()
    }

    #[cfg(test)]
    pub fn all_shards(&self) -> Vec<&RoutingEntry> {
        let mut shards = Vec::with_capacity(self.len());
        shards.extend(&self.local_shards);
        shards.extend(&self.remote_shards);
        shards
    }
}

/// Stores the list of shards the router is aware of for each index and source. The resolution from
/// index and source to shards is performed using index ID (not index UID) and source ID.
#[derive(Debug)]
pub(super) struct RoutingTable {
    pub self_node_id: NodeId,
    pub table: HashMap<(IndexId, SourceId), RoutingTableEntry>,
}

impl RoutingTable {
    pub fn find_entry(
        &self,
        index_id: impl Into<IndexId>,
        source_id: impl Into<SourceId>,
    ) -> Option<&RoutingTableEntry> {
        let key = (index_id.into(), source_id.into());
        self.table.get(&key)
    }

    /// Returns `true` if the router already knows about a shard for a given source that has
    /// an available `leader`.
    ///
    /// If this function returns false, it populates the set of unavailable leaders and closed
    /// shards. These will be joined to the GetOrCreate shard request emitted to the control
    /// plane.
    pub fn has_open_shards(
        &self,
        index_id: impl Into<IndexId>,
        source_id: impl Into<SourceId>,
        ingester_pool: &IngesterPool,
        closed_shards: &mut Vec<ShardIds>,
        unavailable_leaders: &mut HashSet<NodeId>,
    ) -> bool {
        let Some(entry) = self.find_entry(index_id, source_id) else {
            return false;
        };
        let mut closed_shard_ids: Vec<ShardId> = Vec::new();

        let result =
            entry.has_open_shards(ingester_pool, &mut closed_shard_ids, unavailable_leaders);

        if !closed_shard_ids.is_empty() {
            closed_shards.push(ShardIds {
                index_uid: entry.index_uid.clone().into(),
                source_id: entry.source_id.clone(),
                shard_ids: closed_shard_ids,
            });
        }
        result
    }

    /// Replaces the routing table entry for the source with the provided shards.
    pub fn replace_shards(
        &mut self,
        index_uid: impl Into<IndexUid>,
        source_id: impl Into<SourceId>,
        shards: Vec<Shard>,
    ) {
        let index_uid: IndexUid = index_uid.into();
        let index_id: IndexId = index_uid.index_id().into();
        let source_id: SourceId = source_id.into();
        let key = (index_id, source_id.clone());

        match self.table.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(RoutingTableEntry::new(
                    &self.self_node_id,
                    index_uid,
                    source_id,
                    shards,
                ));
            }
            Entry::Occupied(mut entry) => {
                assert!(
                    entry.get().index_uid <= index_uid,
                    "new index incarnation should be greater or equal"
                );

                entry.insert(RoutingTableEntry::new(
                    &self.self_node_id,
                    index_uid,
                    source_id,
                    shards,
                ));
            }
        };
    }

    /// Inserts the shards the routing table is not aware of.
    pub fn insert_open_shards(
        &mut self,
        leader_id: &NodeId,
        index_uid: impl Into<IndexUid>,
        source_id: impl Into<SourceId>,
        shard_ids: &[ShardId],
    ) {
        let index_uid: IndexUid = index_uid.into();
        let index_id: IndexId = index_uid.index_id().into();
        let source_id: SourceId = source_id.into();
        let key = (index_id, source_id.clone());

        self.table
            .entry(key.clone())
            .or_insert_with(|| RoutingTableEntry::empty(index_uid.clone(), source_id))
            .insert_open_shards(&self.self_node_id, leader_id, &index_uid, shard_ids);
    }

    /// Closes the targeted shards.
    pub fn close_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: impl Into<SourceId>,
        shard_ids: &[ShardId],
    ) {
        let key = (index_uid.index_id().into(), source_id.into());
        if let Some(entry) = self.table.get_mut(&key) {
            entry.close_shards(index_uid, shard_ids);
        }
    }

    /// Deletes the targeted shards.
    pub fn delete_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: impl Into<SourceId>,
        shard_ids: &[ShardId],
    ) {
        let key = (index_uid.index_id().into(), source_id.into());
        if let Some(entry) = self.table.get_mut(&key) {
            entry.delete_shards(index_uid, shard_ids);
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.table.len()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ingester::IngesterServiceClient;
    use quickwit_proto::ingest::ShardState;

    use super::*;

    #[test]
    fn test_routing_table_entry_new() {
        let self_node_id: NodeId = "test-node-0".into();
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let table_entry = RoutingTableEntry::new(
            &self_node_id,
            index_uid.clone(),
            source_id.clone(),
            Vec::new(),
        );
        assert_eq!(table_entry.len(), 0);

        let shards = vec![
            Shard {
                index_uid: "test-index:0".into(),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(3)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".into(),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".into(),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(2)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".into(),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1)),
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".into(),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(4)),
                shard_state: ShardState::Closed as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
        ];
        let table_entry = RoutingTableEntry::new(&self_node_id, index_uid, source_id, shards);
        assert_eq!(table_entry.local_shards.len(), 2);
        assert_eq!(table_entry.local_shards[0].shard_id, ShardId::from(1));
        assert_eq!(table_entry.local_shards[1].shard_id, ShardId::from(3));

        assert_eq!(table_entry.remote_shards.len(), 1);
        assert_eq!(table_entry.remote_shards[0].shard_id, ShardId::from(2));
    }

    #[test]
    fn test_routing_table_entry_has_open_shards() {
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());

        let mut closed_shard_ids = Vec::new();
        let ingester_pool = IngesterPool::default();
        let mut unavailable_leaders = HashSet::new();

        assert!(!table_entry.has_open_shards(
            &ingester_pool,
            &mut closed_shard_ids,
            &mut unavailable_leaders
        ));
        assert!(closed_shard_ids.is_empty());
        assert!(unavailable_leaders.is_empty());

        ingester_pool.insert(
            "test-ingester-0".into(),
            IngesterServiceClient::mock().into(),
        );
        ingester_pool.insert(
            "test-ingester-1".into(),
            IngesterServiceClient::mock().into(),
        );

        let table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
            ],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        };
        assert!(table_entry.has_open_shards(
            &ingester_pool,
            &mut closed_shard_ids,
            &mut unavailable_leaders
        ));
        assert_eq!(closed_shard_ids.len(), 1);
        assert_eq!(closed_shard_ids[0], ShardId::from(1));
        assert!(unavailable_leaders.is_empty());

        closed_shard_ids.clear();

        let table_entry = RoutingTableEntry {
            index_uid,
            source_id,
            local_shards: Vec::new(),
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-2".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(3),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
            ],
            remote_round_robin_idx: AtomicUsize::default(),
        };
        assert!(table_entry.has_open_shards(
            &ingester_pool,
            &mut closed_shard_ids,
            &mut unavailable_leaders
        ));
        assert_eq!(closed_shard_ids.len(), 1);
        assert_eq!(closed_shard_ids[0], ShardId::from(1));
        assert_eq!(unavailable_leaders.len(), 1);
        assert!(unavailable_leaders.contains("test-ingester-2"));
    }

    #[test]
    fn test_routing_table_entry_next_open_shard_round_robin() {
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());
        let ingester_pool = IngesterPool::default();

        let shard_opt = table_entry.next_open_shard_round_robin(&ingester_pool);
        assert!(shard_opt.is_none());

        ingester_pool.insert(
            "test-ingester-0".into(),
            IngesterServiceClient::mock().into(),
        );
        ingester_pool.insert(
            "test-ingester-1".into(),
            IngesterServiceClient::mock().into(),
        );

        let table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(3),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
            ],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        };
        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, ShardId::from(2));

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, ShardId::from(3));

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, ShardId::from(2));

        let table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![RoutingEntry {
                index_uid: "test-index:0".into(),
                source_id: "test-source".to_string(),
                shard_id: ShardId::from(1),
                shard_state: ShardState::Closed,
                leader_id: "test-ingester-0".into(),
            }],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(3),
                    shard_state: ShardState::Closed,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(4),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-2".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(5),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
            ],
            remote_round_robin_idx: AtomicUsize::default(),
        };
        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, ShardId::from(2));

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, ShardId::from(5));

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, ShardId::from(2));
    }

    #[test]
    fn test_routing_table_entry_insert_open_shards() {
        let index_uid_0: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid_0.clone(), source_id.clone());

        let local_node_id: NodeId = "test-ingester-0".into();
        let remote_node_id: NodeId = "test-ingester-1".into();
        table_entry.insert_open_shards(&local_node_id, &local_node_id, &index_uid_0, &[]);

        assert_eq!(table_entry.local_shards.len(), 0);
        assert_eq!(table_entry.remote_shards.len(), 0);

        table_entry.insert_open_shards(
            &local_node_id,
            &local_node_id,
            &index_uid_0,
            &[ShardId::from(2)],
        );

        assert_eq!(table_entry.local_shards.len(), 1);
        assert_eq!(table_entry.remote_shards.len(), 0);

        assert_eq!(table_entry.local_shards[0].index_uid, index_uid_0);
        assert_eq!(table_entry.local_shards[0].source_id, source_id);
        assert_eq!(table_entry.local_shards[0].shard_id, ShardId::from(2));
        assert_eq!(table_entry.local_shards[0].shard_state, ShardState::Open);
        assert_eq!(table_entry.local_shards[0].leader_id, local_node_id);

        table_entry.local_shards[0].shard_state = ShardState::Closed;
        table_entry.insert_open_shards(
            &local_node_id,
            &local_node_id,
            &index_uid_0,
            &[ShardId::from(1), ShardId::from(2)],
        );

        assert_eq!(table_entry.local_shards.len(), 2);
        assert_eq!(table_entry.remote_shards.len(), 0);

        assert_eq!(table_entry.local_shards[0].shard_id, ShardId::from(1));
        assert_eq!(table_entry.local_shards[0].shard_state, ShardState::Open);
        assert_eq!(table_entry.local_shards[1].shard_id, ShardId::from(2));
        assert_eq!(table_entry.local_shards[1].shard_state, ShardState::Closed);

        table_entry.local_shards.clear();
        table_entry.insert_open_shards(
            &local_node_id,
            &remote_node_id,
            &index_uid_0,
            &[ShardId::from(2)],
        );

        assert_eq!(table_entry.local_shards.len(), 0);
        assert_eq!(table_entry.remote_shards.len(), 1);

        assert_eq!(table_entry.remote_shards[0].index_uid, index_uid_0);
        assert_eq!(table_entry.remote_shards[0].source_id, source_id);
        assert_eq!(table_entry.remote_shards[0].shard_id, ShardId::from(2));
        assert_eq!(table_entry.remote_shards[0].shard_state, ShardState::Open);
        assert_eq!(table_entry.remote_shards[0].leader_id, remote_node_id);

        table_entry.remote_shards[0].shard_state = ShardState::Closed;
        table_entry.insert_open_shards(
            &local_node_id,
            &remote_node_id,
            &index_uid_0,
            &[ShardId::from(1), ShardId::from(2)],
        );

        assert_eq!(table_entry.local_shards.len(), 0);
        assert_eq!(table_entry.remote_shards.len(), 2);

        assert_eq!(table_entry.remote_shards[0].shard_id, ShardId::from(1));
        assert_eq!(table_entry.remote_shards[0].shard_state, ShardState::Open);
        assert_eq!(table_entry.remote_shards[1].shard_id, ShardId::from(2));
        assert_eq!(table_entry.remote_shards[1].shard_state, ShardState::Closed);

        // Update index incarnation.
        let index_uid_1: IndexUid = IndexUid::new_2("test-index", 1);
        table_entry.insert_open_shards(
            &local_node_id,
            &local_node_id,
            &index_uid_1,
            &[ShardId::from(1)],
        );

        assert_eq!(table_entry.index_uid, index_uid_1);
        assert_eq!(table_entry.local_shards.len(), 1);
        assert_eq!(table_entry.remote_shards.len(), 0);

        assert_eq!(table_entry.local_shards[0].index_uid, index_uid_1);
        assert_eq!(table_entry.local_shards[0].source_id, source_id);
        assert_eq!(table_entry.local_shards[0].shard_id, ShardId::from(1));
        assert_eq!(table_entry.local_shards[0].shard_state, ShardState::Open);
        assert_eq!(table_entry.local_shards[0].leader_id, local_node_id);

        // Ignore previous index incarnation.
        table_entry.insert_open_shards(
            &local_node_id,
            &local_node_id,
            &index_uid_0,
            &[ShardId::from(12), ShardId::from(42), ShardId::from(1337)],
        );
        assert_eq!(table_entry.index_uid, index_uid_1);
        assert_eq!(table_entry.local_shards.len(), 1);
        assert_eq!(table_entry.remote_shards.len(), 0);
    }

    #[test]
    fn test_routing_table_entry_close_shards() {
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();

        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());
        table_entry.close_shards(&index_uid, &[]);
        table_entry.close_shards(&index_uid, &[ShardId::from(1)]);
        assert!(table_entry.local_shards.is_empty());
        assert!(table_entry.remote_shards.is_empty());

        let mut table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(3),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
            ],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(5),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(6),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(7),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
            ],
            remote_round_robin_idx: AtomicUsize::default(),
        };
        table_entry.close_shards(
            &index_uid,
            &[
                ShardId::from(1),
                ShardId::from(3),
                ShardId::from(4),
                ShardId::from(6),
                ShardId::from(8),
            ],
        );
        assert!(table_entry.local_shards[0].shard_state.is_closed());
        assert!(table_entry.local_shards[1].shard_state.is_open());
        assert!(table_entry.local_shards[2].shard_state.is_closed());
        assert!(table_entry.remote_shards[0].shard_state.is_open());
        assert!(table_entry.remote_shards[1].shard_state.is_closed());
        assert!(table_entry.remote_shards[2].shard_state.is_open());
    }

    #[test]
    fn test_routing_table_entry_delete_shards() {
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();

        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());
        table_entry.delete_shards(&index_uid, &[]);
        table_entry.delete_shards(&index_uid, &[ShardId::from(1)]);
        assert!(table_entry.local_shards.is_empty());
        assert!(table_entry.remote_shards.is_empty());

        let mut table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(3),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
            ],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: vec![
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(5),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(6),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: "test-index:0".into(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(7),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
            ],
            remote_round_robin_idx: AtomicUsize::default(),
        };
        table_entry.delete_shards(
            &index_uid,
            &[
                ShardId::from(1),
                ShardId::from(3),
                ShardId::from(4),
                ShardId::from(6),
                ShardId::from(8),
            ],
        );
        assert_eq!(table_entry.local_shards.len(), 1);
        assert_eq!(table_entry.local_shards[0].shard_id, ShardId::from(2));
        assert_eq!(table_entry.remote_shards.len(), 2);
        assert_eq!(table_entry.remote_shards[0].shard_id, ShardId::from(5));
        assert_eq!(table_entry.remote_shards[1].shard_id, ShardId::from(7));
    }
}
