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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::types::{IndexId, IndexUid, NodeId, ShardId, SourceId};
use serde_json::{Value as JsonValue, json};
use tracing::{error, info};

use crate::IngesterPool;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
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
            index_uid: shard.index_uid().clone(),
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
    /// Index UID of the shards.
    pub index_uid: IndexUid,
    /// Source ID of the shards.
    pub source_id: SourceId,
    pub shards_by_node: HashMap<NodeId, HashSet<RoutingEntry>>,
}

impl RoutingTableEntry {
    /// Creates a new entry and ensures that the shards are open, unique, and sorted by shard ID.
    fn new(index_uid: IndexUid, source_id: SourceId, shards: Vec<Shard>) -> Self {
        let shards_by_node: HashMap<NodeId, HashSet<RoutingEntry>> = shards
            .into_iter()
            .filter(|shard| shard.is_open())
            .map(|shard| {
                (
                    NodeId::new(shard.leader_id.clone()),
                    RoutingEntry::from(shard),
                )
            })
            .fold(HashMap::new(), |mut map, (k, v)| {
                map.entry(k).or_insert_with(HashSet::new).insert(v);
                map
            });

        Self {
            index_uid,
            source_id,
            shards_by_node,
        }
    }

    fn empty(index_uid: IndexUid, source_id: SourceId) -> Self {
        Self {
            index_uid,
            source_id,
            ..Default::default()
        }
    }

    /// Returns `true` if at least one shard in the table entry is open and has a leader available.
    pub fn has_open_shards(
        &self,
        ingester_pool: &IngesterPool,
        unavailable_shards: &HashSet<ShardId>,
        unavailable_leaders: &HashSet<NodeId>,
    ) -> bool {
        self.shards_by_node.values().flatten().any(|shard| {
            shard.shard_state == ShardState::Open
                && !unavailable_shards.contains(&shard.shard_id)
                && ingester_pool.contains_key(&shard.leader_id)
                && !unavailable_leaders.contains(&shard.leader_id)
        })
    }

    /// Finds an available node to route to using "power of two choices":
    /// randomly picks between the top 2 nodes by open shard count to avoid hot partitions.
    /// TODO: figure out how to get indexUid not like this
    pub fn find_open_node(
        &self,
        ingester_pool: &IngesterPool,
        unavailable_nodes: &HashSet<NodeId>,
        unavailable_shards: &HashSet<ShardId>,
    ) -> Option<(&NodeId, IndexUid)> {
        let candidates: Vec<(&NodeId, IndexUid, i32)> = self
            .shards_by_node
            .iter()
            .filter(|&(node_id, _)| {
                ingester_pool.contains_key(node_id) && !unavailable_nodes.contains(node_id)
            })
            .filter_map(|(node_id, shards)| {
                // count of number of open shards for a node - we want to filter out nodes with no open shards
                let mut open_count = 0;
                for shard in shards {
                    if shard.shard_state.is_closed() || unavailable_shards.contains(&shard.shard_id)
                    {
                        continue;
                    }
                    open_count += 1;
                }
                (open_count > 0).then_some((node_id, self.index_uid.clone(), open_count))
            })
            .sorted_by(|(_, _, count_left), (_, _, count_right)| count_right.cmp(count_left))
            .collect();

        // Power of two choices: randomly pick between top 2 candidates
        match candidates.len() {
            0 => None,
            1 => Some((candidates[0].0, candidates[0].1.clone())),
            _ => {
                let idx = if rand::random::<bool>() { 0 } else { 1 };
                Some((candidates[idx].0, candidates[idx].1.clone()))
            }
        }
    }

    /// Inserts the open shards the routing table is not aware of.
    fn insert_open_shards(
        &mut self,
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
        let node_shards = self.shards_by_node.entry(leader_id.clone()).or_default();

        let mut num_inserted_shards = 0;
        for shard_id in shard_ids {
            let routing_entry = RoutingEntry {
                index_uid: self.index_uid.clone(),
                source_id: self.source_id.clone(),
                shard_id: shard_id.clone(),
                shard_state: ShardState::Open,
                leader_id: leader_id.clone(),
            };
            if node_shards.contains(&routing_entry) {
                continue;
            }
            num_inserted_shards += 1;
        }

        info!(
            index_uid=%self.index_uid,
            source_id=%self.source_id,
            "inserted {num_inserted_shards} shards into routing table"
        );
    }

    fn clear_shards(&mut self) {
        self.shards_by_node.clear();
    }

    /// Closes the shards identified by their shard IDs.
    fn close_shards(&mut self, index_uid: &IndexUid, shard_ids: &[ShardId]) {
        // If the shard table was just recently updated with shards for a new index UID, then we can
        // safely discard this request.
        if self.index_uid != *index_uid {
            return;
        }
        let shard_ids: HashSet<&ShardId> = shard_ids.iter().collect();
        for (_, routing_entries) in self.shards_by_node.iter_mut() {
            let shards_to_close: Vec<RoutingEntry> = routing_entries
                .iter()
                .filter(|&routing_entry| shard_ids.contains(&routing_entry.shard_id))
                .cloned()
                .collect();

            for shard in shards_to_close {
                let mut entry = routing_entries.take(&shard).unwrap();
                entry.shard_state = ShardState::Closed;
                routing_entries.insert(entry);
            }
        }
    }

    /// Deletes the shards identified by their shard IDs.
    fn delete_shards(&mut self, index_uid: &IndexUid, shard_ids: &[ShardId]) {
        // If the shard table was just recently updated with shards for a new index UID, then we can
        // safely discard this request.
        if self.index_uid != *index_uid {
            return;
        }
        let shards_to_delete: HashSet<&ShardId> = shard_ids.iter().collect();
        for (_, routing_entries) in self.shards_by_node.iter_mut() {
            routing_entries
                .retain(|routing_entry| !shards_to_delete.contains(&routing_entry.shard_id));
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.shards_by_node.values().flatten().count()
    }

    #[cfg(test)]
    pub fn all_shards(&self) -> Vec<&RoutingEntry> {
        self.shards_by_node.values().flatten().collect()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum NextOpenShardError {
    NoShardsAvailable,
    RateLimited,
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
        unavailable_shards: &HashSet<ShardId>,
        unavailable_leaders: &HashSet<NodeId>,
    ) -> bool {
        let Some(entry) = self.find_entry(index_id, source_id) else {
            return false;
        };

        entry.has_open_shards(ingester_pool, unavailable_shards, unavailable_leaders)
    }

    /// Replaces the routing table entry for the source with the provided shards.
    pub fn replace_shards(
        &mut self,
        index_uid: IndexUid,
        source_id: impl Into<SourceId>,
        shards: Vec<Shard>,
    ) {
        let index_id: IndexId = index_uid.index_id.to_string();
        let source_id: SourceId = source_id.into();
        let key = (index_id, source_id.clone());

        match self.table.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(RoutingTableEntry::new(index_uid, source_id, shards));
            }
            Entry::Occupied(mut entry) => {
                assert!(
                    entry.get().index_uid <= index_uid,
                    "new index incarnation should be greater or equal"
                );

                entry.insert(RoutingTableEntry::new(index_uid, source_id, shards));
            }
        };
    }

    /// Inserts the shards the routing table is not aware of.
    pub fn insert_open_shards(
        &mut self,
        leader_id: &NodeId,
        index_uid: IndexUid,
        source_id: impl Into<SourceId>,
        shard_ids: &[ShardId],
    ) {
        let index_id: IndexId = index_uid.index_id.to_string();
        let source_id: SourceId = source_id.into();
        let key = (index_id, source_id.clone());

        self.table
            .entry(key.clone())
            .or_insert_with(|| RoutingTableEntry::empty(index_uid.clone(), source_id))
            .insert_open_shards(leader_id, &index_uid, shard_ids);
    }

    /// Closes the targeted shards.
    pub fn close_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: impl Into<SourceId>,
        shard_ids: &[ShardId],
    ) {
        let key = (index_uid.index_id.clone(), source_id.into());
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
        let key = (index_uid.index_id.clone(), source_id.into());
        if let Some(entry) = self.table.get_mut(&key) {
            entry.delete_shards(index_uid, shard_ids);
        }
    }

    pub fn debug_info(&self) -> HashMap<IndexId, Vec<JsonValue>> {
        let mut per_index_shards_json: HashMap<IndexId, Vec<JsonValue>> = HashMap::new();

        for ((index_id, source_id), entry) in &self.table {
            for (node_id, shards) in entry.shards_by_node.iter() {
                let shards_json = shards.iter().map(|shard| {
                    json!({
                        "index_uid": shard.index_uid,
                        "source_id": source_id,
                        "shard_id": shard.shard_id,
                        "shard_state": shard.shard_state.as_json_str_name(),
                        "leader_id": node_id,
                    })
                });
                per_index_shards_json
                    .entry(index_id.clone())
                    .or_default()
                    .extend(shards_json);
            }
        }
        per_index_shards_json
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.table.len()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::ingest::ingester::IngesterServiceClient;

    use super::*;

    #[test]
    fn test_routing_table_entry_new() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();

        let table_entry = RoutingTableEntry::new(index_uid.clone(), source_id.clone(), vec![]);
        assert_eq!(table_entry.len(), 0);

        let make_shard = |id: u64, leader: &str, state: ShardState| Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(id)),
            shard_state: state as i32,
            leader_id: leader.to_string(),
            ..Default::default()
        };

        let shards = vec![
            make_shard(0, "test-node-0", ShardState::Open),
            make_shard(1, "test-node-0", ShardState::Open),
            make_shard(2, "test-node-1", ShardState::Open),
            make_shard(3, "test-node-1", ShardState::Open),
            make_shard(4, "test-node-2", ShardState::Open),
            make_shard(5, "test-node-2", ShardState::Closed), // Should be filtered out
        ];

        let table_entry = RoutingTableEntry::new(index_uid, source_id, shards);

        assert_eq!(table_entry.shards_by_node.len(), 3);

        let node_0_shards = table_entry.shards_by_node.get("test-node-0").unwrap();
        assert_eq!(node_0_shards.len(), 2);
        assert_eq!(node_0_shards[0].shard_id, ShardId::from(0));
        assert_eq!(node_0_shards[1].shard_id, ShardId::from(1));

        let node_1_shards = table_entry.shards_by_node.get("test-node-1").unwrap();
        assert_eq!(node_1_shards.len(), 2);
        assert_eq!(node_1_shards[0].shard_id, ShardId::from(2));
        assert_eq!(node_1_shards[1].shard_id, ShardId::from(3));

        let node_2_shards = table_entry.shards_by_node.get("test-node-2").unwrap();
        assert_eq!(node_2_shards.len(), 1); // Closed shard filtered out
        assert_eq!(node_2_shards[0].shard_id, ShardId::from(4));
    }

    #[test]
    fn test_routing_table_entry_has_open_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
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

        ingester_pool.insert("test-ingester-0".into(), IngesterServiceClient::mocked());
        ingester_pool.insert("test-ingester-1".into(), IngesterServiceClient::mocked());

        let mut shards_by_node = HashMap::new();
        shards_by_node.insert(
            "test-ingester-0".into(),
            vec![
                RoutingEntry {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    leader_id: "test-ingester-0".into(),
                },
                RoutingEntry {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(2),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-0".into(),
                },
            ],
        );

        let table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shards_by_node,
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

        let mut shards_by_node = HashMap::new();
        shards_by_node.insert(
            "test-ingester-1".into(),
            vec![
                RoutingEntry {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(1),
                    shard_state: ShardState::Closed,
                    leader_id: "test-ingester-1".into(),
                },
                RoutingEntry {
                    index_uid: index_uid.clone(),
                    source_id: "test-source".to_string(),
                    shard_id: ShardId::from(3),
                    shard_state: ShardState::Open,
                    leader_id: "test-ingester-1".into(),
                },
            ],
        );
        shards_by_node.insert(
            "test-ingester-2".into(),
            vec![RoutingEntry {
                index_uid: index_uid.clone(),
                source_id: "test-source".to_string(),
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                leader_id: "test-ingester-2".into(),
            }],
        );

        let table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id,
            shards_by_node,
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

    fn make_routing_entry(
        index_uid: &IndexUid,
        source_id: &SourceId,
        shard_id: u64,
        shard_state: ShardState,
        leader_id: &str,
    ) -> RoutingEntry {
        RoutingEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shard_id: ShardId::from(shard_id),
            shard_state,
            leader_id: leader_id.into(),
        }
    }

    #[test]
    fn test_find_open_node_no_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let table_entry = RoutingTableEntry::empty(index_uid, source_id);
        let ingester_pool = IngesterPool::default();
        let rate_limited_shards = HashSet::new();

        let result = table_entry.find_open_node(&ingester_pool, &rate_limited_shards);
        assert!(matches!(result, Err(NextOpenShardError::NoShardsAvailable)));
    }

    #[test]
    fn test_find_open_node_only_closed_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert("node-0".into(), IngesterServiceClient::mocked());

        let mut shards_by_node = HashMap::new();
        shards_by_node.insert(
            "node-0".into(),
            vec![make_routing_entry(
                &index_uid,
                &source_id,
                1,
                ShardState::Closed,
                "node-0",
            )],
        );

        let table_entry = RoutingTableEntry {
            index_uid,
            source_id,
            shards_by_node,
        };

        let rate_limited_shards = HashSet::new();
        let result = table_entry.find_open_node(&ingester_pool, &rate_limited_shards);
        assert!(matches!(result, Err(NextOpenShardError::NoShardsAvailable)));
    }

    #[test]
    fn test_find_open_node_only_rate_limited_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert("node-0".into(), IngesterServiceClient::mocked());

        let mut shards_by_node = HashMap::new();
        shards_by_node.insert(
            "node-0".into(),
            vec![make_routing_entry(
                &index_uid,
                &source_id,
                1,
                ShardState::Open,
                "node-0",
            )],
        );

        let table_entry = RoutingTableEntry {
            index_uid,
            source_id,
            shards_by_node,
        };

        let mut rate_limited_shards = HashSet::new();
        rate_limited_shards.insert(ShardId::from(1));

        let result = table_entry.find_open_node(&ingester_pool, &rate_limited_shards);
        assert!(matches!(result, Err(NextOpenShardError::RateLimited)));
    }

    #[test]
    fn test_find_open_node_filters_nodes_not_in_pool() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert("node-1".into(), IngesterServiceClient::mocked());

        let mut shards_by_node = HashMap::new();
        // node-0 is NOT in pool - should be filtered out
        shards_by_node.insert(
            "node-0".into(),
            vec![make_routing_entry(
                &index_uid,
                &source_id,
                1,
                ShardState::Open,
                "node-0",
            )],
        );
        // node-1 is in pool and has open shard - should be selected
        shards_by_node.insert(
            "node-1".into(),
            vec![make_routing_entry(
                &index_uid,
                &source_id,
                2,
                ShardState::Open,
                "node-1",
            )],
        );

        let table_entry = RoutingTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            shards_by_node,
        };

        let rate_limited_shards = HashSet::new();
        let result = table_entry.find_open_node(&ingester_pool, &rate_limited_shards);
        let (node_id, result_index_uid, result_source_id) = result.unwrap();
        assert_eq!(node_id, &NodeId::from("node-1"));
        assert_eq!(result_index_uid, &index_uid);
        assert_eq!(result_source_id, &source_id);
    }

    #[test]
    fn test_find_open_node_power_of_two_choices() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let ingester_pool = IngesterPool::default();
        ingester_pool.insert("node-0".into(), IngesterServiceClient::mocked());
        ingester_pool.insert("node-1".into(), IngesterServiceClient::mocked());
        ingester_pool.insert("node-2".into(), IngesterServiceClient::mocked());
        ingester_pool.insert("node-3".into(), IngesterServiceClient::mocked());

        let make_entry = |shard_id: u64, leader: &str| {
            make_routing_entry(&index_uid, &source_id, shard_id, ShardState::Open, leader)
        };

        let mut shards_by_node = HashMap::new();
        shards_by_node.insert(
            "node-0".into(),
            vec![make_entry(1, "node-0"), make_entry(2, "node-0")],
        );
        shards_by_node.insert(
            "node-1".into(),
            vec![make_entry(3, "node-1"), make_entry(4, "node-1")],
        );
        shards_by_node.insert("node-2".into(), vec![make_entry(5, "node-2")]);
        shards_by_node.insert("node-3".into(), vec![make_entry(6, "node-3")]);

        let table_entry = RoutingTableEntry {
            index_uid,
            source_id,
            shards_by_node,
        };

        let rate_limited_shards = HashSet::new();
        let result = table_entry.find_open_node(&ingester_pool, &rate_limited_shards);
        let (node_id, _, _) = result.unwrap();

        // Power of two should pick between the top 2 candidates (node-0 or node-1)
        assert!(
            node_id == &NodeId::from("node-0") || node_id == &NodeId::from("node-1"),
            "Expected node-0 or node-1, got {:?}",
            node_id
        );
    }

    #[test]
    fn test_insert_open_shards_empty_list() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id);

        let node_id: NodeId = "node-0".into();
        table_entry.insert_open_shards(&node_id, &index_uid, &[]);

        // Node entry is created but with no shards
        assert!(table_entry.shards_by_node.get(&node_id).unwrap().is_empty());
    }

    #[test]
    fn test_insert_open_shards_adds_new_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());

        let node_id: NodeId = "node-0".into();
        table_entry.insert_open_shards(&node_id, &index_uid, &[ShardId::from(1), ShardId::from(2)]);

        let shards = table_entry.shards_by_node.get(&node_id).unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].shard_id, ShardId::from(1));
        assert_eq!(shards[0].shard_state, ShardState::Open);
        assert_eq!(shards[0].leader_id, node_id);
        assert_eq!(shards[1].shard_id, ShardId::from(2));
    }

    #[test]
    fn test_insert_open_shards_preserves_existing() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());

        let node_id: NodeId = "node-0".into();
        table_entry.insert_open_shards(&node_id, &index_uid, &[ShardId::from(2)]);

        // Mark existing shard as closed
        table_entry.shards_by_node.get_mut(&node_id).unwrap()[0].shard_state = ShardState::Closed;

        // Insert overlapping shards - shard 2 should keep its Closed state
        table_entry.insert_open_shards(&node_id, &index_uid, &[ShardId::from(1), ShardId::from(2)]);

        let shards = table_entry.shards_by_node.get(&node_id).unwrap();
        assert_eq!(shards.len(), 2);

        let shard_1 = shards
            .iter()
            .find(|s| s.shard_id == ShardId::from(1))
            .unwrap();
        let shard_2 = shards
            .iter()
            .find(|s| s.shard_id == ShardId::from(2))
            .unwrap();

        assert_eq!(shard_1.shard_state, ShardState::Open);
        assert_eq!(shard_2.shard_state, ShardState::Closed);
    }

    #[test]
    fn test_insert_open_shards_multiple_nodes() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id);

        let node_0: NodeId = "node-0".into();
        let node_1: NodeId = "node-1".into();

        table_entry.insert_open_shards(&node_0, &index_uid, &[ShardId::from(1)]);
        table_entry.insert_open_shards(&node_1, &index_uid, &[ShardId::from(2)]);

        assert_eq!(table_entry.shards_by_node.len(), 2);
        assert_eq!(table_entry.shards_by_node.get(&node_0).unwrap().len(), 1);
        assert_eq!(table_entry.shards_by_node.get(&node_1).unwrap().len(), 1);
        assert_eq!(
            table_entry.shards_by_node.get(&node_0).unwrap()[0].leader_id,
            node_0
        );
        assert_eq!(
            table_entry.shards_by_node.get(&node_1).unwrap()[0].leader_id,
            node_1
        );
    }

    #[test]
    fn test_insert_open_shards_new_index_incarnation_clears_existing() {
        let index_uid_0 = IndexUid::for_test("test-index", 0);
        let index_uid_1 = IndexUid::for_test("test-index", 1);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid_0.clone(), source_id);

        let node_id: NodeId = "node-0".into();
        table_entry.insert_open_shards(
            &node_id,
            &index_uid_0,
            &[ShardId::from(1), ShardId::from(2)],
        );

        // New incarnation clears all existing shards
        table_entry.insert_open_shards(&node_id, &index_uid_1, &[ShardId::from(10)]);

        assert_eq!(table_entry.index_uid, index_uid_1);
        let shards = table_entry.shards_by_node.get(&node_id).unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, ShardId::from(10));
        assert_eq!(shards[0].index_uid, index_uid_1);
    }

    #[test]
    fn test_insert_open_shards_old_incarnation_ignored() {
        let index_uid_0 = IndexUid::for_test("test-index", 0);
        let index_uid_1 = IndexUid::for_test("test-index", 1);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid_1.clone(), source_id);

        let node_id: NodeId = "node-0".into();
        table_entry.insert_open_shards(&node_id, &index_uid_1, &[ShardId::from(1)]);

        // Old incarnation is ignored
        table_entry.insert_open_shards(
            &node_id,
            &index_uid_0,
            &[ShardId::from(99), ShardId::from(100)],
        );

        assert_eq!(table_entry.index_uid, index_uid_1);
        let shards = table_entry.shards_by_node.get(&node_id).unwrap();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id, ShardId::from(1));
    }

    #[test]
    fn test_close_shards_on_empty_table() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id);

        table_entry.close_shards(&index_uid, &[]);
        table_entry.close_shards(&index_uid, &[ShardId::from(1)]);

        assert!(table_entry.shards_by_node.is_empty());
    }

    #[test]
    fn test_close_shards_marks_shards_as_closed() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());

        let node_0: NodeId = "node-0".into();
        let node_1: NodeId = "node-1".into();

        table_entry.insert_open_shards(
            &node_0,
            &index_uid,
            &[ShardId::from(1), ShardId::from(2), ShardId::from(3)],
        );
        table_entry.insert_open_shards(
            &node_1,
            &index_uid,
            &[ShardId::from(4), ShardId::from(5), ShardId::from(6)],
        );

        table_entry.close_shards(
            &index_uid,
            &[
                ShardId::from(1),
                ShardId::from(2),
                ShardId::from(3),
                ShardId::from(4),
                ShardId::from(8), // doesnt exist, should no-op
            ],
        );

        let node_0_shards = table_entry.shards_by_node.get(&node_0).unwrap();
        assert!(node_0_shards[0].shard_state.is_closed()); // shard 1
        assert!(node_0_shards[1].shard_state.is_closed()); // shard 2
        assert!(node_0_shards[2].shard_state.is_closed()); // shard 3

        let node_1_shards = table_entry.shards_by_node.get(&node_1).unwrap();
        assert!(node_1_shards[0].shard_state.is_closed()); // shard 4
        assert!(node_1_shards[1].shard_state.is_open()); // shard 5
        assert!(node_1_shards[2].shard_state.is_open()); // shard 6
    }

    #[test]
    fn test_close_shards_wrong_index_uid_ignored() {
        let index_uid_0 = IndexUid::for_test("test-index", 0);
        let index_uid_1 = IndexUid::for_test("test-index", 1);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid_1.clone(), source_id);

        let node_id: NodeId = "node-0".into();
        table_entry.insert_open_shards(&node_id, &index_uid_1, &[ShardId::from(1)]);

        // different index incarnation, ignored
        table_entry.close_shards(&index_uid_0, &[ShardId::from(1)]);

        let shards = table_entry.shards_by_node.get(&node_id).unwrap();
        assert!(shards[0].shard_state.is_open());
    }

    #[test]
    fn test_delete_shards_on_empty_table() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id);

        table_entry.delete_shards(&index_uid, &[]);
        table_entry.delete_shards(&index_uid, &[ShardId::from(1)]);

        assert!(table_entry.shards_by_node.is_empty());
    }

    #[test]
    fn test_delete_shards_removes_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid.clone(), source_id.clone());

        let node_0: NodeId = "node-0".into();
        let node_1: NodeId = "node-1".into();

        table_entry.insert_open_shards(
            &node_0,
            &index_uid,
            &[ShardId::from(1), ShardId::from(2), ShardId::from(3)],
        );
        table_entry.insert_open_shards(
            &node_1,
            &index_uid,
            &[ShardId::from(4), ShardId::from(5), ShardId::from(6)],
        );

        // Delete shards 1, 3 on node-0 and shard 6 on node-1; shard 4 and 8 don't exist
        table_entry.delete_shards(
            &index_uid,
            &[
                ShardId::from(1),
                ShardId::from(2),
                ShardId::from(3),
                ShardId::from(4),
                ShardId::from(8), // doesnt exist, should no-op
            ],
        );

        let node_0_shards = table_entry.shards_by_node.get(&node_0).unwrap();
        assert!(node_0_shards.is_empty());

        let node_1_shards = table_entry.shards_by_node.get(&node_1).unwrap();
        // Shards 5, 6 remain (4 was deleted)
        let open_shards: Vec<_> = node_1_shards
            .iter()
            .filter(|s| s.shard_state.is_open())
            .collect();
        assert_eq!(open_shards.len(), 2);
        assert_eq!(open_shards[0].shard_id, ShardId::from(5));
        assert_eq!(open_shards[1].shard_id, ShardId::from(6));
    }

    #[test]
    fn test_delete_shards_wrong_index_uid_ignored() {
        let index_uid_0 = IndexUid::for_test("test-index", 0);
        let index_uid_1 = IndexUid::for_test("test-index", 1);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = RoutingTableEntry::empty(index_uid_1.clone(), source_id);

        let node_id: NodeId = "node-0".into();
        table_entry.insert_open_shards(&node_id, &index_uid_1, &[ShardId::from(1)]);

        // different index incarnation, ignored
        table_entry.delete_shards(&index_uid_0, &[ShardId::from(1)]);

        let shards = table_entry.shards_by_node.get(&node_id).unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].shard_state.is_open());
    }
}
