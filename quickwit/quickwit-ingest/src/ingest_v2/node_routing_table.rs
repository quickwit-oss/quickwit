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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use quickwit_proto::ingest::Shard;
use quickwit_proto::types::{IndexId, IndexUid, NodeId, SourceId};
use rand::rng;
use rand::seq::IndexedRandom;

use crate::IngesterPool;

/// A single ingester node's routing-relevant data for a specific (index, source) pair.
/// Each entry is self-describing: it carries its own node_id, index_uid, and source_id
/// so it can always be attributed back to a specific source on a specific node.
#[derive(Debug, Clone)]
pub(super) struct IngesterNode {
    pub node_id: NodeId,
    pub index_uid: IndexUid,
    #[allow(unused)]
    pub source_id: SourceId,
    /// Score from 0-10. Higher means more available capacity.
    pub capacity_score: usize,
    /// Number of open shards on this node for this (index, source) pair. Tiebreaker for power of
    /// two choices comparison - we favor a node with more open shards.
    pub open_shard_count: usize,
}

#[derive(Debug, Default)]
pub(super) struct RoutingEntry {
    pub nodes: HashMap<NodeId, IngesterNode>,
}

/// Given a slice of candidates, picks the better of two random choices.
/// Higher capacity_score wins; tiebreak on more open_shard_count (more landing spots).
fn power_of_two_choices<'a>(candidates: &[&'a IngesterNode]) -> &'a IngesterNode {
    debug_assert!(candidates.len() >= 2);
    let mut iter = candidates.choose_multiple(&mut rng(), 2);
    let (&a, &b) = (iter.next().unwrap(), iter.next().unwrap());

    if (a.capacity_score, a.open_shard_count) >= (b.capacity_score, b.open_shard_count) {
        a
    } else {
        b
    }
}

impl RoutingEntry {
    /// Pick an ingester node to persist the request to. Uses power of two choices based on reported
    /// ingester capacity, if more than one eligible node exists.
    pub fn pick_node(
        &self,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &HashSet<NodeId>,
    ) -> Option<&IngesterNode> {
        let eligible: Vec<&IngesterNode> = self
            .nodes
            .values()
            .filter(|node| {
                node.capacity_score > 0
                    && node.open_shard_count > 0
                    && ingester_pool.contains_key(&node.node_id)
                    && !unavailable_leaders.contains(&node.node_id)
            })
            .collect();

        match eligible.len() {
            0 => None,
            1 => Some(eligible[0]),
            _ => Some(power_of_two_choices(&eligible)),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct NodeBasedRoutingTable {
    table: HashMap<(IndexId, SourceId), RoutingEntry>,
}

impl NodeBasedRoutingTable {
    pub fn find_entry(&self, index_id: &str, source_id: &str) -> Option<&RoutingEntry> {
        let key = (index_id.to_string(), source_id.to_string());
        self.table.get(&key)
    }

    pub fn debug_info(&self) -> HashMap<IndexId, Vec<serde_json::Value>> {
        let mut per_index: HashMap<IndexId, Vec<serde_json::Value>> = HashMap::new();
        for ((index_id, source_id), entry) in &self.table {
            for (node_id, node) in &entry.nodes {
                per_index
                    .entry(index_id.clone())
                    .or_default()
                    .push(serde_json::json!({
                        "source_id": source_id,
                        "node_id": node_id,
                        "capacity_score": node.capacity_score,
                        "open_shard_count": node.open_shard_count,
                    }));
            }
        }
        per_index
    }

    pub fn has_open_nodes(
        &self,
        index_id: &str,
        source_id: &str,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &HashSet<NodeId>,
    ) -> bool {
        let key = (index_id.to_string(), source_id.to_string());
        let Some(entry) = self.table.get(&key) else {
            return false;
        };
        entry.nodes.values().any(|node| {
            node.capacity_score > 0
                && node.open_shard_count > 0
                && ingester_pool.contains_key(&node.node_id)
                && !unavailable_leaders.contains(&node.node_id)
        })
    }

    /// Applies a capacity update from the IngesterCapacityScoreUpdate broadcast. This is the
    /// primary way the table learns about node availability and capacity.
    pub fn apply_capacity_update(
        &mut self,
        node_id: NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        capacity_score: usize,
        open_shard_count: usize,
    ) {
        let key = (index_uid.index_id.to_string(), source_id.clone());

        let entry = self.table.entry(key).or_default();
        let ingester_node = IngesterNode {
            node_id: node_id.clone(),
            index_uid,
            source_id,
            capacity_score,
            open_shard_count,
        };
        entry.nodes.insert(node_id, ingester_node);
    }

    /// Merges routing updates from a GetOrCreateOpenShards control plane response into the
    /// table. For existing nodes, updates their open shard count, including counts of 0, from the
    /// CP response while preserving capacity scores if they already exist.
    /// New nodes get a default capacity_score of 5.
    pub fn merge_from_shards(
        &mut self,
        index_uid: IndexUid,
        source_id: SourceId,
        shards: Vec<Shard>,
    ) {
        let per_leader_count: HashMap<NodeId, usize> = shards
            .iter()
            .map(|shard| {
                (
                    NodeId::from(shard.leader_id.clone()),
                    shard.is_open() as usize,
                )
            })
            .into_grouping_map()
            .sum();

        let key = (index_uid.index_id.to_string(), source_id.clone());
        let entry = self.table.entry(key).or_default();

        for (node_id, open_shard_count) in per_leader_count {
            entry
                .nodes
                .entry(node_id.clone())
                .and_modify(|node| node.open_shard_count = open_shard_count)
                .or_insert_with(|| IngesterNode {
                    node_id,
                    index_uid: index_uid.clone(),
                    source_id: source_id.clone(),
                    capacity_score: 5,
                    open_shard_count,
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::ingest::ingester::IngesterServiceClient;
    use quickwit_proto::types::ShardId;

    use super::*;

    const TEST_SOURCE: &str = "test-source";

    fn test_index_uid() -> IndexUid {
        IndexUid::for_test("test-index", 0)
    }

    #[test]
    fn test_apply_capacity_update() {
        let mut table = NodeBasedRoutingTable::default();
        let key = ("test-index".to_string(), TEST_SOURCE.to_string());

        // Insert first node.
        table.apply_capacity_update("node-1".into(), test_index_uid(), TEST_SOURCE.into(), 8, 3);
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 1);
        assert_eq!(entry.nodes.get("node-1").unwrap().capacity_score, 8);

        // Update existing node.
        table.apply_capacity_update("node-1".into(), test_index_uid(), TEST_SOURCE.into(), 4, 5);
        let node = table.table.get(&key).unwrap().nodes.get("node-1").unwrap();
        assert_eq!(node.capacity_score, 4);
        assert_eq!(node.open_shard_count, 5);

        // Add second node.
        table.apply_capacity_update("node-2".into(), test_index_uid(), TEST_SOURCE.into(), 6, 2);
        assert_eq!(table.table.get(&key).unwrap().nodes.len(), 2);

        // Zero shards: node stays in table but becomes ineligible for routing.
        table.apply_capacity_update("node-1".into(), test_index_uid(), TEST_SOURCE.into(), 0, 0);
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 2);
        assert_eq!(entry.nodes.get("node-1").unwrap().open_shard_count, 0);
        assert_eq!(entry.nodes.get("node-1").unwrap().capacity_score, 0);
    }

    #[test]
    fn test_has_open_nodes() {
        let mut table = NodeBasedRoutingTable::default();
        let pool = IngesterPool::default();

        // Empty table.
        assert!(!table.has_open_nodes("test-index", TEST_SOURCE, &pool, &HashSet::new()));

        // Node exists but is not in pool.
        table.apply_capacity_update("node-1".into(), test_index_uid(), TEST_SOURCE.into(), 8, 3);
        assert!(!table.has_open_nodes("test-index", TEST_SOURCE, &pool, &HashSet::new()));

        // Node is in pool → true.
        pool.insert("node-1".into(), IngesterServiceClient::mocked());
        assert!(table.has_open_nodes("test-index", TEST_SOURCE, &pool, &HashSet::new()));

        // Node is unavailable → false.
        let unavailable: HashSet<NodeId> = HashSet::from(["node-1".into()]);
        assert!(!table.has_open_nodes("test-index", TEST_SOURCE, &pool, &unavailable));

        // Second node available → true despite first being unavailable.
        table.apply_capacity_update("node-2".into(), test_index_uid(), TEST_SOURCE.into(), 6, 2);
        pool.insert("node-2".into(), IngesterServiceClient::mocked());
        assert!(table.has_open_nodes("test-index", TEST_SOURCE, &pool, &unavailable));

        // Node with capacity_score=0 is not eligible.
        table.apply_capacity_update("node-2".into(), test_index_uid(), TEST_SOURCE.into(), 0, 2);
        assert!(!table.has_open_nodes("test-index", TEST_SOURCE, &pool, &unavailable));
    }

    #[test]
    fn test_pick_node() {
        let mut table = NodeBasedRoutingTable::default();
        let pool = IngesterPool::default();
        let key = ("test-index".to_string(), TEST_SOURCE.to_string());

        // Node exists but not in pool → None.
        table.apply_capacity_update("node-1".into(), test_index_uid(), TEST_SOURCE.into(), 8, 3);
        assert!(
            table
                .table
                .get(&key)
                .unwrap()
                .pick_node(&pool, &HashSet::new())
                .is_none()
        );

        // Single node in pool → picks it.
        pool.insert("node-1".into(), IngesterServiceClient::mocked());
        let picked = table
            .table
            .get(&key)
            .unwrap()
            .pick_node(&pool, &HashSet::new())
            .unwrap();
        assert_eq!(picked.node_id, NodeId::from("node-1"));

        // Multiple nodes → something is returned.
        table.apply_capacity_update("node-2".into(), test_index_uid(), TEST_SOURCE.into(), 2, 1);
        pool.insert("node-2".into(), IngesterServiceClient::mocked());
        assert!(
            table
                .table
                .get(&key)
                .unwrap()
                .pick_node(&pool, &HashSet::new())
                .is_some()
        );

        // Node with capacity_score=0 is skipped.
        table.apply_capacity_update("node-1".into(), test_index_uid(), TEST_SOURCE.into(), 0, 3);
        table.apply_capacity_update("node-2".into(), test_index_uid(), TEST_SOURCE.into(), 0, 1);
        assert!(
            table
                .table
                .get(&key)
                .unwrap()
                .pick_node(&pool, &HashSet::new())
                .is_none()
        );
    }

    #[test]
    fn test_power_of_two_choices() {
        // 3 candidates: best appears in the random pair 2/3 of the time and always
        // wins when it does, so it should win ~67% of 1000 runs. Asserting > 550
        // is ~7.5 standard deviations from the mean — effectively impossible to flake.
        let high = IngesterNode {
            node_id: "high".into(),
            index_uid: IndexUid::for_test("idx", 0),
            source_id: "src".into(),
            capacity_score: 9,
            open_shard_count: 2,
        };
        let mid = IngesterNode {
            node_id: "mid".into(),
            index_uid: IndexUid::for_test("idx", 0),
            source_id: "src".into(),
            capacity_score: 5,
            open_shard_count: 2,
        };
        let low = IngesterNode {
            node_id: "low".into(),
            index_uid: IndexUid::for_test("idx", 0),
            source_id: "src".into(),
            capacity_score: 1,
            open_shard_count: 2,
        };
        let candidates: Vec<&IngesterNode> = vec![&high, &mid, &low];

        let mut high_wins = 0;
        for _ in 0..1000 {
            if power_of_two_choices(&candidates).node_id == "high" {
                high_wins += 1;
            }
        }
        assert!(high_wins > 550, "high won only {high_wins}/1000 times");
    }

    #[test]
    fn test_merge_from_shards() {
        let mut table = NodeBasedRoutingTable::default();
        let index_uid = IndexUid::for_test("test-index", 0);
        let key = ("test-index".to_string(), "test-source".to_string());

        let make_shard = |id: u64, leader: &str, open: bool| Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(id)),
            shard_state: if open {
                ShardState::Open as i32
            } else {
                ShardState::Closed as i32
            },
            leader_id: leader.to_string(),
            ..Default::default()
        };

        // Two open shards on node-1, one open + one closed on node-2, only closed on node-3.
        let shards = vec![
            make_shard(1, "node-1", true),
            make_shard(2, "node-1", true),
            make_shard(3, "node-2", true),
            make_shard(4, "node-2", false),
            make_shard(5, "node-3", false),
        ];
        table.merge_from_shards(index_uid.clone(), "test-source".into(), shards);

        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 3);

        let n1 = entry.nodes.get("node-1").unwrap();
        assert_eq!(n1.open_shard_count, 2);
        assert_eq!(n1.capacity_score, 5);

        let n2 = entry.nodes.get("node-2").unwrap();
        assert_eq!(n2.open_shard_count, 1);

        let n3 = entry.nodes.get("node-3").unwrap();
        assert_eq!(n3.open_shard_count, 0);

        // Merging again adds new nodes but preserves existing ones.
        let shards = vec![make_shard(10, "node-4", true)];
        table.merge_from_shards(index_uid, "test-source".into(), shards);

        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 4);
        assert!(entry.nodes.contains_key("node-1"));
        assert!(entry.nodes.contains_key("node-2"));
        assert!(entry.nodes.contains_key("node-3"));
        assert!(entry.nodes.contains_key("node-4"));
    }
}
