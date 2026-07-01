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

use std::cmp::Ordering;
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
    /// Score from 0-10. Higher means more available capacity.
    pub capacity_score: usize,
    /// Number of open shards on this node for this (index, source) pair. Tiebreaker for power of
    /// two choices comparison - we favor a node with more open shards.
    pub open_shard_count: usize,
}

impl IngesterNode {
    fn is_routing_candidate(
        &self,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &mut HashSet<NodeId>,
    ) -> bool {
        if self.capacity_score == 0 || self.open_shard_count == 0 {
            return false;
        }
        if unavailable_leaders.contains(&self.node_id) {
            return false;
        }
        let is_ready = ingester_pool
            .get(&self.node_id)
            .map(|ingester| ingester.status.is_ready())
            .unwrap_or(false);

        if !is_ready {
            unavailable_leaders.insert(self.node_id.clone());
        }
        is_ready
    }
}

#[derive(Debug)]
pub(super) struct RoutingEntry {
    pub index_uid: IndexUid,
    pub nodes: HashMap<NodeId, IngesterNode>,
    /// Whether this entry has been seeded from a control plane response. During a rolling
    /// deployment, Chitchat broadcasts from already-upgraded nodes may populate the table
    /// before the router ever asks the CP, causing it to miss old-version nodes. This flag
    /// ensures the router asks the CP at least once per (index, source) pair.
    seeded_from_cp: bool,
}

impl RoutingEntry {
    fn new(index_uid: IndexUid) -> Self {
        Self {
            index_uid,
            nodes: HashMap::new(),
            seeded_from_cp: false,
        }
    }
}

/// Given a slice of candidates, picks the better of two random choices.
/// Higher capacity_score wins; tiebreak on more open_shard_count (more landing spots).
fn power_of_two_choices<'a>(candidates: &[&'a IngesterNode]) -> &'a IngesterNode {
    debug_assert!(candidates.len() >= 2);
    let mut iter = candidates.sample(&mut rng(), 2);
    let (&a, &b) = (iter.next().unwrap(), iter.next().unwrap());

    if (a.capacity_score, a.open_shard_count) >= (b.capacity_score, b.open_shard_count) {
        a
    } else {
        b
    }
}

fn pick_from(candidates: Vec<&IngesterNode>) -> Option<&IngesterNode> {
    match candidates.len() {
        0 => None,
        1 => Some(candidates[0]),
        _ => Some(power_of_two_choices(&candidates)),
    }
}

impl RoutingEntry {
    /// Pick an ingester node to persist the request to. Uses power of two choices based on reported
    /// ingester capacity, if more than one eligible node exists. Prefers nodes in the same
    /// availability zone, falling back to remote nodes.
    fn pick_node(
        &self,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &HashSet<NodeId>,
        self_availability_zone: &Option<String>,
    ) -> Option<&IngesterNode> {
        let (local_ingesters, remote_ingesters): (Vec<&IngesterNode>, Vec<&IngesterNode>) = self
            .nodes
            .values()
            .filter(|node| {
                node.capacity_score > 0
                    && node.open_shard_count > 0
                    && ingester_pool
                        .get(&node.node_id)
                        .map(|entry| entry.status.is_ready())
                        .unwrap_or(false)
                    && !unavailable_leaders.contains(&node.node_id)
            })
            .partition(|node| {
                let node_az = ingester_pool
                    .get(&node.node_id)
                    .and_then(|h| h.availability_zone);
                node_az == *self_availability_zone
            });

        pick_from(local_ingesters).or_else(|| pick_from(remote_ingesters))
    }
}

#[derive(Debug, Default)]
pub(super) struct RoutingTable {
    table: HashMap<(IndexId, SourceId), RoutingEntry>,
    self_availability_zone: Option<String>,
}

impl RoutingTable {
    pub fn new(self_availability_zone: Option<String>) -> Self {
        Self {
            self_availability_zone,
            ..Default::default()
        }
    }

    pub fn pick_node(
        &self,
        index_id: &str,
        source_id: &str,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &HashSet<NodeId>,
    ) -> Option<&IngesterNode> {
        let key = (index_id.to_string(), source_id.to_string());
        let entry = self.table.get(&key)?;
        entry.pick_node(
            ingester_pool,
            unavailable_leaders,
            &self.self_availability_zone,
        )
    }

    pub fn classify_az_locality(
        &self,
        target_node_id: &NodeId,
        ingester_pool: &IngesterPool,
    ) -> &'static str {
        let Some(self_az) = &self.self_availability_zone else {
            return "az_unaware";
        };
        let target_az = ingester_pool
            .get(target_node_id)
            .and_then(|entry| entry.availability_zone);
        match target_az {
            Some(ref az) if az == self_az => "same_az",
            Some(_) => "cross_az",
            None => "az_unaware",
        }
    }

    pub fn debug_info(
        &self,
        ingester_pool: &IngesterPool,
    ) -> HashMap<IndexId, Vec<serde_json::Value>> {
        let mut per_index: HashMap<IndexId, Vec<serde_json::Value>> = HashMap::new();
        for ((index_id, source_id), entry) in &self.table {
            for (node_id, node) in &entry.nodes {
                let az = ingester_pool.get(node_id).and_then(|h| h.availability_zone);
                per_index
                    .entry(index_id.clone())
                    .or_default()
                    .push(serde_json::json!({
                        "source_id": source_id,
                        "node_id": node_id,
                        "capacity_score": node.capacity_score,
                        "open_shard_count": node.open_shard_count,
                        "availability_zone": az,
                    }));
            }
        }
        per_index
    }

    /// Returns `true` if the entry has at least one routing candidate, i.e. an available node with
    /// at least one open shard and a capacity score greater than 0. As it scans the entry, it
    /// records any leader that has open shards but is no longer in the ingester pool or is not
    /// ready into `unavailable_leaders`, so they can be reported to the control plane.
    pub fn has_any_routing_candidate(
        &self,
        index_id: &str,
        source_id: &str,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &mut HashSet<NodeId>,
    ) -> bool {
        let key = (index_id.to_string(), source_id.to_string());
        let Some(entry) = self.table.get(&key) else {
            return false;
        };
        // Routers must sync with the control plane at least once per (index, source).
        if !entry.seeded_from_cp {
            return false;
        }
        // We scan every node (rather than short-circuiting with `any`) so that all unavailable
        // nodes are recorded and reported to the control plane in a single round.
        let mut has_any_candidate = false;

        for node in entry.nodes.values() {
            has_any_candidate |= node.is_routing_candidate(ingester_pool, unavailable_leaders);
        }
        has_any_candidate
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
        let key = (index_uid.index_id.to_string(), source_id);

        let entry = self
            .table
            .entry(key)
            .or_insert_with(|| RoutingEntry::new(index_uid.clone()));
        match entry.index_uid.cmp(&index_uid) {
            // If we receive an update for a new incarnation of the index, then we clear the entry.
            Ordering::Less => {
                entry.index_uid = index_uid.clone();
                entry.nodes.clear();
                entry.seeded_from_cp = false;
            }
            // If we receive an update for a previous incarnation of the index, then we ignore it.
            Ordering::Greater => return,
            Ordering::Equal => {}
        }
        let ingester_node = IngesterNode {
            node_id: node_id.clone(),
            index_uid,
            capacity_score,
            open_shard_count,
        };
        entry.nodes.insert(node_id, ingester_node);
    }

    /// Zeros out the open shard count for `node_id` on the (index, source) entry while preserving
    /// its capacity score. Called when a persist response reports that the ingester no longer
    /// holds a shard for this (index_uid, source_id), so the entry stops being picked until a
    /// fresh routing update or control-plane response repopulates it.
    ///
    /// Mirrors the incarnation handling of [`Self::apply_capacity_update`]: a stale signal
    /// (entry newer than `index_uid`) is ignored, and a signal for a newer incarnation advances
    /// the entry and clears stale nodes so the next attempt re-queries the control plane.
    pub fn mark_node_no_shards(&mut self, node_id: &NodeId, index_uid: &IndexUid, source_id: &str) {
        let key = (index_uid.index_id.to_string(), source_id.to_string());
        let Some(entry) = self.table.get_mut(&key) else {
            return;
        };
        match entry.index_uid.cmp(index_uid) {
            // The entry is stale relative to the signal: advance it, drop stale nodes, and force
            // a control-plane re-seed on the next attempt.
            Ordering::Less => {
                entry.index_uid = index_uid.clone();
                entry.nodes.clear();
                entry.seeded_from_cp = false;
            }
            // The signal is stale relative to the entry: leave the fresher entry alone.
            Ordering::Greater => return,
            Ordering::Equal => {}
        }
        if let Some(node) = entry.nodes.get_mut(node_id) {
            node.open_shard_count = 0;
        }
    }

    /// Merges routing updates from a GetOrCreateOpenShards control plane response into the
    /// table. For existing nodes, updates their open shard count, including if the count is 0, from
    /// the CP response while preserving capacity scores if they already exist.
    /// New nodes get a default capacity_score of 5.
    pub fn merge_from_shards(
        &mut self,
        index_uid: IndexUid,
        source_id: SourceId,
        shards: Vec<Shard>,
    ) {
        let key = (index_uid.index_id.to_string(), source_id);
        let entry = self
            .table
            .entry(key)
            .or_insert_with(|| RoutingEntry::new(index_uid.clone()));
        match entry.index_uid.cmp(&index_uid) {
            // If we receive an update for a new incarnation of the index, then we clear the entry.
            Ordering::Less => {
                entry.index_uid = index_uid.clone();
                entry.nodes.clear();
            }
            // If we receive an update for a previous incarnation of the index, then we ignore it.
            Ordering::Greater => return,
            Ordering::Equal => {}
        }

        let per_leader_count: HashMap<NodeId, usize> = shards
            .iter()
            .map(|shard| {
                let num_open_shards = shard.is_open() as usize;
                let leader_id = NodeId::from_str(&shard.leader_id);
                (leader_id, num_open_shards)
            })
            .into_grouping_map()
            .sum();

        for (node_id, open_shard_count) in per_leader_count {
            entry
                .nodes
                .entry(node_id.clone())
                .and_modify(|node| node.open_shard_count = open_shard_count)
                .or_insert_with(|| IngesterNode {
                    node_id,
                    index_uid: index_uid.clone(),
                    capacity_score: 5,
                    open_shard_count,
                });
        }
        entry.seeded_from_cp = true;
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::ingest::ingester::{IngesterServiceClient, IngesterStatus};
    use quickwit_proto::types::ShardId;

    use super::*;
    use crate::IngesterPoolEntry;

    fn mocked_ingester(availability_zone: Option<&str>) -> IngesterPoolEntry {
        IngesterPoolEntry {
            client: IngesterServiceClient::mocked(),
            status: IngesterStatus::Ready,
            availability_zone: availability_zone.map(|s| s.to_string()),
        }
    }

    fn ingester_node(
        node_id: &str,
        capacity_score: usize,
        open_shard_count: usize,
    ) -> IngesterNode {
        IngesterNode {
            node_id: NodeId::from_str(node_id),
            index_uid: IndexUid::for_test("test-index", 0),
            capacity_score,
            open_shard_count,
        }
    }

    #[test]
    fn test_ingester_node_is_routing_candidate() {
        let pool = IngesterPool::default();
        pool.insert(NodeId::from_str("node-1"), mocked_ingester(None));

        // No capacity or no open shards: not a routing candidate, not reported as unavailable.
        let mut unavailable_leaders = HashSet::new();
        assert!(
            !ingester_node("node-1", 0, 3).is_routing_candidate(&pool, &mut unavailable_leaders)
        );
        assert!(
            !ingester_node("node-1", 5, 0).is_routing_candidate(&pool, &mut unavailable_leaders)
        );
        assert!(unavailable_leaders.is_empty());

        // Open shards and a ready leader: open, not reported as unavailable.
        assert!(
            ingester_node("node-1", 5, 3).is_routing_candidate(&pool, &mut unavailable_leaders)
        );
        assert!(unavailable_leaders.is_empty());

        // Open shards but the leader is missing from the pool: not open, reported as unavailable.
        assert!(
            !ingester_node("node-2", 5, 3).is_routing_candidate(&pool, &mut unavailable_leaders)
        );
        assert_eq!(
            unavailable_leaders,
            HashSet::from([NodeId::from_str("node-2")])
        );

        // A leader already known to be unavailable is skipped without re-inserting.
        assert!(
            !ingester_node("node-2", 5, 3).is_routing_candidate(&pool, &mut unavailable_leaders)
        );
        assert_eq!(
            unavailable_leaders,
            HashSet::from([NodeId::from_str("node-2")])
        );
    }

    #[test]
    fn test_apply_capacity_update() {
        let mut table = RoutingTable::default();
        let key = ("test-index".to_string(), "test-source".into());

        // Insert first node.
        table.apply_capacity_update(
            NodeId::from_str("node-1"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            8,
            3,
        );
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 1);
        assert_eq!(entry.nodes.get("node-1").unwrap().capacity_score, 8);

        // Update existing node.
        table.apply_capacity_update(
            NodeId::from_str("node-1"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            4,
            5,
        );
        let node = table.table.get(&key).unwrap().nodes.get("node-1").unwrap();
        assert_eq!(node.capacity_score, 4);
        assert_eq!(node.open_shard_count, 5);

        // Add second node.
        table.apply_capacity_update(
            NodeId::from_str("node-2"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            6,
            2,
        );
        assert_eq!(table.table.get(&key).unwrap().nodes.len(), 2);

        // Zero shards: node stays in table but becomes ineligible for routing.
        table.apply_capacity_update(
            NodeId::from_str("node-1"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            0,
            0,
        );
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 2);
        assert_eq!(entry.nodes.get("node-1").unwrap().open_shard_count, 0);
        assert_eq!(entry.nodes.get("node-1").unwrap().capacity_score, 0);
    }

    #[test]
    fn test_has_any_routing_candidate() {
        let mut table = RoutingTable::default();
        let pool = IngesterPool::default();
        let index_uid = IndexUid::for_test("test-index", 0);

        // Empty table.
        assert!(!table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut HashSet::new()
        ));

        // Seed from CP so has_any_routing_candidate can return true.
        let shards = vec![
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(1u64)),
                shard_state: ShardState::Open as i32,
                leader_id: "node-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: Some(index_uid.clone()),
                source_id: "test-source".to_string(),
                shard_id: Some(ShardId::from(2u64)),
                shard_state: ShardState::Open as i32,
                leader_id: "node-2".to_string(),
                ..Default::default()
            },
        ];
        table.merge_from_shards(index_uid.clone(), "test-source".into(), shards);

        // Neither node is in the pool: both leaders are recorded as unavailable and reported to
        // the control plane.
        let mut unavailable_leaders: HashSet<NodeId> = HashSet::new();
        assert!(!table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut unavailable_leaders
        ));
        assert_eq!(unavailable_leaders.len(), 2);
        assert!(unavailable_leaders.contains(&NodeId::from_str("node-1")));
        assert!(unavailable_leaders.contains(&NodeId::from_str("node-2")));

        // node-1 is in pool → true. node-2 is still missing from the pool and gets recorded.
        pool.insert(NodeId::from_str("node-1"), mocked_ingester(None));
        let mut unavailable_leaders = HashSet::new();
        assert!(table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut unavailable_leaders
        ));
        assert_eq!(
            unavailable_leaders,
            HashSet::from([NodeId::from_str("node-2")])
        );

        // node-1 is already known to be unavailable, and node-2 is not in the pool → false. The
        // leader already in the set is left untouched.
        let mut unavailable_leaders: HashSet<NodeId> = HashSet::from([NodeId::from_str("node-1")]);
        assert!(!table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut unavailable_leaders
        ));

        // Second node available → true despite first being unavailable.
        pool.insert(NodeId::from_str("node-2"), mocked_ingester(None));
        let mut unavailable_leaders: HashSet<NodeId> = HashSet::from([NodeId::from_str("node-1")]);
        assert!(table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut unavailable_leaders
        ));

        // Node with capacity_score=0 is not eligible and is not reported as unavailable.
        table.apply_capacity_update(
            NodeId::from_str("node-2"),
            index_uid.clone(),
            "test-source".into(),
            0,
            2,
        );
        let mut unavailable_leaders: HashSet<NodeId> = HashSet::from([NodeId::from_str("node-1")]);
        assert!(!table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut unavailable_leaders
        ));
        assert_eq!(
            unavailable_leaders,
            HashSet::from([NodeId::from_str("node-1")])
        );
    }

    #[test]
    fn test_has_any_routing_candidate_requires_cp_seed() {
        let mut table = RoutingTable::default();
        let pool = IngesterPool::default();
        pool.insert(NodeId::from_str("node-1"), mocked_ingester(None));

        // Chitchat broadcast populates the entry, but has_any_routing_candidate still returns false
        // because the entry hasn't been seeded from the control plane yet.
        table.apply_capacity_update(
            NodeId::from_str("node-1"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            8,
            3,
        );
        assert!(!table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut HashSet::new()
        ));

        // After merge_from_shards (CP response), has_any_routing_candidate returns true.
        let shards = vec![Shard {
            index_uid: Some(IndexUid::for_test("test-index", 0)),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1u64)),
            shard_state: ShardState::Open as i32,
            leader_id: "node-1".to_string(),
            ..Default::default()
        }];
        table.merge_from_shards(
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            shards,
        );
        assert!(table.has_any_routing_candidate(
            "test-index",
            "test-source",
            &pool,
            &mut HashSet::new()
        ));
    }

    #[test]
    fn test_pick_node_prefers_same_az() {
        let mut table = RoutingTable::new(Some("az-1".to_string()));
        let pool = IngesterPool::default();

        table.apply_capacity_update(
            NodeId::from_str("node-1"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            5,
            1,
        );
        table.apply_capacity_update(
            NodeId::from_str("node-2"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            5,
            1,
        );
        pool.insert(NodeId::from_str("node-1"), mocked_ingester(Some("az-1")));
        pool.insert(NodeId::from_str("node-2"), mocked_ingester(Some("az-2")));

        let picked = table
            .pick_node("test-index", "test-source", &pool, &HashSet::new())
            .unwrap();
        assert_eq!(picked.node_id, NodeId::from_str("node-1"));
    }

    #[test]
    fn test_pick_node_falls_back_to_cross_az() {
        let mut table = RoutingTable::new(Some("az-1".to_string()));
        let pool = IngesterPool::default();

        table.apply_capacity_update(
            NodeId::from_str("node-2"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            5,
            1,
        );
        pool.insert(NodeId::from_str("node-2"), mocked_ingester(Some("az-2")));

        let picked = table
            .pick_node("test-index", "test-source", &pool, &HashSet::new())
            .unwrap();
        assert_eq!(picked.node_id, NodeId::from_str("node-2"));
    }

    #[test]
    fn test_pick_node_no_az_awareness() {
        let mut table = RoutingTable::default();
        let pool = IngesterPool::default();

        table.apply_capacity_update(
            NodeId::from_str("node-1"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            5,
            1,
        );
        pool.insert(NodeId::from_str("node-1"), mocked_ingester(Some("az-1")));

        let picked = table
            .pick_node("test-index", "test-source", &pool, &HashSet::new())
            .unwrap();
        assert_eq!(picked.node_id, NodeId::from_str("node-1"));
    }

    #[test]
    fn test_pick_node_missing_entry() {
        let table = RoutingTable::new(Some("az-1".to_string()));
        let pool = IngesterPool::default();

        assert!(
            table
                .pick_node("nonexistent", "source", &pool, &HashSet::new())
                .is_none()
        );
    }

    #[test]
    fn test_power_of_two_choices() {
        // 3 candidates: best appears in the random pair 2/3 of the time and always
        // wins when it does, so it should win ~67% of 1000 runs. Asserting > 550
        // is ~7.5 standard deviations from the mean — effectively impossible to flake.
        let high = IngesterNode {
            node_id: NodeId::from_str("high"),
            index_uid: IndexUid::for_test("idx", 0),
            capacity_score: 9,
            open_shard_count: 2,
        };
        let mid = IngesterNode {
            node_id: NodeId::from_str("mid"),
            index_uid: IndexUid::for_test("idx", 0),
            capacity_score: 5,
            open_shard_count: 2,
        };
        let low = IngesterNode {
            node_id: NodeId::from_str("low"),
            index_uid: IndexUid::for_test("idx", 0),
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
        let mut table = RoutingTable::default();
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

    #[test]
    fn test_classify_az_locality() {
        let table = RoutingTable::new(Some("az-1".to_string()));
        let pool = IngesterPool::default();
        pool.insert(
            NodeId::from_str("node-local"),
            mocked_ingester(Some("az-1")),
        );
        pool.insert(
            NodeId::from_str("node-remote"),
            mocked_ingester(Some("az-2")),
        );
        pool.insert(NodeId::from_str("node-no-az"), mocked_ingester(None));

        assert_eq!(
            table.classify_az_locality(&NodeId::from_str("node-local"), &pool),
            "same_az"
        );
        assert_eq!(
            table.classify_az_locality(&NodeId::from_str("node-remote"), &pool),
            "cross_az"
        );
        assert_eq!(
            table.classify_az_locality(&NodeId::from_str("node-no-az"), &pool),
            "az_unaware"
        );

        let table_no_az = RoutingTable::default();
        assert_eq!(
            table_no_az.classify_az_locality(&NodeId::from_str("node-local"), &pool),
            "az_unaware"
        );
    }

    #[test]
    fn test_incarnation_check_clears_stale_nodes() {
        let mut table = RoutingTable::default();
        let key = ("test-index".to_string(), "test-source".to_string());

        // Populate with incarnation 0: two nodes.
        table.apply_capacity_update(
            NodeId::from_str("node-1"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            8,
            3,
        );
        table.apply_capacity_update(
            NodeId::from_str("node-2"),
            IndexUid::for_test("test-index", 0),
            "test-source".into(),
            6,
            2,
        );
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 2);
        assert_eq!(entry.index_uid, IndexUid::for_test("test-index", 0));

        // Capacity update with incarnation 1 clears stale nodes.
        table.apply_capacity_update(
            NodeId::from_str("node-3"),
            IndexUid::for_test("test-index", 1),
            "test-source".into(),
            5,
            1,
        );
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 1);
        assert!(entry.nodes.contains_key("node-3"));
        assert!(!entry.nodes.contains_key("node-1"));
        assert!(!entry.nodes.contains_key("node-2"));
        assert_eq!(entry.index_uid, IndexUid::for_test("test-index", 1));

        // merge_from_shards with incarnation 2 clears stale nodes.
        let shards = vec![Shard {
            index_uid: Some(IndexUid::for_test("test-index", 2)),
            source_id: "test-source".to_string(),
            shard_id: Some(ShardId::from(1u64)),
            shard_state: ShardState::Open as i32,
            leader_id: "node-4".to_string(),
            ..Default::default()
        }];
        table.merge_from_shards(
            IndexUid::for_test("test-index", 2),
            "test-source".into(),
            shards,
        );
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.len(), 1);
        assert!(entry.nodes.contains_key("node-4"));
        assert!(!entry.nodes.contains_key("node-3"));
        assert_eq!(entry.index_uid, IndexUid::for_test("test-index", 2));
    }

    #[test]
    fn test_mark_node_no_shards() {
        let mut table = RoutingTable::default();
        let index_uid = IndexUid::for_test("test-index", 1);
        let key = ("test-index".to_string(), "test-source".to_string());

        // Missing entry: no-op, no panic, nothing inserted.
        table.mark_node_no_shards(&"node-1".into(), &index_uid, "test-source");
        assert!(table.table.get(&key).is_none());

        // Seed an entry with two nodes carrying real capacity scores.
        table.apply_capacity_update(
            "node-1".into(),
            index_uid.clone(),
            "test-source".into(),
            8,
            3,
        );
        table.apply_capacity_update(
            "node-2".into(),
            index_uid.clone(),
            "test-source".into(),
            6,
            2,
        );

        // Missing node within the entry: no-op.
        table.mark_node_no_shards(&"unknown".into(), &index_uid, "test-source");
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.nodes.get("node-1").unwrap().open_shard_count, 3);
        assert_eq!(entry.nodes.get("node-2").unwrap().open_shard_count, 2);

        // Matching incarnation: zero only the open shard count, capacity score is preserved.
        table.mark_node_no_shards(&"node-1".into(), &index_uid, "test-source");
        let entry = table.table.get(&key).unwrap();
        let node_1 = entry.nodes.get("node-1").unwrap();
        assert_eq!(node_1.open_shard_count, 0);
        assert_eq!(node_1.capacity_score, 8);
        // Sibling node untouched.
        let node_2 = entry.nodes.get("node-2").unwrap();
        assert_eq!(node_2.open_shard_count, 2);
        assert_eq!(node_2.capacity_score, 6);

        // Older incarnation argument: no-op (must not roll the entry back).
        let stale_index_uid = IndexUid::for_test("test-index", 0);
        table.mark_node_no_shards(&"node-2".into(), &stale_index_uid, "test-source");
        assert_eq!(
            table
                .table
                .get(&key)
                .unwrap()
                .nodes
                .get("node-2")
                .unwrap()
                .open_shard_count,
            2
        );

        // Newer incarnation argument: advance the entry, drop stale nodes, and force a CP
        // re-seed (mirrors apply_capacity_update's Less arm). No node is inserted — the next
        // CP query is responsible for repopulating the entry.
        let newer_index_uid = IndexUid::for_test("test-index", 2);
        table.mark_node_no_shards(&"node-2".into(), &newer_index_uid, "test-source");
        let entry = table.table.get(&key).unwrap();
        assert_eq!(entry.index_uid, newer_index_uid);
        assert!(entry.nodes.is_empty());
        assert!(!entry.seeded_from_cp);
    }
}
