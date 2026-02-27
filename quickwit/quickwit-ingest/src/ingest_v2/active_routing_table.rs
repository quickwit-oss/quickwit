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

use std::collections::HashSet;

use quickwit_proto::ingest::Shard;
use quickwit_proto::types::{IndexUid, NodeId, SourceId};
use serde_json::{Value as JsonValue, json};

use super::node_routing_table::NodeBasedRoutingTable;
use super::routing_table::RoutingTable;
use crate::IngesterPool;

pub(super) struct RoutingTarget<'a> {
    pub node_id: &'a NodeId,
    pub index_uid: &'a IndexUid,
}

/// Gates both routing strategies behind a single interface. `ShardBased` is a temporary fallback
/// controlled by `disable_node_based_routing`. This abstraction should be removed once node-based
/// routing is stable.
#[allow(private_interfaces)]
pub enum ActiveRoutingTable {
    ShardBased(RoutingTable),
    NodeBased(NodeBasedRoutingTable),
}

impl ActiveRoutingTable {
    pub fn new_shard_based(self_node_id: NodeId) -> Self {
        Self::ShardBased(RoutingTable {
            self_node_id,
            table: Default::default(),
        })
    }

    pub fn new_node_based() -> Self {
        Self::NodeBased(NodeBasedRoutingTable::default())
    }

    pub(super) fn has_available_targets(
        &self,
        index_id: &str,
        source_id: &str,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &HashSet<NodeId>,
    ) -> bool {
        match self {
            Self::ShardBased(table) => {
                let mut dummy_closed_shards = Vec::new();
                let mut unavailable = unavailable_leaders.clone();
                table.has_open_shards(
                    index_id,
                    source_id,
                    ingester_pool,
                    &mut dummy_closed_shards,
                    &mut unavailable,
                )
            }
            Self::NodeBased(table) => {
                table.has_open_nodes(index_id, source_id, ingester_pool, unavailable_leaders)
            }
        }
    }

    pub(super) fn pick_target<'a>(
        &'a self,
        index_id: &str,
        source_id: &str,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &HashSet<NodeId>,
    ) -> Option<RoutingTarget<'a>> {
        match self {
            Self::ShardBased(table) => {
                let empty_rate_limited = HashSet::new();
                table
                    .find_entry(index_id, source_id)
                    .and_then(|entry| {
                        entry
                            .next_open_shard_round_robin(ingester_pool, &empty_rate_limited)
                            .ok()
                    })
                    .map(|entry| RoutingTarget {
                        node_id: &entry.leader_id,
                        index_uid: &entry.index_uid,
                    })
            }
            Self::NodeBased(table) => table
                .find_entry(index_id, source_id)
                .and_then(|entry| entry.pick_node(ingester_pool, unavailable_leaders))
                .map(|node| RoutingTarget {
                    node_id: &node.node_id,
                    index_uid: &node.index_uid,
                }),
        }
    }

    pub(super) fn populate_from_shards(
        &mut self,
        index_uid: IndexUid,
        source_id: String,
        open_shards: Vec<Shard>,
    ) {
        match self {
            Self::ShardBased(table) => {
                table.replace_shards(index_uid, source_id, open_shards);
            }
            Self::NodeBased(table) => {
                table.merge_from_shards(index_uid, source_id, open_shards);
            }
        }
    }

    pub(super) fn apply_capacity_update(
        &mut self,
        node_id: NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        capacity_score: usize,
        open_shard_count: usize,
    ) {
        if let Self::NodeBased(table) = self {
            table.apply_capacity_update(node_id, index_uid, source_id, capacity_score, open_shard_count);
        }
    }

    pub(super) fn debug_info(&self) -> JsonValue {
        match self {
            Self::ShardBased(table) => json!(table.debug_info()),
            Self::NodeBased(table) => json!(table.debug_info()),
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::ingester::IngesterServiceClient;
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::types::IndexUid;

    use super::*;

    fn make_shard(index_uid: &IndexUid, leader: &str, shard_id: u64) -> Shard {
        Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "test-source".to_string(),
            shard_id: Some(shard_id.into()),
            shard_state: ShardState::Open as i32,
            leader_id: leader.to_string(),
            ..Default::default()
        }
    }

    fn pool_with(nodes: &[&str]) -> IngesterPool {
        let pool = IngesterPool::default();
        for node in nodes {
            pool.insert((*node).into(), IngesterServiceClient::mocked());
        }
        pool
    }

    #[test]
    fn test_populate_from_shards() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let shards = vec![make_shard(&index_uid, "ingester-0", 1)];

        let mut shard_table = ActiveRoutingTable::new_shard_based("router".into());
        shard_table.populate_from_shards(
            index_uid.clone(),
            "test-source".to_string(),
            shards.clone(),
        );
        let ActiveRoutingTable::ShardBased(ref inner) = shard_table else {
            panic!();
        };
        assert_eq!(inner.len(), 1);
        assert!(inner.find_entry("test-index", "test-source").is_some());

        let mut node_table = ActiveRoutingTable::new_node_based();
        node_table.populate_from_shards(index_uid.clone(), "test-source".to_string(), shards);
        let ActiveRoutingTable::NodeBased(ref inner) = node_table else {
            panic!();
        };
        assert!(inner.find_entry("test-index", "test-source").is_some());
    }

    #[test]
    fn test_has_available_targets() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let shards = vec![make_shard(&index_uid, "ingester-0", 1)];
        let pool = pool_with(&["ingester-0"]);
        let unavailable = HashSet::new();

        let mut shard_table = ActiveRoutingTable::new_shard_based("router".into());
        assert!(!shard_table.has_available_targets(
            "test-index",
            "test-source",
            &pool,
            &unavailable,
        ));
        shard_table.populate_from_shards(
            index_uid.clone(),
            "test-source".to_string(),
            shards.clone(),
        );
        assert!(shard_table.has_available_targets(
            "test-index",
            "test-source",
            &pool,
            &unavailable,
        ));

        let mut node_table = ActiveRoutingTable::new_node_based();
        assert!(!node_table.has_available_targets(
            "test-index",
            "test-source",
            &pool,
            &unavailable,
        ));
        node_table.populate_from_shards(index_uid.clone(), "test-source".to_string(), shards);
        assert!(
            node_table.has_available_targets("test-index", "test-source", &pool, &unavailable,)
        );
    }

    #[test]
    fn test_pick_target() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let shards = vec![make_shard(&index_uid, "ingester-0", 1)];
        let pool = pool_with(&["ingester-0"]);
        let unavailable = HashSet::new();

        let mut shard_table = ActiveRoutingTable::new_shard_based("router".into());
        assert!(
            shard_table
                .pick_target("test-index", "test-source", &pool, &unavailable)
                .is_none()
        );
        shard_table.populate_from_shards(
            index_uid.clone(),
            "test-source".to_string(),
            shards.clone(),
        );
        let target = shard_table
            .pick_target("test-index", "test-source", &pool, &unavailable)
            .expect("should pick a target");
        assert_eq!(target.node_id.as_str(), "ingester-0");
        assert_eq!(*target.index_uid, index_uid);

        let mut node_table = ActiveRoutingTable::new_node_based();
        assert!(
            node_table
                .pick_target("test-index", "test-source", &pool, &unavailable)
                .is_none()
        );
        node_table.populate_from_shards(index_uid.clone(), "test-source".to_string(), shards);
        let target = node_table
            .pick_target("test-index", "test-source", &pool, &unavailable)
            .expect("should pick a target");
        assert_eq!(target.node_id.as_str(), "ingester-0");
        assert_eq!(*target.index_uid, index_uid);
    }

    #[test]
    fn test_debug_info() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let shards = vec![make_shard(&index_uid, "ingester-0", 1)];

        let mut shard_table = ActiveRoutingTable::new_shard_based("router".into());
        assert!(shard_table.debug_info().as_object().unwrap().is_empty());
        shard_table.populate_from_shards(
            index_uid.clone(),
            "test-source".to_string(),
            shards.clone(),
        );
        assert!(
            shard_table
                .debug_info()
                .as_object()
                .unwrap()
                .contains_key("test-index")
        );

        let mut node_table = ActiveRoutingTable::new_node_based();
        assert!(node_table.debug_info().as_object().unwrap().is_empty());
        node_table.populate_from_shards(index_uid.clone(), "test-source".to_string(), shards);
        assert!(
            node_table
                .debug_info()
                .as_object()
                .unwrap()
                .contains_key("test-index")
        );
    }
}