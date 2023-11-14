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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use quickwit_proto::ingest::{Shard, ShardIds, ShardState};
use quickwit_proto::types::{IndexId, IndexUid, NodeId, ShardId, SourceId};
use tracing::warn;

use crate::IngesterPool;

/// The set of shards the router is aware of for the given index and source.
#[derive(Debug, Default)]
pub(super) struct ShardTableEntry {
    /// The index UID of the shards.
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub local_shards: Vec<Shard>,
    pub local_round_robin_idx: AtomicUsize,
    pub remote_shards: Vec<Shard>,
    pub remote_round_robin_idx: AtomicUsize,
}

impl ShardTableEntry {
    /// Creates a new entry and ensures that the shards are open, unique, and sorted by shard ID.
    pub fn new(
        self_node_id: &NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        mut shards: Vec<Shard>,
    ) -> Self {
        let num_shards = shards.len();

        shards.sort_unstable_by_key(|shard| shard.shard_id);
        shards.dedup_by_key(|shard| shard.shard_id);

        let (local_shards, remote_shards): (Vec<_>, Vec<_>) = shards
            .into_iter()
            .filter(|shard| shard.is_open())
            .partition(|shard| *self_node_id == shard.leader_id);

        if num_shards > local_shards.len() + remote_shards.len() {
            warn!("input shards should not contain closed shards or duplicates");
        }

        Self {
            index_uid,
            source_id,
            local_shards,
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards,
            remote_round_robin_idx: AtomicUsize::default(),
        }
    }

    /// Returns `true` if at least one shard in the table entry is open and has a leader available.
    /// As it goes through the list of shards in the entry, it populates `closed_shard_ids` and
    /// `unavailable_leaders` with the shard IDs of the closed shards and the node ID of the
    /// unavailable ingesters encountered along the way.
    pub fn has_open_shards(
        &self,
        closed_shard_ids: &mut Vec<ShardId>,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &mut HashSet<NodeId>,
    ) -> bool {
        for shards in [&self.local_shards, &self.remote_shards] {
            for shard in shards {
                if shard.is_closed() {
                    closed_shard_ids.push(shard.shard_id);
                } else if shard.is_open() {
                    if ingester_pool.contains_key(&shard.leader_id) {
                        return true;
                    } else {
                        let leader_id: NodeId = shard.leader_id.clone().into();
                        unavailable_leaders.insert(leader_id);
                    }
                }
            }
        }
        false
    }

    /// Returns the next open and available shard in the table entry in a round-robin fashion.
    pub fn next_open_shard_round_robin(&self, ingester_pool: &IngesterPool) -> Option<&Shard> {
        for (shards, round_robin_idx) in [
            (&self.local_shards, &self.local_round_robin_idx),
            (&self.remote_shards, &self.remote_round_robin_idx),
        ] {
            if shards.is_empty() {
                continue;
            }
            let mut num_attempts = 0;
            let max_num_attempts = shards.len();

            while num_attempts < max_num_attempts {
                let shard_idx = round_robin_idx.fetch_add(1, Ordering::Relaxed);
                let shard = &shards[shard_idx % shards.len()];

                if shard.is_open() && ingester_pool.contains_key(&shard.leader_id) {
                    return Some(shard);
                }
                num_attempts += 1;
            }
        }
        None
    }

    /// Closes the shards identified by their shard IDs.
    pub fn close_shards(&mut self, index_uid: &IndexUid, shard_ids: &[ShardId]) {
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
            let shard_ids_range = shards[0].shard_id..=shards[num_shards - 1].shard_id;

            for shard_id in shard_ids {
                if !shard_ids_range.contains(shard_id) {
                    continue;
                }
                if let Ok(shard_idx) = shards.binary_search_by_key(shard_id, |shard| shard.shard_id)
                {
                    shards[shard_idx].shard_state = ShardState::Closed as i32;
                }
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.local_shards.len() + self.remote_shards.len()
    }

    #[cfg(test)]
    pub fn shards(&self) -> Vec<&Shard> {
        let mut shards = Vec::with_capacity(self.len());
        shards.extend(&self.local_shards);
        shards.extend(&self.remote_shards);
        shards
    }
}

/// Stores the list of shards the router is aware of for each index and source. The resolution from
/// index and source to shards is performed using index ID (not index UID) and source ID.
#[derive(Debug)]
pub(super) struct ShardTable {
    pub self_node_id: NodeId,
    pub table: HashMap<(IndexId, SourceId), ShardTableEntry>,
}

impl ShardTable {
    pub fn find_entry(
        &self,
        index_id: impl Into<IndexId>,
        source_id: impl Into<SourceId>,
    ) -> Option<&ShardTableEntry> {
        let key = (index_id.into(), source_id.into());
        self.table.get(&key)
    }

    pub fn has_open_shards(
        &self,
        index_id: impl Into<IndexId>,
        source_id: impl Into<SourceId>,
        closed_shards: &mut Vec<ShardIds>,
        ingester_pool: &IngesterPool,
        unavailable_leaders: &mut HashSet<NodeId>,
    ) -> bool {
        if let Some(entry) = self.find_entry(index_id, source_id) {
            let mut closed_shard_ids: Vec<ShardId> = Vec::new();

            let result =
                entry.has_open_shards(&mut closed_shard_ids, ingester_pool, unavailable_leaders);

            if !closed_shard_ids.is_empty() {
                closed_shards.push(ShardIds {
                    index_uid: entry.index_uid.clone().into(),
                    source_id: entry.source_id.clone(),
                    shard_ids: closed_shard_ids,
                });
            }
            result
        } else {
            false
        }
    }

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

    pub fn insert_shards(
        &mut self,
        index_uid: impl Into<IndexUid>,
        source_id: impl Into<SourceId>,
        shards: Vec<Shard>,
    ) {
        let index_uid: IndexUid = index_uid.into();
        let index_id: IndexId = index_uid.index_id().into();
        let source_id: SourceId = source_id.into();
        let key = (index_id, source_id.clone());
        self.table.insert(
            key,
            ShardTableEntry::new(&self.self_node_id, index_uid, source_id, shards),
        );
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
    fn test_shard_table_entry_new() {
        let self_node_id: NodeId = "test-node-0".into();
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let table_entry = ShardTableEntry::new(
            &self_node_id,
            index_uid.clone(),
            source_id.clone(),
            Vec::new(),
        );
        assert_eq!(table_entry.len(), 0);

        let shards = vec![
            Shard {
                index_uid: "test-index:0".to_string(),
                shard_id: 3,
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".to_string(),
                shard_id: 1,
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".to_string(),
                shard_id: 2,
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-1".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".to_string(),
                shard_id: 1,
                shard_state: ShardState::Open as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
            Shard {
                index_uid: "test-index:0".to_string(),
                shard_id: 4,
                shard_state: ShardState::Closed as i32,
                leader_id: "test-node-0".to_string(),
                ..Default::default()
            },
        ];
        let table_entry = ShardTableEntry::new(&self_node_id, index_uid, source_id, shards);
        assert_eq!(table_entry.local_shards.len(), 2);
        assert_eq!(table_entry.local_shards[0].shard_id, 1);
        assert_eq!(table_entry.local_shards[1].shard_id, 3);

        assert_eq!(table_entry.remote_shards.len(), 1);
        assert_eq!(table_entry.remote_shards[0].shard_id, 2);
    }

    #[test]
    fn test_shard_table_entry_has_open_shards() {
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let table_entry = ShardTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: Vec::new(),
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        };
        let mut closed_shard_ids = Vec::new();
        let ingester_pool = IngesterPool::default();
        let mut unavailable_leaders = HashSet::new();

        assert!(!table_entry.has_open_shards(
            &mut closed_shard_ids,
            &ingester_pool,
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

        let table_entry = ShardTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 1,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Closed as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 2,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
            ],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        };
        assert!(table_entry.has_open_shards(
            &mut closed_shard_ids,
            &ingester_pool,
            &mut unavailable_leaders
        ));
        assert_eq!(closed_shard_ids.len(), 1);
        assert_eq!(closed_shard_ids[0], 1);
        assert!(unavailable_leaders.is_empty());

        closed_shard_ids.clear();

        let table_entry = ShardTableEntry {
            index_uid,
            source_id,
            local_shards: Vec::new(),
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 1,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Closed as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 2,
                    leader_id: "test-ingester-2".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 3,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
            ],
            remote_round_robin_idx: AtomicUsize::default(),
        };
        assert!(table_entry.has_open_shards(
            &mut closed_shard_ids,
            &ingester_pool,
            &mut unavailable_leaders
        ));
        assert_eq!(closed_shard_ids.len(), 1);
        assert_eq!(closed_shard_ids[0], 1);
        assert_eq!(unavailable_leaders.len(), 1);
        assert!(unavailable_leaders.contains("test-ingester-2"));
    }

    #[test]
    fn test_shard_table_entry_next_open_shard_round_robin() {
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let table_entry = ShardTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: Vec::new(),
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        };
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

        let table_entry = ShardTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 1,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Closed as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 2,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 3,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
            ],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        };
        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, 2);

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, 3);

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, 2);

        let table_entry = ShardTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![Shard {
                index_uid: "test-index:0".to_string(),
                shard_id: 1,
                leader_id: "test-ingester-0".to_string(),
                shard_state: ShardState::Closed as i32,
                ..Default::default()
            }],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 2,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 3,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Closed as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 4,
                    leader_id: "test-ingester-2".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 5,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
            ],
            remote_round_robin_idx: AtomicUsize::default(),
        };
        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, 2);

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, 5);

        let shard = table_entry
            .next_open_shard_round_robin(&ingester_pool)
            .unwrap();
        assert_eq!(shard.shard_id, 2);
    }

    #[test]
    fn test_shard_table_entry_close_shards() {
        let index_uid: IndexUid = IndexUid::new_2("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let mut table_entry = ShardTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: Vec::new(),
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: Vec::new(),
            remote_round_robin_idx: AtomicUsize::default(),
        };
        table_entry.close_shards(&index_uid, &[1]);

        let mut table_entry = ShardTableEntry {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
            local_shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 1,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 2,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 3,
                    leader_id: "test-ingester-0".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
            ],
            local_round_robin_idx: AtomicUsize::default(),
            remote_shards: vec![
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 5,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 6,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
                Shard {
                    index_uid: "test-index:0".to_string(),
                    shard_id: 7,
                    leader_id: "test-ingester-1".to_string(),
                    shard_state: ShardState::Open as i32,
                    ..Default::default()
                },
            ],
            remote_round_robin_idx: AtomicUsize::default(),
        };
        table_entry.close_shards(&index_uid, &[1, 3, 4, 6, 8]);
        assert!(table_entry.local_shards[0].is_closed());
        assert!(table_entry.local_shards[1].is_open());
        assert!(table_entry.local_shards[2].is_closed());
        assert!(table_entry.remote_shards[0].is_open());
        assert!(table_entry.remote_shards[1].is_closed());
        assert!(table_entry.remote_shards[2].is_open());
    }
}
