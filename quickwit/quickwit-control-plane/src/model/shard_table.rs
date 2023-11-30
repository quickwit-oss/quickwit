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
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use fnv::{FnvHashMap, FnvHashSet};
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::tower::ConstantRate;
use quickwit_ingest::{RateMibPerSec, ShardInfo, ShardInfos};
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceId, SourceUid};
use tracing::{error, warn};

/// Limits the number of shards that can be opened for scaling up a source to 5 per minute.
const SCALING_UP_RATE_LIMITER_SETTINGS: RateLimiterSettings = RateLimiterSettings {
    burst_limit: 5,
    rate_limit: ConstantRate::new(5, Duration::from_secs(60)),
    refill_period: Duration::from_secs(12),
};

/// Limits the number of shards that can be closed for scaling down a source to 1 per minute.
const SCALING_DOWN_RATE_LIMITER_SETTINGS: RateLimiterSettings = RateLimiterSettings {
    burst_limit: 1,
    rate_limit: ConstantRate::new(1, Duration::from_secs(60)),
    refill_period: Duration::from_secs(60),
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum ScalingMode {
    Up,
    Down,
}

pub(crate) type NextShardId = ShardId;

#[derive(Debug, Clone)]
pub(crate) struct ShardEntry {
    pub shard: Shard,
    pub ingestion_rate: RateMibPerSec,
}

impl Deref for ShardEntry {
    type Target = Shard;

    fn deref(&self) -> &Self::Target {
        &self.shard
    }
}

impl DerefMut for ShardEntry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shard
    }
}

impl From<Shard> for ShardEntry {
    fn from(shard: Shard) -> Self {
        Self {
            shard,
            ingestion_rate: RateMibPerSec::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ShardTableEntry {
    shard_entries: FnvHashMap<ShardId, ShardEntry>,
    next_shard_id: NextShardId,
    scaling_up_rate_limiter: RateLimiter,
    scaling_down_rate_limiter: RateLimiter,
}

impl Default for ShardTableEntry {
    fn default() -> Self {
        Self {
            shard_entries: Default::default(),
            next_shard_id: Self::DEFAULT_NEXT_SHARD_ID,
            scaling_up_rate_limiter: RateLimiter::from_settings(SCALING_UP_RATE_LIMITER_SETTINGS),
            scaling_down_rate_limiter: RateLimiter::from_settings(
                SCALING_DOWN_RATE_LIMITER_SETTINGS,
            ),
        }
    }
}

impl ShardTableEntry {
    const DEFAULT_NEXT_SHARD_ID: NextShardId = 1; // `1` matches the PostgreSQL sequence min value.

    pub fn from_shards(shards: Vec<Shard>, next_shard_id: NextShardId) -> Self {
        let shard_entries = shards
            .into_iter()
            .map(|shard| (shard.shard_id, shard.into()))
            .collect();
        Self {
            shard_entries,
            next_shard_id,
            ..Default::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.shard_entries.is_empty()
    }

    fn is_default(&self) -> bool {
        self.is_empty() && self.next_shard_id == Self::DEFAULT_NEXT_SHARD_ID
    }
}

// A table that keeps track of the existing shards for each index and source.
#[derive(Debug, Default)]
pub(crate) struct ShardTable {
    pub table_entries: FnvHashMap<SourceUid, ShardTableEntry>,
}

impl ShardTable {
    /// Removes all the entries that match the target index ID.
    pub fn delete_index(&mut self, index_id: &str) {
        self.table_entries
            .retain(|source_uid, _| source_uid.index_uid.index_id() != index_id);
    }

    /// Adds a new empty entry for the given index and source.
    ///
    /// TODO check and document the behavior on error (if the source was already here).
    pub fn add_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
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

    pub fn delete_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        self.table_entries.remove(&source_uid);
    }

    pub fn all_shards_mut(&mut self) -> impl Iterator<Item = &mut ShardEntry> + '_ {
        self.table_entries
            .values_mut()
            .flat_map(|table_entry| table_entry.shard_entries.values_mut())
    }

    /// Lists the shards of a given source. Returns `None` if the source does not exist.
    pub fn list_shards(&self, source_uid: &SourceUid) -> Option<impl Iterator<Item = &ShardEntry>> {
        self.table_entries
            .get(source_uid)
            .map(|table_entry| table_entry.shard_entries.values())
    }

    pub fn next_shard_id(&self, source_uid: &SourceUid) -> Option<ShardId> {
        self.table_entries
            .get(source_uid)
            .map(|table_entry| table_entry.next_shard_id)
    }

    /// Updates the shard table.
    pub fn insert_newly_opened_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        opened_shards: Vec<Shard>,
        next_shard_id: NextShardId,
    ) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        match self.table_entries.entry(source_uid) {
            Entry::Occupied(mut entry) => {
                let table_entry = entry.get_mut();

                for opened_shard in opened_shards {
                    // We only insert shards that we don't know about because the control plane
                    // knows more about the state of the shards than the metastore.
                    table_entry
                        .shard_entries
                        .entry(opened_shard.shard_id)
                        .or_insert(opened_shard.into());
                }
                table_entry.next_shard_id = next_shard_id;
            }
            // This should never happen if the control plane view is consistent with the state of
            // the metastore, so should we panic here? Warnings are most likely going to go
            // unnoticed.
            Entry::Vacant(entry) => {
                let shard_entries: FnvHashMap<ShardId, ShardEntry> = opened_shards
                    .into_iter()
                    .map(|shard| (shard.shard_id, shard.into()))
                    .collect();
                let table_entry = ShardTableEntry {
                    shard_entries,
                    next_shard_id,
                    ..Default::default()
                };
                entry.insert(table_entry);
            }
        }
    }

    /// Finds open shards for a given index and source and whose leaders are not in the set of
    /// unavailable ingesters.
    pub fn find_open_shards(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        unavailable_leaders: &FnvHashSet<NodeId>,
    ) -> Option<(Vec<ShardEntry>, NextShardId)> {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = self.table_entries.get(&source_uid)?;
        let open_shards: Vec<ShardEntry> = table_entry
            .shard_entries
            .values()
            .filter(|shard_entry| {
                shard_entry.shard.is_open() && !unavailable_leaders.contains(&shard_entry.leader_id)
            })
            .cloned()
            .collect();

        Some((open_shards, table_entry.next_shard_id))
    }

    pub fn update_shards(
        &mut self,
        source_uid: &SourceUid,
        shard_infos: &ShardInfos,
    ) -> ShardStats {
        let mut num_open_shards = 0;
        let mut ingestion_rate_sum = RateMibPerSec::default();

        if let Some(table_entry) = self.table_entries.get_mut(source_uid) {
            for shard_info in shard_infos {
                let ShardInfo {
                    shard_id,
                    shard_state,
                    ingestion_rate,
                } = shard_info;

                if let Some(shard_entry) = table_entry.shard_entries.get_mut(shard_id) {
                    shard_entry.ingestion_rate = *ingestion_rate;
                    // `ShardInfos` are broadcasted via Chitchat and eventually consistent. As a
                    // result, we can only trust the `Closed` state, which is final.
                    if shard_state.is_closed() {
                        shard_entry.set_shard_state(ShardState::Closed);
                    }
                }
            }
            for shard_entry in table_entry.shard_entries.values() {
                if shard_entry.is_open() {
                    num_open_shards += 1;
                    ingestion_rate_sum += shard_entry.ingestion_rate;
                }
            }
        }
        let avg_ingestion_rate = if num_open_shards > 0 {
            ingestion_rate_sum.0 as f32 / num_open_shards as f32
        } else {
            0.0
        };
        ShardStats {
            num_open_shards,
            avg_ingestion_rate,
        }
    }

    /// Sets the state of the shards identified by their index UID, source ID, and shard IDs to
    /// `Closed`.
    pub fn close_shards(&mut self, source_uid: &SourceUid, shard_ids: &[ShardId]) -> Vec<ShardId> {
        let mut closed_shard_ids = Vec::new();

        if let Some(table_entry) = self.table_entries.get_mut(source_uid) {
            for shard_id in shard_ids {
                if let Some(shard_entry) = table_entry.shard_entries.get_mut(shard_id) {
                    if !shard_entry.is_closed() {
                        shard_entry.set_shard_state(ShardState::Closed);
                        closed_shard_ids.push(*shard_id);
                    }
                }
            }
        }
        closed_shard_ids
    }

    /// Removes the shards identified by their index UID, source ID, and shard IDs.
    pub fn delete_shards(&mut self, source_uid: &SourceUid, shard_ids: &[ShardId]) {
        if let Some(table_entry) = self.table_entries.get_mut(source_uid) {
            for shard_id in shard_ids {
                if table_entry.shard_entries.remove(shard_id).is_none() {
                    warn!(shard = *shard_id, "deleting a non-existing shard");
                }
            }
        }
    }

    pub fn acquire_scaling_permits(
        &mut self,
        source_uid: &SourceUid,
        scaling_mode: ScalingMode,
        num_permits: u64,
    ) -> Option<bool> {
        let table_entry = self.table_entries.get_mut(source_uid)?;
        let scaling_rate_limiter = match scaling_mode {
            ScalingMode::Up => &mut table_entry.scaling_up_rate_limiter,
            ScalingMode::Down => &mut table_entry.scaling_down_rate_limiter,
        };
        Some(scaling_rate_limiter.acquire(num_permits))
    }

    pub fn release_scaling_permits(
        &mut self,
        source_uid: &SourceUid,
        scaling_mode: ScalingMode,
        num_permits: u64,
    ) {
        if let Some(table_entry) = self.table_entries.get_mut(source_uid) {
            let scaling_rate_limiter = match scaling_mode {
                ScalingMode::Up => &mut table_entry.scaling_up_rate_limiter,
                ScalingMode::Down => &mut table_entry.scaling_down_rate_limiter,
            };
            scaling_rate_limiter.release(num_permits);
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ShardStats {
    pub num_open_shards: usize,
    pub avg_ingestion_rate: f32,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use itertools::Itertools;
    use quickwit_proto::ingest::Shard;

    use super::*;

    impl ShardTableEntry {
        pub fn shards(&self) -> Vec<Shard> {
            self.shard_entries
                .values()
                .map(|shard_entry| shard_entry.shard.clone())
                .sorted_unstable_by_key(|shard| shard.shard_id)
                .collect()
        }
    }

    impl ShardTable {
        pub fn find_open_shards_sorted(
            &self,
            index_uid: &IndexUid,
            source_id: &SourceId,
            unavailable_leaders: &FnvHashSet<NodeId>,
        ) -> Option<(Vec<ShardEntry>, NextShardId)> {
            self.find_open_shards(index_uid, source_id, unavailable_leaders)
                .map(|(mut shards, next_shard_id)| {
                    shards.sort_by_key(|shard_entry| shard_entry.shard.shard_id);
                    (shards, next_shard_id)
                })
        }
    }

    #[test]
    fn test_shard_table_delete_index() {
        let mut shard_table = ShardTable::default();
        shard_table.delete_index("test-index");

        let index_uid_0: IndexUid = "test-index-foo:0".into();
        let source_id_0 = "test-source-0".to_string();
        shard_table.add_source(&index_uid_0, &source_id_0);

        let source_id_1 = "test-source-1".to_string();
        shard_table.add_source(&index_uid_0, &source_id_1);

        let index_uid_1: IndexUid = "test-index-bar:1".into();
        shard_table.add_source(&index_uid_1, &source_id_0);

        shard_table.delete_index("test-index-foo");
        assert_eq!(shard_table.table_entries.len(), 1);

        assert!(shard_table.table_entries.contains_key(&SourceUid {
            index_uid: index_uid_1,
            source_id: source_id_0
        }));
    }

    #[test]
    fn test_shard_table_add_source() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();
        shard_table.add_source(&index_uid, &source_id);
        assert_eq!(shard_table.table_entries.len(), 1);

        let source_uid = SourceUid {
            index_uid,
            source_id,
        };
        let table_entry = shard_table.table_entries.get(&source_uid).unwrap();
        assert!(table_entry.shard_entries.is_empty());
        assert_eq!(table_entry.next_shard_id, 1);
    }

    #[test]
    fn test_shard_table_list_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut shard_table = ShardTable::default();

        assert!(shard_table.list_shards(&source_uid).is_none());

        shard_table.add_source(&index_uid, &source_id);
        let shards = shard_table.list_shards(&source_uid).unwrap();
        assert_eq!(shards.count(), 0);

        let shard_01 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: 1,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        shard_table.insert_newly_opened_shards(&index_uid, &source_id, vec![shard_01], 2);

        let shards = shard_table.list_shards(&source_uid).unwrap();
        assert_eq!(shards.count(), 1);
    }

    #[test]
    fn test_shard_table_insert_newly_opened_shards() {
        let index_uid_0: IndexUid = "test-index:0".into();
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
        shard_table.insert_newly_opened_shards(&index_uid_0, &source_id, vec![shard_01.clone()], 2);

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

        shard_table
            .table_entries
            .get_mut(&source_uid)
            .unwrap()
            .shard_entries
            .get_mut(&1)
            .unwrap()
            .set_shard_state(ShardState::Unavailable);

        let shard_02 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: 2,
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };

        shard_table.insert_newly_opened_shards(
            &index_uid_0,
            &source_id,
            vec![shard_01.clone(), shard_02.clone()],
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
        assert_eq!(shards[0].shard_state(), ShardState::Unavailable);
        assert_eq!(shards[1], shard_02);
        assert_eq!(table_entry.next_shard_id, 3);
    }

    #[test]
    fn test_shard_table_find_open_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();
        shard_table.add_source(&index_uid, &source_id);

        let mut unavailable_ingesters = FnvHashSet::default();

        let (open_shards, next_shard_id) = shard_table
            .find_open_shards_sorted(&index_uid, &source_id, &unavailable_ingesters)
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
            shard_state: ShardState::Unavailable as i32,
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
        shard_table.insert_newly_opened_shards(
            &index_uid,
            &source_id,
            vec![shard_01, shard_02, shard_03.clone(), shard_04.clone()],
            5,
        );
        let (open_shards, next_shard_id) = shard_table
            .find_open_shards_sorted(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 2);
        assert_eq!(open_shards[0].shard, shard_03);
        assert_eq!(open_shards[1].shard, shard_04);
        assert_eq!(next_shard_id, 5);

        unavailable_ingesters.insert("test-leader-0".into());

        let (open_shards, next_shard_id) = shard_table
            .find_open_shards_sorted(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 1);
        assert_eq!(open_shards[0].shard, shard_04);
        assert_eq!(next_shard_id, 5);
    }

    #[test]
    fn test_shard_table_update_shards() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let shard_01 = Shard {
            shard_id: 1,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            shard_id: 2,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_03 = Shard {
            shard_id: 3,
            shard_state: ShardState::Unavailable as i32,
            ..Default::default()
        };
        let shard_04 = Shard {
            shard_id: 4,
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.insert_newly_opened_shards(
            &index_uid,
            &source_id,
            vec![shard_01, shard_02, shard_03, shard_04],
            5,
        );
        let source_uid = SourceUid {
            index_uid,
            source_id,
        };
        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: 1,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(1),
            },
            ShardInfo {
                shard_id: 2,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(2),
            },
            ShardInfo {
                shard_id: 3,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(3),
            },
            ShardInfo {
                shard_id: 4,
                shard_state: ShardState::Closed,
                ingestion_rate: RateMibPerSec(4),
            },
            ShardInfo {
                shard_id: 5,
                shard_state: ShardState::Open,
                ingestion_rate: RateMibPerSec(5),
            },
        ]);
        let shard_stats = shard_table.update_shards(&source_uid, &shard_infos);
        assert_eq!(shard_stats.num_open_shards, 2);
        assert_eq!(shard_stats.avg_ingestion_rate, 1.5);

        let shard_entries: Vec<ShardEntry> = shard_table
            .list_shards(&source_uid)
            .unwrap()
            .cloned()
            .sorted_by_key(|shard_entry| shard_entry.shard.shard_id)
            .collect();
        assert_eq!(shard_entries.len(), 4);

        assert_eq!(shard_entries[0].shard.shard_id, 1);
        assert_eq!(shard_entries[0].shard.shard_state(), ShardState::Open);
        assert_eq!(shard_entries[0].ingestion_rate, RateMibPerSec(1));

        assert_eq!(shard_entries[1].shard.shard_id, 2);
        assert_eq!(shard_entries[1].shard.shard_state(), ShardState::Open);
        assert_eq!(shard_entries[1].ingestion_rate, RateMibPerSec(2));

        assert_eq!(shard_entries[2].shard.shard_id, 3);
        assert_eq!(
            shard_entries[2].shard.shard_state(),
            ShardState::Unavailable
        );
        assert_eq!(shard_entries[2].ingestion_rate, RateMibPerSec(3));

        assert_eq!(shard_entries[3].shard.shard_id, 4);
        assert_eq!(shard_entries[3].shard.shard_state(), ShardState::Closed);
        assert_eq!(shard_entries[3].ingestion_rate, RateMibPerSec(4));
    }

    #[test]
    fn test_shard_table_close_shards() {
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
            shard_state: ShardState::Closed as i32,
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
        shard_table.insert_newly_opened_shards(
            &index_uid_0,
            &source_id,
            vec![shard_01, shard_02],
            3,
        );
        shard_table.insert_newly_opened_shards(&index_uid_0, &source_id, vec![shard_11], 2);

        let source_uid_0 = SourceUid {
            index_uid: index_uid_0,
            source_id,
        };
        let closed_shard_ids = shard_table.close_shards(&source_uid_0, &[1, 2, 3]);
        assert_eq!(closed_shard_ids, &[1]);

        let table_entry = shard_table.table_entries.get(&source_uid_0).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards[0].shard_state(), ShardState::Closed);
    }

    #[test]
    fn test_shard_table_delete_shards() {
        let mut shard_table = ShardTable::default();

        let index_uid_0: IndexUid = "test-index:0".into();
        let index_uid_1: IndexUid = "test-index:1".into();
        let source_id = "test-source".to_string();

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
        shard_table.insert_newly_opened_shards(
            &index_uid_0,
            &source_id,
            vec![shard_01.clone(), shard_02],
            3,
        );
        shard_table.insert_newly_opened_shards(&index_uid_1, &source_id, vec![shard_11], 2);

        let source_uid_0 = SourceUid {
            index_uid: index_uid_0.clone(),
            source_id: source_id.clone(),
        };
        shard_table.delete_shards(&source_uid_0, &[2]);

        let source_uid_1 = SourceUid {
            index_uid: index_uid_1.clone(),
            source_id: source_id.clone(),
        };
        shard_table.delete_shards(&source_uid_1, &[1]);

        assert_eq!(shard_table.table_entries.len(), 2);

        let table_entry = shard_table.table_entries.get(&source_uid_0).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], shard_01);
        assert_eq!(table_entry.next_shard_id, 3);

        let table_entry = shard_table.table_entries.get(&source_uid_1).unwrap();
        assert!(table_entry.is_empty());
        assert_eq!(table_entry.next_shard_id, 2);
    }

    #[test]
    fn test_shard_table_acquire_scaling_up_permits() {
        let mut shard_table = ShardTable::default();

        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        assert!(shard_table
            .acquire_scaling_permits(&source_uid, ScalingMode::Up, 1)
            .is_none());

        shard_table.add_source(&index_uid, &source_id);

        let previous_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert!(shard_table
            .acquire_scaling_permits(&source_uid, ScalingMode::Up, 1)
            .unwrap());

        let new_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert_eq!(new_available_permits, previous_available_permits - 1);
    }

    #[test]
    fn test_shard_table_acquire_scaling_down_permits() {
        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        assert!(shard_table
            .acquire_scaling_permits(&source_uid, ScalingMode::Down, 1)
            .is_none());

        shard_table.add_source(&index_uid, &source_id);

        let previous_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_down_rate_limiter
            .available_permits();

        assert!(shard_table
            .acquire_scaling_permits(&source_uid, ScalingMode::Down, 1)
            .unwrap());

        let new_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_down_rate_limiter
            .available_permits();

        assert_eq!(new_available_permits, previous_available_permits - 1);
    }

    #[test]
    fn test_shard_table_release_scaling_up_permits() {
        let mut shard_table = ShardTable::default();

        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        shard_table.add_source(&index_uid, &source_id);

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let previous_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert!(shard_table
            .acquire_scaling_permits(&source_uid, ScalingMode::Up, 1)
            .unwrap());

        shard_table.release_scaling_permits(&source_uid, ScalingMode::Up, 1);

        let new_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert_eq!(new_available_permits, previous_available_permits);
    }

    #[test]
    fn test_shard_table_release_scaling_down_permits() {
        let mut shard_table = ShardTable::default();

        let index_uid: IndexUid = "test-index:0".into();
        let source_id = "test-source".to_string();

        shard_table.add_source(&index_uid, &source_id);

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let previous_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert!(shard_table
            .acquire_scaling_permits(&source_uid, ScalingMode::Down, 1)
            .unwrap());

        shard_table.release_scaling_permits(&source_uid, ScalingMode::Down, 1);

        let new_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert_eq!(new_available_permits, previous_available_permits);
    }
}
