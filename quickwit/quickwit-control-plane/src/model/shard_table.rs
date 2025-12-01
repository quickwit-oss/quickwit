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
use std::collections::{BTreeSet, HashMap};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use fnv::{FnvHashMap, FnvHashSet};
use quickwit_common::metrics::index_label;
use quickwit_common::rate_limiter::{RateLimiter, RateLimiterSettings};
use quickwit_common::tower::ConstantRate;
use quickwit_ingest::{RateMibPerSec, ShardInfo, ShardInfos};
use quickwit_proto::ingest::{Shard, ShardState};
use quickwit_proto::types::{IndexUid, NodeId, ShardId, SourceId, SourceUid};
use tracing::{error, info, warn};

/// Limits the number of scale up operations that can happen to a source to 5 per minute.
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ScalingMode {
    /// Scale up by adding this number of shards
    Up(usize),
    /// Scale down by removing one shard
    Down,
}

#[derive(Debug, Clone)]
pub(crate) struct ShardEntry {
    pub shard: Shard,
    pub short_term_ingestion_rate: RateMibPerSec,
    pub long_term_ingestion_rate: RateMibPerSec,
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
            short_term_ingestion_rate: RateMibPerSec::default(),
            long_term_ingestion_rate: RateMibPerSec::default(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ShardTableEntry {
    shard_entries: FnvHashMap<ShardId, ShardEntry>,
    scaling_up_rate_limiter: RateLimiter,
    scaling_down_rate_limiter: RateLimiter,
}

impl Default for ShardTableEntry {
    fn default() -> Self {
        Self {
            shard_entries: Default::default(),
            scaling_up_rate_limiter: RateLimiter::from_settings(SCALING_UP_RATE_LIMITER_SETTINGS),
            scaling_down_rate_limiter: RateLimiter::from_settings(
                SCALING_DOWN_RATE_LIMITER_SETTINGS,
            ),
        }
    }
}

impl ShardTableEntry {
    fn is_empty(&self) -> bool {
        self.shard_entries.is_empty()
    }

    fn shards_stats(&self) -> ShardStats {
        let mut num_open_shards = 0;
        let mut num_closed_shards = 0;
        let mut short_term_ingestion_rate_sum = 0;
        let mut long_term_ingestion_rate_sum = 0;

        for shard_entry in self.shard_entries.values() {
            if shard_entry.is_open() {
                num_open_shards += 1;
                short_term_ingestion_rate_sum += shard_entry.short_term_ingestion_rate.0 as usize;
                long_term_ingestion_rate_sum += shard_entry.long_term_ingestion_rate.0 as usize;
            } else if shard_entry.is_closed() {
                num_closed_shards += 1;
            }
        }
        let avg_short_term_ingestion_rate = if num_open_shards > 0 {
            short_term_ingestion_rate_sum as f32 / num_open_shards as f32
        } else {
            0.0
        };
        let avg_long_term_ingestion_rate = if num_open_shards > 0 {
            long_term_ingestion_rate_sum as f32 / num_open_shards as f32
        } else {
            0.0
        };
        ShardStats {
            num_open_shards,
            num_closed_shards,
            avg_short_term_ingestion_rate,
            avg_long_term_ingestion_rate,
        }
    }
}

#[derive(Default)]
pub struct ShardLocations<'a> {
    shard_locations: HashMap<&'a ShardId, smallvec::SmallVec<[&'a NodeId; 2]>>,
}

impl<'a> ShardLocations<'a> {
    pub(crate) fn add_location(&mut self, shard_id: &'a ShardId, ingester_id: &'a NodeId) {
        let locations = self.shard_locations.entry(shard_id).or_default();
        if locations.contains(&ingester_id) {
            warn!("shard {shard_id:?} was registered twice the same ingester {ingester_id:?}");
        } else {
            locations.push(ingester_id);
        }
    }

    /// Returns the list of indexer holding the given shard.
    /// No guarantee is made on the order of the returned list.
    pub fn get_shard_locations(&self, shard_id: &ShardId) -> &[&'a NodeId] {
        let Some(node_ids) = self.shard_locations.get(shard_id) else {
            return &[];
        };
        node_ids.as_slice()
    }
}

// A table that keeps track of the existing shards for each index and source,
// and for each ingester, the list of shards it is supposed to host.
//
// (All mutable methods must maintain these two invariants.)
#[derive(Debug, Default)]
pub(crate) struct ShardTable {
    table_entries: FnvHashMap<SourceUid, ShardTableEntry>,
    ingester_shards: FnvHashMap<NodeId, FnvHashMap<SourceUid, BTreeSet<ShardId>>>,
}

// Removes the shards from the ingester_shards map.
//
// This function is used to maintain the shard table invariant.
// Logs an error if the shard was not found in the ingester_shards map.
fn remove_shard_from_ingesters_internal(
    source_uid: &SourceUid,
    shard: &Shard,
    ingester_shards: &mut FnvHashMap<NodeId, FnvHashMap<SourceUid, BTreeSet<ShardId>>>,
) {
    for node in shard.ingesters() {
        let ingester_shards = ingester_shards
            .get_mut(node)
            .expect("shard table reached inconsistent state");
        let shard_ids = ingester_shards.get_mut(source_uid).unwrap();
        let shard_was_removed = shard_ids.remove(shard.shard_id());
        if !shard_was_removed {
            error!(
                "shard table has reached an inconsistent state. shard {shard:?} was removed from \
                 the shard table but was apparently not in the ingester_shards map."
            );
        }
    }
}

impl ShardTable {
    /// Returns a ShardLocations object that maps each shard to the list of ingesters hosting it.
    /// All shards are considered regardless of their state (including unavailable).
    pub fn shard_locations(&self) -> ShardLocations<'_> {
        let mut shard_locations = ShardLocations::default();
        for (ingester_id, source_shards) in &self.ingester_shards {
            for shard_ids in source_shards.values() {
                for shard_id in shard_ids {
                    shard_locations.add_location(shard_id, ingester_id);
                }
            }
        }
        shard_locations
    }

    /// Removes all the entries that match the target index ID.
    pub fn delete_index(&mut self, index_id: &str) {
        let shards_removed = self
            .table_entries
            .iter()
            .filter(|(source_uid, _)| source_uid.index_uid.index_id == index_id)
            .flat_map(|(source_uid, shard_table_entry)| {
                shard_table_entry
                    .shard_entries
                    .values()
                    .map(move |shard_entry: &ShardEntry| (source_uid, &shard_entry.shard))
            });
        for (source_uid, shard) in shards_removed {
            remove_shard_from_ingesters_internal(source_uid, shard, &mut self.ingester_shards);
        }
        self.table_entries
            .retain(|source_uid, _| source_uid.index_uid.index_id != index_id);
        self.check_invariant();
    }

    /// Checks whether the shard table is consistent.
    ///
    /// Panics if it is not.
    #[allow(clippy::mutable_key_type)]
    fn check_invariant(&self) {
        // This function is expensive! Let's not call it in release mode.
        if !cfg!(debug_assertions) {
            return;
        };
        let mut shard_sets_in_shard_table = FnvHashSet::default();
        for (source_uid, shard_table_entry) in &self.table_entries {
            for (shard_id, shard_entry) in &shard_table_entry.shard_entries {
                debug_assert_eq!(shard_id, shard_entry.shard.shard_id());
                debug_assert_eq!(&source_uid.index_uid, shard_entry.shard.index_uid());
                for node in shard_entry.shard.ingesters() {
                    shard_sets_in_shard_table.insert((node, source_uid, shard_id));
                }
            }
        }
        for (node, ingester_shards) in &self.ingester_shards {
            for (source_uid, shard_ids) in ingester_shards {
                for shard_id in shard_ids {
                    let shard_table_entry = self.table_entries.get(source_uid).unwrap();
                    debug_assert!(shard_table_entry.shard_entries.contains_key(shard_id));
                    debug_assert!(shard_sets_in_shard_table.remove(&(node, source_uid, shard_id)));
                }
            }
        }
    }

    /// Lists all the shards hosted on a given node, regardless of whether it is a
    /// leader or a follower.
    pub fn list_shards_for_node(
        &self,
        ingester: &NodeId,
    ) -> Option<&FnvHashMap<SourceUid, BTreeSet<ShardId>>> {
        self.ingester_shards.get(ingester)
    }

    pub fn list_shards_for_index<'a>(
        &'a self,
        index_uid: &'a IndexUid,
    ) -> impl Iterator<Item = &'a ShardEntry> + 'a {
        self.table_entries
            .iter()
            .filter(move |(source_uid, _)| source_uid.index_uid == *index_uid)
            .flat_map(|(_, shard_table_entry)| shard_table_entry.shard_entries.values())
    }

    pub fn num_sources(&self) -> usize {
        self.table_entries.len()
    }

    #[cfg(test)]
    pub fn num_shards(&self) -> usize {
        self.table_entries
            .values()
            .map(|shard_table_entry| shard_table_entry.shard_entries.len())
            .sum()
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
        if let Some(previous_table_entry) = previous_table_entry_opt
            && !previous_table_entry.is_empty()
        {
            error!(
                "shard table entry for index `{}` and source `{}` already exists",
                index_uid.index_id, source_id
            );
        }
        self.check_invariant();
    }

    pub fn delete_source(&mut self, index_uid: &IndexUid, source_id: &SourceId) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let Some(shard_table_entry) = self.table_entries.remove(&source_uid) else {
            return;
        };
        for shard_entry in shard_table_entry.shard_entries.values() {
            remove_shard_from_ingesters_internal(
                &source_uid,
                &shard_entry.shard,
                &mut self.ingester_shards,
            );
        }
        self.check_invariant();
    }

    pub(crate) fn all_shards(&self) -> impl Iterator<Item = &ShardEntry> + '_ {
        self.table_entries
            .values()
            .flat_map(|table_entry| table_entry.shard_entries.values())
    }

    pub(crate) fn all_shards_with_source(
        &self,
    ) -> impl Iterator<Item = (&SourceUid, impl Iterator<Item = &ShardEntry>)> + '_ {
        self.table_entries
            .iter()
            .map(|(source, shard_table)| (source, shard_table.shard_entries.values()))
    }

    /// Lists the shards of a given source. Returns `None` if the source does not exist.
    pub fn get_shards(&self, source_uid: &SourceUid) -> Option<&FnvHashMap<ShardId, ShardEntry>> {
        self.table_entries
            .get(source_uid)
            .map(|table_entry| &table_entry.shard_entries)
    }

    /// Lists the shards of a given source. Returns `None` if the source does not exist.
    pub fn get_shards_mut(
        &mut self,
        source_uid: &SourceUid,
    ) -> Option<&mut FnvHashMap<ShardId, ShardEntry>> {
        self.table_entries
            .get_mut(source_uid)
            .map(|table_entry| &mut table_entry.shard_entries)
    }

    /// Inserts the shards into the shard table.
    pub fn insert_shards(
        &mut self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        opened_shards: Vec<Shard>,
    ) {
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        for shard in &opened_shards {
            if shard.index_uid() != &source_uid.index_uid || shard.source_id != source_uid.source_id
            {
                panic!(
                    "shard source UID `{}/{}` does not match source UID `{source_uid}`",
                    shard.index_uid(),
                    shard.source_id,
                );
            }
        }
        for shard in &opened_shards {
            for node in shard.ingesters() {
                let ingester_shards = self.ingester_shards.entry(node.to_owned()).or_default();
                let shard_ids = ingester_shards.entry(source_uid.clone()).or_default();
                shard_ids.insert(shard.shard_id().clone());
            }
        }
        match self.table_entries.entry(source_uid.clone()) {
            Entry::Occupied(mut entry) => {
                let table_entry = entry.get_mut();
                for opened_shard in opened_shards {
                    // We only insert shards that we don't know about because the control plane
                    // knows more about the state of the shards than the metastore.
                    table_entry
                        .shard_entries
                        .entry(opened_shard.shard_id().clone())
                        .or_insert_with(|| ShardEntry::from(opened_shard));
                }
            }
            // This should never happen if the control plane view is consistent with the state of
            // the metastore, so should we panic here? Warnings are most likely going to go
            // unnoticed.
            Entry::Vacant(entry) => {
                warn!(
                    "control plane inconsistent with metastore: inserting shards for a \
                     non-existing source (please report)"
                );
                let shard_entries: FnvHashMap<ShardId, ShardEntry> = opened_shards
                    .into_iter()
                    .map(|shard| (shard.shard_id().clone(), shard.into()))
                    .collect();
                let table_entry = ShardTableEntry {
                    shard_entries,
                    ..Default::default()
                };
                entry.insert(table_entry);
            }
        }
        // Let's now update the open shard metrics for this specific index.
        self.update_shard_metrics_for_source_uid(&source_uid);
        self.check_invariant();
    }

    /// Finds open shards for a given index and source and whose leaders are not in the set of
    /// unavailable ingesters.
    pub fn find_open_shards(
        &self,
        index_uid: &IndexUid,
        source_id: &SourceId,
        unavailable_leaders: &FnvHashSet<NodeId>,
    ) -> Option<Vec<ShardEntry>> {
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
        Some(open_shards)
    }

    pub fn update_shard_metrics_for_source_uid(&self, source_uid: &SourceUid) {
        let Some(table_entry) = self.table_entries.get(source_uid) else {
            return;
        };
        let index_id = source_uid.index_uid.index_id.as_str();
        let index_label = index_label(index_id);

        // If `index_label(index_id)` returns `index_id`, then per-index metrics are enabled and we
        // can update the metrics for this specific index.
        if index_label == index_id {
            let shard_stats = table_entry.shards_stats();
            crate::metrics::CONTROL_PLANE_METRICS
                .open_shards
                .with_label_values([index_label])
                .set(shard_stats.num_open_shards as i64);
            crate::metrics::CONTROL_PLANE_METRICS
                .closed_shards
                .with_label_values([index_label])
                .set(shard_stats.num_closed_shards as i64);
            return;
        }
        // Per-index metrics are disabled, so we update the metrics for all sources.
        let mut num_open_shards = 0;
        let mut num_closed_shards = 0;

        for shard_entry in self.all_shards() {
            if shard_entry.is_open() {
                num_open_shards += 1;
            } else if shard_entry.is_closed() {
                num_closed_shards += 1;
            }
        }
        crate::metrics::CONTROL_PLANE_METRICS
            .open_shards
            .with_label_values([index_label])
            .set(num_open_shards as i64);
        crate::metrics::CONTROL_PLANE_METRICS
            .closed_shards
            .with_label_values([index_label])
            .set(num_closed_shards as i64);
    }

    pub fn update_shards(
        &mut self,
        source_uid: &SourceUid,
        shard_infos: &ShardInfos,
    ) -> ShardStats {
        let Some(table_entry) = self.table_entries.get_mut(source_uid) else {
            return ShardStats::default();
        };
        for shard_info in shard_infos {
            let ShardInfo {
                shard_id,
                shard_state,
                short_term_ingestion_rate,
                long_term_ingestion_rate,
            } = shard_info;

            if let Some(shard_entry) = table_entry.shard_entries.get_mut(shard_id) {
                shard_entry.short_term_ingestion_rate = *short_term_ingestion_rate;
                shard_entry.long_term_ingestion_rate = *long_term_ingestion_rate;
                // `ShardInfos` are broadcasted via Chitchat and eventually consistent. As a
                // result, we can only trust the `Closed` state, which is final.
                if shard_state.is_closed() {
                    shard_entry.set_shard_state(ShardState::Closed);
                }
            }
        }
        table_entry.shards_stats()
    }

    /// Sets the state of the shards identified by their index UID, source ID, and shard IDs to
    /// `Closed`.
    pub fn close_shards(&mut self, source_uid: &SourceUid, shard_ids: &[ShardId]) -> Vec<ShardId> {
        let Some(table_entry) = self.table_entries.get_mut(source_uid) else {
            return Vec::new();
        };
        let mut closed_shard_ids = Vec::new();

        for shard_id in shard_ids {
            if let Some(shard_entry) = table_entry.shard_entries.get_mut(shard_id) {
                if !shard_entry.is_closed() {
                    shard_entry.set_shard_state(ShardState::Closed);
                    closed_shard_ids.push(shard_id.clone());
                }
            } else {
                info!(
                    index_id=%source_uid.index_uid.index_id,
                    source_id=%source_uid.source_id,
                    %shard_id,
                    "ignoring attempt to close shard: it is unknown (probably because it has been deleted)"
                );
            }
        }
        self.update_shard_metrics_for_source_uid(source_uid);
        closed_shard_ids
    }

    /// Removes the shards identified by their index UID, source ID, and shard IDs.
    pub fn delete_shards(&mut self, source_uid: &SourceUid, shard_ids: &[ShardId]) {
        let Some(table_entry) = self.table_entries.get_mut(source_uid) else {
            return;
        };
        let mut shard_entries_to_remove: Vec<ShardEntry> = Vec::new();
        for shard_id in shard_ids {
            if let Some(shard_entry) = table_entry.shard_entries.remove(shard_id) {
                shard_entries_to_remove.push(shard_entry);
            } else {
                warn!(shard=%shard_id, "deleting a non-existing shard");
            }
        }
        for shard_entry in shard_entries_to_remove {
            remove_shard_from_ingesters_internal(
                source_uid,
                &shard_entry.shard,
                &mut self.ingester_shards,
            );
        }
        self.check_invariant();
        self.update_shard_metrics_for_source_uid(source_uid);
    }

    pub fn acquire_scaling_permits(
        &mut self,
        source_uid: &SourceUid,
        scaling_mode: ScalingMode,
    ) -> Option<bool> {
        let table_entry = self.table_entries.get_mut(source_uid)?;
        let scaling_rate_limiter = match scaling_mode {
            ScalingMode::Up(_) => &mut table_entry.scaling_up_rate_limiter,
            ScalingMode::Down => &mut table_entry.scaling_down_rate_limiter,
        };
        Some(scaling_rate_limiter.acquire(1))
    }

    pub fn drain_scaling_permits(&mut self, source_uid: &SourceUid, scaling_mode: ScalingMode) {
        if let Some(table_entry) = self.table_entries.get_mut(source_uid) {
            let scaling_rate_limiter = match scaling_mode {
                ScalingMode::Up(_) => &mut table_entry.scaling_up_rate_limiter,
                ScalingMode::Down => &mut table_entry.scaling_down_rate_limiter,
            };
            scaling_rate_limiter.drain();
        }
    }

    pub fn release_scaling_permits(&mut self, source_uid: &SourceUid, scaling_mode: ScalingMode) {
        if let Some(table_entry) = self.table_entries.get_mut(source_uid) {
            let scaling_rate_limiter = match scaling_mode {
                ScalingMode::Up(_) => &mut table_entry.scaling_up_rate_limiter,
                ScalingMode::Down => &mut table_entry.scaling_down_rate_limiter,
            };
            scaling_rate_limiter.release(1);
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ShardStats {
    pub num_open_shards: usize,
    pub num_closed_shards: usize,
    /// Average short-term ingestion rate (MiB/s) over all open shards.
    pub avg_short_term_ingestion_rate: f32,
    /// Average long-term ingestion rate (MiB/s) over all open shards.
    pub avg_long_term_ingestion_rate: f32,
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
                .sorted_unstable_by(|left, right| left.shard_id.cmp(&right.shard_id))
                .collect()
        }
    }

    impl ShardTable {
        pub fn find_open_shards_sorted(
            &self,
            index_uid: &IndexUid,
            source_id: &SourceId,
            unavailable_leaders: &FnvHashSet<NodeId>,
        ) -> Option<Vec<ShardEntry>> {
            self.find_open_shards(index_uid, source_id, unavailable_leaders)
                .map(|mut shards| {
                    shards.sort_unstable_by(|left, right| {
                        left.shard.shard_id.cmp(&right.shard.shard_id)
                    });
                    shards
                })
        }
    }

    #[test]
    fn test_shard_table_delete_index() {
        let mut shard_table = ShardTable::default();
        shard_table.delete_index("test-index");

        let index_uid_0: IndexUid = IndexUid::for_test("test-index-foo", 0);
        let source_id_0 = "test-source-0".to_string();
        shard_table.add_source(&index_uid_0, &source_id_0);

        let source_id_1 = "test-source-1".to_string();
        shard_table.add_source(&index_uid_0, &source_id_1);

        let index_uid_1: IndexUid = IndexUid::for_test("test-index-bar", 1);
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
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
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
    }

    #[test]
    fn test_shard_table_list_shards() {
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        let mut shard_table = ShardTable::default();

        assert!(shard_table.get_shards(&source_uid).is_none());

        shard_table.add_source(&index_uid, &source_id);
        let shards = shard_table.get_shards(&source_uid).unwrap();
        assert_eq!(shards.len(), 0);

        let shard_01 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        shard_table.insert_shards(&index_uid, &source_id, vec![shard_01]);

        let shards = shard_table.get_shards(&source_uid).unwrap();
        assert_eq!(shards.len(), 1);
    }

    #[test]
    fn test_shard_table_insert_newly_opened_shards() {
        let index_uid_0: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let shard_01 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.insert_shards(&index_uid_0, &source_id, vec![shard_01.clone()]);

        assert_eq!(shard_table.table_entries.len(), 1);

        let source_uid = SourceUid {
            index_uid: index_uid_0.clone(),
            source_id: source_id.clone(),
        };
        let table_entry = shard_table.table_entries.get(&source_uid).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], shard_01);

        shard_table
            .table_entries
            .get_mut(&source_uid)
            .unwrap()
            .shard_entries
            .get_mut(&ShardId::from(1))
            .unwrap()
            .set_shard_state(ShardState::Unavailable);

        let shard_02 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };

        shard_table.insert_shards(
            &index_uid_0,
            &source_id,
            vec![shard_01.clone(), shard_02.clone()],
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
    }

    #[test]
    fn test_shard_table_find_open_shards() {
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();
        shard_table.add_source(&index_uid, &source_id);

        let mut unavailable_ingesters = FnvHashSet::default();

        let open_shards = shard_table
            .find_open_shards_sorted(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 0);

        let shard_01 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Unavailable as i32,
            ..Default::default()
        };
        let shard_03 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_04 = Shard {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(4)),
            leader_id: "test-leader-1".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.insert_shards(
            &index_uid,
            &source_id,
            vec![shard_01, shard_02, shard_03.clone(), shard_04.clone()],
        );
        let open_shards = shard_table
            .find_open_shards_sorted(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 2);
        assert_eq!(open_shards[0].shard, shard_03);
        assert_eq!(open_shards[1].shard, shard_04);

        unavailable_ingesters.insert("test-leader-0".into());

        let open_shards = shard_table
            .find_open_shards_sorted(&index_uid, &source_id, &unavailable_ingesters)
            .unwrap();
        assert_eq!(open_shards.len(), 1);
        assert_eq!(open_shards[0].shard, shard_04);
    }

    #[test]
    fn test_shard_table_update_shards() {
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let shard_01 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_03 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(3)),
            shard_state: ShardState::Unavailable as i32,
            ..Default::default()
        };
        let shard_04 = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(4)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.insert_shards(
            &index_uid,
            &source_id,
            vec![shard_01, shard_02, shard_03, shard_04],
        );
        let source_uid = SourceUid {
            index_uid,
            source_id,
        };
        let shard_infos = BTreeSet::from_iter([
            ShardInfo {
                shard_id: ShardId::from(1),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(1),
                long_term_ingestion_rate: RateMibPerSec(1),
            },
            ShardInfo {
                shard_id: ShardId::from(2),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(2),
                long_term_ingestion_rate: RateMibPerSec(2),
            },
            ShardInfo {
                shard_id: ShardId::from(3),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(3),
                long_term_ingestion_rate: RateMibPerSec(3),
            },
            ShardInfo {
                shard_id: ShardId::from(4),
                shard_state: ShardState::Closed,
                short_term_ingestion_rate: RateMibPerSec(4),
                long_term_ingestion_rate: RateMibPerSec(4),
            },
            ShardInfo {
                shard_id: ShardId::from(5),
                shard_state: ShardState::Open,
                short_term_ingestion_rate: RateMibPerSec(5),
                long_term_ingestion_rate: RateMibPerSec(5),
            },
        ]);
        let shard_stats = shard_table.update_shards(&source_uid, &shard_infos);
        assert_eq!(shard_stats.num_open_shards, 2);
        assert_eq!(shard_stats.avg_short_term_ingestion_rate, 1.5);

        assert_eq!(shard_stats.avg_short_term_ingestion_rate, 1.5);

        let shard_entries: Vec<ShardEntry> = shard_table
            .get_shards(&source_uid)
            .unwrap()
            .values()
            .cloned()
            .sorted_unstable_by(|left, right| left.shard.shard_id.cmp(&right.shard.shard_id))
            .collect();
        assert_eq!(shard_entries.len(), 4);

        assert_eq!(shard_entries[0].shard.shard_id(), ShardId::from(1));
        assert_eq!(shard_entries[0].shard.shard_state(), ShardState::Open);
        assert_eq!(shard_entries[0].short_term_ingestion_rate, RateMibPerSec(1));

        assert_eq!(shard_entries[1].shard.shard_id(), ShardId::from(2));
        assert_eq!(shard_entries[1].shard.shard_state(), ShardState::Open);
        assert_eq!(shard_entries[1].short_term_ingestion_rate, RateMibPerSec(2));

        assert_eq!(shard_entries[2].shard.shard_id(), ShardId::from(3));
        assert_eq!(
            shard_entries[2].shard.shard_state(),
            ShardState::Unavailable
        );
        assert_eq!(shard_entries[2].short_term_ingestion_rate, RateMibPerSec(3));

        assert_eq!(shard_entries[3].shard.shard_id(), ShardId::from(4));
        assert_eq!(shard_entries[3].shard.shard_state(), ShardState::Closed);
        assert_eq!(shard_entries[3].short_term_ingestion_rate, RateMibPerSec(4));
    }

    #[test]
    fn test_shard_table_close_shards() {
        let index_uid_0: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid_1: IndexUid = IndexUid::for_test("test-index", 1);
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let shard_01 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Closed as i32,
            ..Default::default()
        };
        let shard_11 = Shard {
            index_uid: index_uid_1.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.insert_shards(&index_uid_0, &source_id, vec![shard_01, shard_02]);
        shard_table.insert_shards(&index_uid_1, &source_id, vec![shard_11]);

        let source_uid_0 = SourceUid {
            index_uid: index_uid_0,
            source_id,
        };
        let closed_shard_ids = shard_table.close_shards(
            &source_uid_0,
            &[ShardId::from(1), ShardId::from(2), ShardId::from(3)],
        );
        assert_eq!(closed_shard_ids, &[ShardId::from(1)]);

        let table_entry = shard_table.table_entries.get(&source_uid_0).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards[0].shard_state(), ShardState::Closed);
    }

    #[test]
    fn test_shard_table_delete_shards() {
        let mut shard_table = ShardTable::default();

        let index_uid_0: IndexUid = IndexUid::for_test("test-index", 0);
        let index_uid_1: IndexUid = IndexUid::for_test("test-index", 1);
        let source_id = "test-source".to_string();

        let shard_01 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_02 = Shard {
            index_uid: index_uid_0.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(2)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        let shard_11 = Shard {
            index_uid: index_uid_1.clone().into(),
            source_id: source_id.clone(),
            shard_id: Some(ShardId::from(1)),
            leader_id: "test-leader-0".to_string(),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        shard_table.insert_shards(&index_uid_0, &source_id, vec![shard_01.clone(), shard_02]);
        shard_table.insert_shards(&index_uid_1, &source_id, vec![shard_11]);

        let source_uid_0 = SourceUid {
            index_uid: index_uid_0.clone(),
            source_id: source_id.clone(),
        };
        shard_table.delete_shards(&source_uid_0, &[ShardId::from(2)]);

        let source_uid_1 = SourceUid {
            index_uid: index_uid_1.clone(),
            source_id: source_id.clone(),
        };
        shard_table.delete_shards(&source_uid_1, &[ShardId::from(1)]);

        assert_eq!(shard_table.table_entries.len(), 2);

        let table_entry = shard_table.table_entries.get(&source_uid_0).unwrap();
        let shards = table_entry.shards();
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0], shard_01);

        let table_entry = shard_table.table_entries.get(&source_uid_1).unwrap();
        assert!(table_entry.is_empty());
    }

    #[test]
    fn test_shard_table_acquire_scaling_up_permits() {
        let mut shard_table = ShardTable::default();

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        assert!(
            shard_table
                .acquire_scaling_permits(&source_uid, ScalingMode::Up(1))
                .is_none()
        );

        shard_table.add_source(&index_uid, &source_id);

        let previous_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert!(
            shard_table
                .acquire_scaling_permits(&source_uid, ScalingMode::Up(1))
                .unwrap()
        );

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
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id = "test-source".to_string();

        let mut shard_table = ShardTable::default();

        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.clone(),
        };
        assert!(
            shard_table
                .acquire_scaling_permits(&source_uid, ScalingMode::Down)
                .is_none()
        );

        shard_table.add_source(&index_uid, &source_id);

        let previous_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_down_rate_limiter
            .available_permits();

        assert!(
            shard_table
                .acquire_scaling_permits(&source_uid, ScalingMode::Down)
                .unwrap()
        );

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

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
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

        assert!(
            shard_table
                .acquire_scaling_permits(&source_uid, ScalingMode::Up(1))
                .unwrap()
        );

        shard_table.release_scaling_permits(&source_uid, ScalingMode::Up(1));

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

        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
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

        assert!(
            shard_table
                .acquire_scaling_permits(&source_uid, ScalingMode::Down)
                .unwrap()
        );

        shard_table.release_scaling_permits(&source_uid, ScalingMode::Down);

        let new_available_permits = shard_table
            .table_entries
            .get(&source_uid)
            .unwrap()
            .scaling_up_rate_limiter
            .available_permits();

        assert_eq!(new_available_permits, previous_available_permits);
    }

    #[test]
    fn test_shard_locations() {
        let shard1 = ShardId::from("shard1");
        let shard2 = ShardId::from("shard1");
        let unlisted_shard = ShardId::from("unlisted");
        let node1 = NodeId::new("node1".to_string());
        let node2 = NodeId::new("node2".to_string());
        let mut shard_locations = ShardLocations::default();
        shard_locations.add_location(&shard1, &node1);
        shard_locations.add_location(&shard1, &node2);
        // add location called several times should counted once.
        shard_locations.add_location(&shard2, &node2);
        assert_eq!(
            shard_locations.get_shard_locations(&shard1),
            &[&node1, &node2]
        );
        assert_eq!(
            shard_locations.get_shard_locations(&shard2),
            &[&node1, &node2]
        );
        // If the shard is not listed, we do not panic but just return an empty list.
        assert!(
            shard_locations
                .get_shard_locations(&unlisted_shard)
                .is_empty()
        );
    }

    #[test]
    fn test_shard_table_shard_locations() {
        let mut shard_table = ShardTable::default();

        let index_uid0: IndexUid = IndexUid::for_test("test-index0", 0);
        let source_id = "test-source0".to_string();
        shard_table.add_source(&index_uid0, &source_id);

        let index_uid1: IndexUid = IndexUid::for_test("test-index1", 0);
        let source_id = "test-source1".to_string();
        shard_table.add_source(&index_uid1, &source_id);

        let source_uid0 = SourceUid {
            index_uid: index_uid0.clone(),
            source_id: source_id.clone(),
        };

        let source_uid1 = SourceUid {
            index_uid: index_uid1.clone(),
            source_id: source_id.clone(),
        };

        let make_shard = |source_uid: &SourceUid,
                          leader_id: &str,
                          shard_id: u64,
                          follower_id: Option<&str>,
                          shard_state: ShardState| {
            Shard {
                index_uid: source_uid.index_uid.clone().into(),
                source_id: source_uid.source_id.clone(),
                shard_id: Some(ShardId::from(shard_id)),
                leader_id: leader_id.to_string(),
                follower_id: follower_id.map(|s| s.to_string()),
                shard_state: shard_state as i32,
                ..Default::default()
            }
        };

        shard_table.insert_shards(
            &source_uid0.index_uid,
            &source_uid0.source_id,
            vec![
                make_shard(
                    &source_uid0,
                    "indexer1",
                    0,
                    Some("indexer2"),
                    ShardState::Open,
                ),
                make_shard(&source_uid0, "indexer1", 1, None, ShardState::Closed),
                make_shard(&source_uid0, "indexer2", 2, None, ShardState::Open),
            ],
        );

        shard_table.insert_shards(
            &source_uid1.index_uid,
            &source_uid1.source_id,
            vec![
                make_shard(
                    &source_uid1,
                    "indexer2",
                    3,
                    Some("indexer1"),
                    ShardState::Unavailable,
                ),
                make_shard(
                    &source_uid1,
                    "indexer2",
                    3,
                    Some("indexer1"),
                    ShardState::Open,
                ),
            ],
        );

        let shard_locations = shard_table.shard_locations();
        let get_sorted_locations_for_shard = |shard_id: u64| {
            let mut locations = shard_locations
                .get_shard_locations(&ShardId::from(shard_id))
                .to_vec();
            locations.sort();
            locations
        };
        assert_eq!(
            &get_sorted_locations_for_shard(0u64),
            &[&NodeId::from("indexer1"), &NodeId::from("indexer2")]
        );
        assert_eq!(
            &get_sorted_locations_for_shard(1u64),
            &[&NodeId::from("indexer1")]
        );
        assert_eq!(
            &get_sorted_locations_for_shard(2u64),
            &[&NodeId::from("indexer2")]
        );
        assert_eq!(
            &get_sorted_locations_for_shard(3u64),
            &[&NodeId::from("indexer1"), &NodeId::from("indexer2")]
        );
    }
}
