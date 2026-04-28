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

//! Stateright model for Parquet Data Model invariants (ADR-001).
//!
//! Mirrors `docs/internals/specs/tla/ParquetDataModel.tla`.
//!
//! # Invariants
//! - DM-1: Each row is exactly one data point (all required fields populated)
//! - DM-2: No last-write-wins; duplicate (metric, tags, ts) from separate ingests both survive
//! - DM-3: No interpolation; storage contains only ingested points
//! - DM-4: timeseries_id is deterministic for a given tag set
//! - DM-5: timeseries_id persists through compaction without recomputation

use std::collections::BTreeSet;

use stateright::*;

/// Node identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Node(pub u8);

/// Metric name.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MetricName(pub u8);

/// Tag set (opaque identifier; deterministic hash is identity).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagSet(pub u8);

/// Deterministic hash of a tag set to a timeseries_id.
/// Mirrors TLA+ `TSIDHash(tags) == CHOOSE n \in 0..100 : TRUE`.
/// We use the tag set's inner value as the hash (deterministic + injective).
fn tsid_hash(tags: TagSet) -> u32 {
    tags.0 as u32
}

/// A data point.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DataPoint {
    pub metric_name: MetricName,
    pub tags: TagSet,
    pub timestamp: i64,
    pub value: i32,
    pub request_id: u32,
    pub timeseries_id: u32,
}

/// A split in object storage.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DataModelSplit {
    pub split_id: u32,
    pub rows: BTreeSet<DataPoint>,
}

/// Model state. Mirrors TLA+ `VARIABLES`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DataModelState {
    /// Per-node pending batches.
    pub pending: Vec<(Node, BTreeSet<DataPoint>)>,
    /// Published splits in object storage.
    pub splits: BTreeSet<DataModelSplit>,
    /// All points ever ingested (ghost variable for DM-3).
    pub all_ingested_points: BTreeSet<DataPoint>,
    pub next_split_id: u32,
    pub next_request_id: u32,
}

impl DataModelState {
    fn pending_for(&self, node: Node) -> &BTreeSet<DataPoint> {
        for (n, set) in &self.pending {
            if *n == node {
                return set;
            }
        }
        // Should not happen in well-formed model.
        panic!("node not found in pending");
    }

    fn pending_for_mut(&mut self, node: Node) -> &mut BTreeSet<DataPoint> {
        for (n, set) in &mut self.pending {
            if *n == node {
                return set;
            }
        }
        panic!("node not found in pending");
    }

    fn all_stored_rows(&self) -> BTreeSet<DataPoint> {
        self.splits
            .iter()
            .flat_map(|s| s.rows.iter().cloned())
            .collect()
    }

    fn all_pending_rows(&self) -> BTreeSet<DataPoint> {
        self.pending
            .iter()
            .flat_map(|(_, set)| set.iter().cloned())
            .collect()
    }
}

/// Actions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum DataModelAction {
    IngestPoint {
        node: Node,
        metric_name: MetricName,
        tags: TagSet,
        timestamp: i64,
    },
    FlushSplit {
        node: Node,
    },
    CompactSplits {
        selected_ids: BTreeSet<u32>,
    },
}

/// Model configuration. Mirrors TLA+ `CONSTANTS`.
#[derive(Clone, Debug)]
pub struct DataModelModel {
    pub nodes: Vec<Node>,
    pub metric_names: Vec<MetricName>,
    pub tag_sets: Vec<TagSet>,
    pub timestamps: Vec<i64>,
    pub request_count_max: u32,
}

impl DataModelModel {
    /// Small model matching `ParquetDataModel_small.cfg`.
    pub fn small() -> Self {
        DataModelModel {
            nodes: vec![Node(1)],
            metric_names: vec![MetricName(1)],
            tag_sets: vec![TagSet(1)],
            timestamps: vec![1],
            request_count_max: 3,
        }
    }
}

/// Generate all subsets of size >= 2 from a set of split IDs.
fn subsets_ge2(ids: &[u32]) -> Vec<BTreeSet<u32>> {
    let n = ids.len();
    let mut result = Vec::new();
    for mask in 0..(1u32 << n) {
        if mask.count_ones() >= 2 {
            let mut subset = BTreeSet::new();
            for (i, &id) in ids.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    subset.insert(id);
                }
            }
            result.push(subset);
        }
    }
    result
}

impl Model for DataModelModel {
    type State = DataModelState;
    type Action = DataModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        let pending: Vec<(Node, BTreeSet<DataPoint>)> =
            self.nodes.iter().map(|&n| (n, BTreeSet::new())).collect();
        vec![DataModelState {
            pending,
            splits: BTreeSet::new(),
            all_ingested_points: BTreeSet::new(),
            next_split_id: 1,
            next_request_id: 1,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // IngestPoint
        if state.next_request_id < self.request_count_max {
            for &node in &self.nodes {
                for &mn in &self.metric_names {
                    for &tags in &self.tag_sets {
                        for &ts in &self.timestamps {
                            actions.push(DataModelAction::IngestPoint {
                                node,
                                metric_name: mn,
                                tags,
                                timestamp: ts,
                            });
                        }
                    }
                }
            }
        }

        // FlushSplit
        for &node in &self.nodes {
            if !state.pending_for(node).is_empty() {
                actions.push(DataModelAction::FlushSplit { node });
            }
        }

        // CompactSplits
        if state.splits.len() >= 2 {
            let ids: Vec<u32> = state.splits.iter().map(|s| s.split_id).collect();
            for subset in subsets_ge2(&ids) {
                actions.push(DataModelAction::CompactSplits {
                    selected_ids: subset,
                });
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();

        match action {
            DataModelAction::IngestPoint {
                node,
                metric_name,
                tags,
                timestamp,
            } => {
                let point = DataPoint {
                    metric_name,
                    tags,
                    timestamp,
                    value: 1,
                    request_id: next.next_request_id,
                    timeseries_id: tsid_hash(tags),
                };
                next.pending_for_mut(node).insert(point.clone());
                next.all_ingested_points.insert(point);
                next.next_request_id += 1;
            }
            DataModelAction::FlushSplit { node } => {
                let rows = next.pending_for(node).clone();
                if rows.is_empty() {
                    return None;
                }
                let new_split = DataModelSplit {
                    split_id: next.next_split_id,
                    rows,
                };
                next.splits.insert(new_split);
                next.next_split_id += 1;
                *next.pending_for_mut(node) = BTreeSet::new();
            }
            DataModelAction::CompactSplits { selected_ids } => {
                let selected: Vec<DataModelSplit> = next
                    .splits
                    .iter()
                    .filter(|s| selected_ids.contains(&s.split_id))
                    .cloned()
                    .collect();

                if selected.len() < 2 {
                    return None;
                }

                let merged_rows: BTreeSet<DataPoint> = selected
                    .iter()
                    .flat_map(|s| s.rows.iter().cloned())
                    .collect();
                let new_split = DataModelSplit {
                    split_id: next.next_split_id,
                    rows: merged_rows,
                };

                for s in &selected {
                    next.splits.remove(s);
                }
                next.splits.insert(new_split);
                next.next_split_id += 1;
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // DM-1: Each row is exactly one data point.
            // Every row has all required fields populated.
            // Mirrors ParquetDataModel.tla lines 174-180
            Property::always(
                "DM-1: point per row",
                |model: &DataModelModel, state: &DataModelState| {
                    for s in &state.splits {
                        for row in &s.rows {
                            if !model.metric_names.contains(&row.metric_name) {
                                return false;
                            }
                            if !model.tag_sets.contains(&row.tags) {
                                return false;
                            }
                            if !model.timestamps.contains(&row.timestamp) {
                                return false;
                            }
                            if row.timeseries_id != tsid_hash(row.tags) {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // DM-2: No last-write-wins.
            // If two points share (metric, tags, ts) with different request_id,
            // and both have been flushed, both must be in storage.
            // Mirrors ParquetDataModel.tla lines 198-208
            Property::always(
                "DM-2: no LWW",
                |_model: &DataModelModel, state: &DataModelState| {
                    let stored = state.all_stored_rows();
                    let pending = state.all_pending_rows();
                    for p1 in &state.all_ingested_points {
                        for p2 in &state.all_ingested_points {
                            if p1.metric_name == p2.metric_name
                                && p1.tags == p2.tags
                                && p1.timestamp == p2.timestamp
                                && p1.request_id != p2.request_id
                                && !pending.contains(p1)
                                && !pending.contains(p2)
                                && (!stored.contains(p1) || !stored.contains(p2))
                            {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // DM-3: No interpolation.
            // Storage only contains ingested points.
            // Mirrors ParquetDataModel.tla lines 214-215
            Property::always(
                "DM-3: no interpolation",
                |_model: &DataModelModel, state: &DataModelState| {
                    let stored = state.all_stored_rows();
                    stored.is_subset(&state.all_ingested_points)
                },
            ),
            // DM-4: Deterministic timeseries_id.
            // Same tags => same timeseries_id.
            // Mirrors ParquetDataModel.tla lines 221-224
            Property::always(
                "DM-4: deterministic TSID",
                |_model: &DataModelModel, state: &DataModelState| {
                    let stored = state.all_stored_rows();
                    let pending = state.all_pending_rows();
                    let all: BTreeSet<&DataPoint> = stored.iter().chain(pending.iter()).collect();
                    for r1 in &all {
                        for r2 in &all {
                            if r1.tags == r2.tags && r1.timeseries_id != r2.timeseries_id {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // DM-5: timeseries_id persists through compaction.
            // Every stored row's timeseries_id equals TSIDHash(tags).
            // Mirrors ParquetDataModel.tla lines 234-236
            Property::always(
                "DM-5: TSID persistence",
                |_model: &DataModelModel, state: &DataModelState| {
                    for row in state.all_stored_rows() {
                        if row.timeseries_id != tsid_hash(row.tags) {
                            return false;
                        }
                    }
                    true
                },
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_data_model_small() {
        let model = DataModelModel::small();
        model.checker().spawn_bfs().join().assert_properties();
    }

    #[test]
    fn tsid_hash_deterministic() {
        let t1 = TagSet(42);
        assert_eq!(tsid_hash(t1), tsid_hash(t1));
        assert_eq!(tsid_hash(TagSet(1)), tsid_hash(TagSet(1)));
    }
}
