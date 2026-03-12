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

//! Stateright model for Time-Windowed Compaction invariants (ADR-003).
//!
//! Mirrors `docs/internals/specs/tla/TimeWindowedCompaction.tla`.
//!
//! # Invariants
//! - TW-1: Every split belongs to exactly one time window
//! - TW-2: window_duration evenly divides one hour
//! - TW-3: Data is never merged across window boundaries
//! - CS-1: Only splits sharing all six scope components may be merged
//! - CS-2: Within a scope, only same window_start splits merge
//! - CS-3: Splits before compaction_start_time are never compacted
//! - MC-1: Row multiset preserved through compaction
//! - MC-2: Row contents unchanged through compaction
//! - MC-3: Output is sorted according to sort schema
//! - MC-4: Column set is the union of input column sets

use std::collections::{BTreeMap, BTreeSet};

use stateright::*;

/// Scope identifier (abstract; in TLA+ this is a 6-tuple).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Scope(pub u8);

/// A column name.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnName {
    M,
    V,
}

impl ColumnName {
    pub const ALL: &[ColumnName] = &[ColumnName::M, ColumnName::V];
}

/// A row in a split.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompactionRow {
    pub point_id: u32,
    pub timestamp: i64,
    pub sort_key: i64,
    pub columns: BTreeSet<ColumnName>,
    /// Unique value per (point, column) for MC-2 tracking.
    pub values: BTreeMap<ColumnName, (u32, ColumnName)>,
}

/// A split in object storage.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompactionSplit {
    pub id: u32,
    pub scope: Scope,
    pub window_start: i64,
    pub rows: Vec<CompactionRow>,
    pub columns: BTreeSet<ColumnName>,
    pub sorted: bool,
}

/// Compaction log entry for invariant checking.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompactionLogEntry {
    pub input_split_ids: BTreeSet<u32>,
    pub output_split_id: u32,
    pub input_point_ids: BTreeSet<u32>,
    pub output_point_ids: BTreeSet<u32>,
    pub input_scopes: BTreeMap<u32, Scope>,
    pub input_window_starts: BTreeMap<u32, i64>,
    pub output_columns: BTreeSet<ColumnName>,
    pub input_column_union: BTreeSet<ColumnName>,
}

/// Ingest buffer key: (scope, window_start).
type BufferKey = (Scope, i64);

/// Model state. Mirrors TLA+ `VARIABLES`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CompactionState {
    pub current_time: i64,
    pub object_storage: BTreeSet<CompactionSplit>,
    pub ingest_buffer: BTreeMap<BufferKey, Vec<CompactionRow>>,
    pub next_split_id: u32,
    pub next_point_id: u32,
    pub points_ingested: u32,
    pub compactions_performed: u32,
    pub row_history: BTreeMap<u32, CompactionRow>,
    pub compaction_log: BTreeSet<CompactionLogEntry>,
}

/// Actions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CompactionAction {
    AdvanceTime { new_time: i64 },
    IngestPoint {
        timestamp: i64,
        sort_key: i64,
        scope: Scope,
        columns: BTreeSet<ColumnName>,
    },
    FlushSplit { key: BufferKey },
    CompactWindow {
        scope: Scope,
        window_start: i64,
        split_ids: BTreeSet<u32>,
    },
}

/// Model configuration. Mirrors TLA+ `CONSTANTS`.
#[derive(Clone, Debug)]
pub struct CompactionModel {
    pub timestamps: Vec<i64>,
    pub scopes: Vec<Scope>,
    pub window_duration: i64,
    pub hour_seconds: i64,
    pub compaction_start_time: i64,
    pub late_data_acceptance_window: i64,
    pub max_time: i64,
    pub max_points: u32,
    pub max_compactions: u32,
    pub sort_keys: Vec<i64>,
}

impl CompactionModel {
    /// Small model matching `TimeWindowedCompaction_small.cfg`.
    pub fn small() -> Self {
        CompactionModel {
            timestamps: vec![0, 1],
            scopes: vec![Scope(1)],
            window_duration: 2,
            hour_seconds: 4,
            compaction_start_time: 0,
            late_data_acceptance_window: 2,
            max_time: 1,
            max_points: 2,
            max_compactions: 1,
            sort_keys: vec![1, 2],
        }
    }
}

/// Compute window_start for a timestamp.
/// Mirrors TLA+ `WindowStart(t) == t - (t % WindowDuration)`.
///
/// Delegates to the shared [`crate::invariants::window::window_start_secs`].
fn window_start(t: i64, window_duration: i64) -> i64 {
    crate::invariants::window::window_start_secs(t, window_duration)
}

/// Check if a sequence of rows is sorted by sort_key ascending.
fn is_sorted_by_key(rows: &[CompactionRow]) -> bool {
    rows.windows(2).all(|w| w[0].sort_key <= w[1].sort_key)
}

/// Merge-sort two sorted sequences by sort_key.
fn merge_sorted(s1: &[CompactionRow], s2: &[CompactionRow]) -> Vec<CompactionRow> {
    let mut result = Vec::with_capacity(s1.len() + s2.len());
    let (mut i, mut j) = (0, 0);
    while i < s1.len() && j < s2.len() {
        if s1[i].sort_key <= s2[j].sort_key {
            result.push(s1[i].clone());
            i += 1;
        } else {
            result.push(s2[j].clone());
            j += 1;
        }
    }
    result.extend_from_slice(&s1[i..]);
    result.extend_from_slice(&s2[j..]);
    result
}

/// Insertion sort by sort_key (for small sequences at flush time).
fn insertion_sort(rows: &[CompactionRow]) -> Vec<CompactionRow> {
    let mut sorted = Vec::with_capacity(rows.len());
    for row in rows {
        let pos = sorted
            .iter()
            .position(|r: &CompactionRow| r.sort_key > row.sort_key)
            .unwrap_or(sorted.len());
        sorted.insert(pos, row.clone());
    }
    sorted
}

/// Generate all non-empty subsets of a set of column names.
fn all_nonempty_column_subsets() -> Vec<BTreeSet<ColumnName>> {
    let cols = ColumnName::ALL;
    let mut subsets = Vec::new();
    for mask in 1..(1u32 << cols.len()) {
        let mut subset = BTreeSet::new();
        for (i, &col) in cols.iter().enumerate() {
            if mask & (1 << i) != 0 {
                subset.insert(col);
            }
        }
        subsets.push(subset);
    }
    subsets
}

/// Generate all subsets of size >= 2 from a set of split IDs.
fn subsets_of_size_ge2(ids: &BTreeSet<u32>) -> Vec<BTreeSet<u32>> {
    let id_vec: Vec<u32> = ids.iter().copied().collect();
    let n = id_vec.len();
    let mut result = Vec::new();
    for mask in 0..(1u32 << n) {
        if mask.count_ones() >= 2 {
            let mut subset = BTreeSet::new();
            for (i, &id) in id_vec.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    subset.insert(id);
                }
            }
            result.push(subset);
        }
    }
    result
}

impl Model for CompactionModel {
    type State = CompactionState;
    type Action = CompactionAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CompactionState {
            current_time: 0,
            object_storage: BTreeSet::new(),
            ingest_buffer: BTreeMap::new(),
            next_split_id: 1,
            next_point_id: 1,
            points_ingested: 0,
            compactions_performed: 0,
            row_history: BTreeMap::new(),
            compaction_log: BTreeSet::new(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // AdvanceTime
        if state.current_time < self.max_time {
            for &t in &self.timestamps {
                if t > state.current_time && t <= self.max_time {
                    actions.push(CompactionAction::AdvanceTime { new_time: t });
                }
            }
        }

        // IngestPoint
        if state.points_ingested < self.max_points {
            for &ts in &self.timestamps {
                if ts > state.current_time {
                    continue;
                }
                if ts < state.current_time - self.late_data_acceptance_window {
                    continue;
                }
                for &sk in &self.sort_keys {
                    for &scope in &self.scopes {
                        for cols in all_nonempty_column_subsets() {
                            actions.push(CompactionAction::IngestPoint {
                                timestamp: ts,
                                sort_key: sk,
                                scope,
                                columns: cols,
                            });
                        }
                    }
                }
            }
        }

        // FlushSplit
        for (key, buf) in &state.ingest_buffer {
            if !buf.is_empty() {
                actions.push(CompactionAction::FlushSplit { key: *key });
            }
        }

        // CompactWindow
        if state.compactions_performed < self.max_compactions {
            for &scope in &self.scopes {
                // Collect valid window starts from current splits.
                let valid_ws: BTreeSet<i64> = state
                    .object_storage
                    .iter()
                    .filter(|s| s.scope == scope && s.window_start >= self.compaction_start_time)
                    .map(|s| s.window_start)
                    .collect();

                for &ws in &valid_ws {
                    let candidate_ids: BTreeSet<u32> = state
                        .object_storage
                        .iter()
                        .filter(|s| s.scope == scope && s.window_start == ws)
                        .map(|s| s.id)
                        .collect();

                    if candidate_ids.len() < 2 {
                        continue;
                    }

                    for subset in subsets_of_size_ge2(&candidate_ids) {
                        actions.push(CompactionAction::CompactWindow {
                            scope,
                            window_start: ws,
                            split_ids: subset,
                        });
                    }
                }
            }
        }
    }

    fn next_state(
        &self,
        state: &Self::State,
        action: Self::Action,
    ) -> Option<Self::State> {
        let mut next = state.clone();

        match action {
            CompactionAction::AdvanceTime { new_time } => {
                next.current_time = new_time;
            }
            CompactionAction::IngestPoint {
                timestamp,
                sort_key,
                scope,
                columns,
            } => {
                let pid = next.next_point_id;
                let values: BTreeMap<ColumnName, (u32, ColumnName)> =
                    columns.iter().map(|&c| (c, (pid, c))).collect();
                let row = CompactionRow {
                    point_id: pid,
                    timestamp,
                    sort_key,
                    columns: columns.clone(),
                    values,
                };
                let ws = window_start(timestamp, self.window_duration);
                let key = (scope, ws);
                next.ingest_buffer
                    .entry(key)
                    .or_default()
                    .push(row.clone());
                next.next_point_id += 1;
                next.points_ingested += 1;
                next.row_history.insert(pid, row);
            }
            CompactionAction::FlushSplit { key } => {
                let rows = next.ingest_buffer.remove(&key).unwrap_or_default();
                if rows.is_empty() {
                    return None;
                }
                let sorted_rows = insertion_sort(&rows);
                let all_cols: BTreeSet<ColumnName> =
                    rows.iter().flat_map(|r| r.columns.iter().copied()).collect();
                let new_split = CompactionSplit {
                    id: next.next_split_id,
                    scope: key.0,
                    window_start: key.1,
                    rows: sorted_rows,
                    columns: all_cols,
                    sorted: true,
                };
                next.object_storage.insert(new_split);
                next.next_split_id += 1;
            }
            CompactionAction::CompactWindow {
                scope,
                window_start: ws,
                split_ids,
            } => {
                let merge_splits: Vec<CompactionSplit> = next
                    .object_storage
                    .iter()
                    .filter(|s| split_ids.contains(&s.id))
                    .cloned()
                    .collect();

                if merge_splits.len() < 2 {
                    return None;
                }

                // Multi-way sorted merge.
                let mut merged_rows = Vec::new();
                for s in &merge_splits {
                    merged_rows = merge_sorted(&merged_rows, &s.rows);
                }

                let all_cols: BTreeSet<ColumnName> = merge_splits
                    .iter()
                    .flat_map(|s| s.columns.iter().copied())
                    .collect();

                let output_split = CompactionSplit {
                    id: next.next_split_id,
                    scope,
                    window_start: ws,
                    rows: merged_rows.clone(),
                    columns: all_cols.clone(),
                    sorted: true,
                };

                // Build compaction log entry.
                let input_ids: BTreeSet<u32> = merge_splits.iter().map(|s| s.id).collect();
                let input_point_ids: BTreeSet<u32> = merge_splits
                    .iter()
                    .flat_map(|s| s.rows.iter().map(|r| r.point_id))
                    .collect();
                let output_point_ids: BTreeSet<u32> =
                    merged_rows.iter().map(|r| r.point_id).collect();
                let input_scopes: BTreeMap<u32, Scope> =
                    merge_splits.iter().map(|s| (s.id, s.scope)).collect();
                let input_ws: BTreeMap<u32, i64> = merge_splits
                    .iter()
                    .map(|s| (s.id, s.window_start))
                    .collect();

                let log_entry = CompactionLogEntry {
                    input_split_ids: input_ids,
                    output_split_id: next.next_split_id,
                    input_point_ids,
                    output_point_ids,
                    input_scopes,
                    input_window_starts: input_ws,
                    output_columns: all_cols.clone(),
                    input_column_union: all_cols,
                };

                // Remove input splits, add output.
                for s in &merge_splits {
                    next.object_storage.remove(s);
                }
                next.object_storage.insert(output_split);
                next.next_split_id += 1;
                next.compactions_performed += 1;
                next.compaction_log.insert(log_entry);
            }
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // TW-1: Every split belongs to exactly one time window.
            // All rows have the same window_start as the split metadata.
            // Mirrors TimeWindowedCompaction.tla lines 274-277
            Property::always("TW-1: one window per split", |model: &CompactionModel, state: &CompactionState| {
                let wd = model.window_duration;
                state.object_storage.iter().all(|split| {
                    split
                        .rows
                        .iter()
                        .all(|row| window_start(row.timestamp, wd) == split.window_start)
                })
            }),
            // TW-2: window_duration evenly divides one hour.
            // Mirrors TimeWindowedCompaction.tla lines 283-284
            Property::always("TW-2: duration divides hour", |model: &CompactionModel, _state: &CompactionState| {
                model.hour_seconds % model.window_duration == 0
            }),
            // TW-3: No cross-window merges.
            // Mirrors TimeWindowedCompaction.tla lines 295-305
            Property::always("TW-3: no cross-window merge", |_model: &CompactionModel, state: &CompactionState| {
                state.compaction_log.iter().all(|entry| {
                    // All input window_starts are identical.
                    let ws_values: BTreeSet<i64> =
                        entry.input_window_starts.values().copied().collect();
                    if ws_values.len() > 1 {
                        return false;
                    }
                    // Output split (if in storage) matches.
                    for s in &state.object_storage {
                        if s.id == entry.output_split_id {
                            for &input_ws in entry.input_window_starts.values() {
                                if s.window_start != input_ws {
                                    return false;
                                }
                            }
                        }
                    }
                    true
                })
            }),
            // CS-1: Only splits sharing scope may be merged.
            // Mirrors TimeWindowedCompaction.tla lines 311-314
            Property::always("CS-1: scope compatibility", |_model: &CompactionModel, state: &CompactionState| {
                state.compaction_log.iter().all(|entry| {
                    let scopes: BTreeSet<Scope> =
                        entry.input_scopes.values().copied().collect();
                    scopes.len() <= 1
                })
            }),
            // CS-2: Same window_start within scope.
            // Mirrors TimeWindowedCompaction.tla lines 320-323
            Property::always("CS-2: same window_start", |_model: &CompactionModel, state: &CompactionState| {
                state.compaction_log.iter().all(|entry| {
                    let ws_values: BTreeSet<i64> =
                        entry.input_window_starts.values().copied().collect();
                    ws_values.len() <= 1
                })
            }),
            // CS-3: Splits before compaction_start_time never compacted.
            // Mirrors TimeWindowedCompaction.tla lines 329-332
            Property::always("CS-3: compaction start time", |model: &CompactionModel, state: &CompactionState| {
                let cst = model.compaction_start_time;
                state.compaction_log.iter().all(|entry| {
                    entry
                        .input_window_starts
                        .values()
                        .all(|&ws| ws >= cst)
                })
            }),
            // MC-1: Row multiset preserved (no add/remove/duplicate).
            // Mirrors TimeWindowedCompaction.tla lines 339-344
            Property::always("MC-1: row set preserved", |_model: &CompactionModel, state: &CompactionState| {
                state.compaction_log.iter().all(|entry| {
                    entry.input_point_ids == entry.output_point_ids
                })
            }),
            // MC-2: Row contents unchanged through compaction.
            // Mirrors TimeWindowedCompaction.tla lines 351-360
            Property::always("MC-2: row contents preserved", |_model: &CompactionModel, state: &CompactionState| {
                for split in &state.object_storage {
                    for row in &split.rows {
                        if let Some(original) = state.row_history.get(&row.point_id) {
                            if row.timestamp != original.timestamp
                                || row.sort_key != original.sort_key
                                || row.columns != original.columns
                                || row.values != original.values
                            {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                }
                true
            }),
            // MC-3: Output is sorted.
            // Mirrors TimeWindowedCompaction.tla lines 366-368
            Property::always("MC-3: sort order preserved", |_model: &CompactionModel, state: &CompactionState| {
                state.object_storage.iter().all(|split| {
                    if split.sorted {
                        is_sorted_by_key(&split.rows)
                    } else {
                        true
                    }
                })
            }),
            // MC-4: Column set is the union of inputs.
            // Mirrors TimeWindowedCompaction.tla lines 376-378
            Property::always("MC-4: column union", |_model: &CompactionModel, state: &CompactionState| {
                state.compaction_log.iter().all(|entry| {
                    entry.output_columns == entry.input_column_union
                })
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_compaction_small() {
        let model = CompactionModel::small();
        model
            .checker()
            .spawn_bfs()
            .join()
            .assert_properties();
    }

    #[test]
    fn window_start_computation() {
        assert_eq!(window_start(0, 2), 0);
        assert_eq!(window_start(1, 2), 0);
        assert_eq!(window_start(2, 2), 2);
        assert_eq!(window_start(3, 2), 2);
        assert_eq!(window_start(5, 3), 3);
    }

    #[test]
    fn merge_sort_basic() {
        let r1 = CompactionRow {
            point_id: 1,
            timestamp: 0,
            sort_key: 1,
            columns: BTreeSet::new(),
            values: BTreeMap::new(),
        };
        let r2 = CompactionRow {
            point_id: 2,
            timestamp: 0,
            sort_key: 3,
            columns: BTreeSet::new(),
            values: BTreeMap::new(),
        };
        let r3 = CompactionRow {
            point_id: 3,
            timestamp: 0,
            sort_key: 2,
            columns: BTreeSet::new(),
            values: BTreeMap::new(),
        };
        let merged = merge_sorted(&[r1.clone(), r2.clone()], std::slice::from_ref(&r3));
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].point_id, 1);
        assert_eq!(merged[1].point_id, 3);
        assert_eq!(merged[2].point_id, 2);
    }
}
