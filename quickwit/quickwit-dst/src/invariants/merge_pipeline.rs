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

//! Shared state and lifecycle invariants for the Parquet merge pipeline.
//!
//! This module is the **single source of truth** for the predicates verified
//! by the TLA+ spec `MergePipelineShutdown.tla`, the Stateright model
//! `models::merge_pipeline`, and (via [`check_invariant!`](crate::check_invariant))
//! the production code paths in `quickwit-indexing`.
//!
//! The state struct mirrors the variables of the TLA+ spec literally so the
//! Stateright model can use the *same* type rather than maintaining its own
//! parallel definition.
//!
//! # Predicate ↔ TLA+ Invariant Mapping
//!
//! | Predicate                       | TLA+ name                     | InvariantId |
//! |---------------------------------|-------------------------------|-------------|
//! | [`mp1_level_homogeneity`]       | `MP1_LevelHomogeneity`        | `MP1`       |
//! | [`rows_conserved`]              | `RowsConserved`               | `MP4`       |
//! | [`bounded_write_amp`]           | `BoundedWriteAmp`             | `MP5`       |
//! | [`no_split_loss`]               | `NoSplitLoss`                 | `MP6`       |
//! | [`no_duplicate_merge`]          | `NoDuplicateMerge`            | `MP7`       |
//! | [`no_orphan_in_planner`]        | `NoOrphanInPlanner`           | `MP8`       |
//! | [`no_orphan_when_connected`]    | `NoOrphanWhenConnected`       | `MP9`       |
//! | [`leak_is_object_store_only`]   | `LeakIsObjectStoreOnly`       | `MP10`      |
//! | [`restart_re_seeds_all_immature`]| `RestartReSeedsAllImmature`  | `MP11`      |

use std::collections::{BTreeMap, BTreeSet};

/// Logical split identifier in the model. Production code uses `SplitId`
/// (UUID-flavored string); the model uses `u32` so state is hashable.
pub type SplitId = u32;

/// Logical window identifier (compaction time bucket).
pub type Window = u32;

/// Logical merge operation identifier.
pub type MergeId = u32;

/// Compaction level — number of merge operations a split has been through.
pub type Level = u32;

/// Per-split bookkeeping. Combines the TLA+ functions split_rows,
/// split_merge_ops, and split_window into a single record.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SplitInfo {
    /// Number of original rows this split contains. For ingested splits
    /// this is set at ingest; for merge outputs it's the sum of input rows.
    pub rows: u64,
    /// Compaction level. 0 for ingested splits; level + 1 for merge outputs.
    pub merge_ops: Level,
    /// Compaction window assignment.
    pub window: Window,
}

/// In-flight merge record. Mirrors the TLA+ record type with one
/// adjustment: `output_id` is `Option<SplitId>` rather than the sentinel
/// 0 used in TLA+ — `None` before upload, `Some(_)` after.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InFlightMerge {
    pub id: MergeId,
    pub inputs: BTreeSet<SplitId>,
    pub level: Level,
    pub window: Window,
    pub uploaded: bool,
    pub output_id: Option<SplitId>,
}

/// State of the merge pipeline. Field-for-field mirror of the TLA+ spec
/// `MergePipelineShutdown.tla`'s VARIABLES block.
///
/// Uses `BTreeMap`/`BTreeSet` rather than `HashMap`/`HashSet` so equality
/// and hashing are deterministic — required by Stateright's state-graph
/// search.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MergePipelineState {
    /// Whether the publisher sends feedback to the planner.
    pub planner_connected: bool,
    /// Whether the planner is alive and processing.
    pub planner_alive: bool,
    /// In-flight merges keyed by merge ID.
    pub in_flight_merges: BTreeMap<MergeId, InFlightMerge>,
    /// Window -> set of immature split IDs available for merging.
    pub cold_windows: BTreeMap<Window, BTreeSet<SplitId>>,
    /// Splits durable in the metastore (queryable).
    pub published_splits: BTreeSet<SplitId>,
    /// Per-split bookkeeping (rows, merge_ops, window).
    pub splits: BTreeMap<SplitId, SplitInfo>,
    /// Splits uploaded to object storage but never published — left as
    /// orphans by a crash between UploadMergeOutput and
    /// PublishMergeAndFeedback.
    pub orphan_outputs: BTreeSet<SplitId>,
    /// Whether finalization has been requested.
    pub finalize_requested: bool,
    /// Number of finalize merge operations emitted.
    pub finalize_ops_emitted: u32,
    /// Whether the pipeline has completed graceful shutdown.
    pub shutdown_complete: bool,
    /// Counter for generating unique IDs.
    pub next_id: u32,
    /// Total number of ingests performed (bounds state space in models).
    pub ingests_performed: u32,
    /// Number of crashes (bounds state space).
    pub crashes_performed: u32,
    /// Number of restarts (bounds state space).
    pub restarts_performed: u32,
    /// Ghost variable: cumulative rows ever ingested. Used by
    /// [`rows_conserved`].
    pub total_ingested_rows: u64,
}

impl MergePipelineState {
    /// Initial state: planner alive and connected, everything else empty.
    pub fn initial() -> Self {
        MergePipelineState {
            planner_connected: true,
            planner_alive: true,
            in_flight_merges: BTreeMap::new(),
            cold_windows: BTreeMap::new(),
            published_splits: BTreeSet::new(),
            splits: BTreeMap::new(),
            orphan_outputs: BTreeSet::new(),
            finalize_requested: false,
            finalize_ops_emitted: 0,
            shutdown_complete: false,
            next_id: 1,
            ingests_performed: 0,
            crashes_performed: 0,
            restarts_performed: 0,
            total_ingested_rows: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// All split IDs currently held by any in-flight merge as inputs.
pub fn all_in_flight_input_ids(state: &MergePipelineState) -> BTreeSet<SplitId> {
    state
        .in_flight_merges
        .values()
        .flat_map(|m| m.inputs.iter().copied())
        .collect()
}

/// All split IDs currently visible to the planner across all windows.
pub fn all_planner_split_ids(state: &MergePipelineState) -> BTreeSet<SplitId> {
    state
        .cold_windows
        .values()
        .flat_map(|set| set.iter().copied())
        .collect()
}

/// Sum of `rows` across a set of split IDs.
pub fn sum_rows(state: &MergePipelineState, ids: &BTreeSet<SplitId>) -> u64 {
    ids.iter()
        .filter_map(|id| state.splits.get(id).map(|info| info.rows))
        .sum()
}

/// Splits that are published, immature, and *not* tracked by the planner
/// (neither in `cold_windows` nor in any in-flight merge). These are the
/// splits at risk of being permanently invisible to compaction without a
/// process restart re-seeding.
///
/// Mirrors the TLA+ `OrphanSet` operator.
pub fn orphan_set(state: &MergePipelineState, max_merge_ops: Level) -> BTreeSet<SplitId> {
    let in_flight = all_in_flight_input_ids(state);
    let in_planner = all_planner_split_ids(state);
    state
        .published_splits
        .iter()
        .copied()
        .filter(|id| {
            let info = match state.splits.get(id) {
                Some(info) => info,
                None => return false,
            };
            info.merge_ops < max_merge_ops && !in_planner.contains(id) && !in_flight.contains(id)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Predicates (single source of truth for TLA+ / Stateright / production)
// ---------------------------------------------------------------------------

/// MP-1: every input of an in-flight merge shares the same `merge_ops` level.
///
/// Mirrors `MP1_LevelHomogeneity` in the TLA+ spec. Violation indicates a
/// planner bug — mixing levels would prematurely mature lower-level data
/// and break bounded write amplification.
pub fn mp1_level_homogeneity(state: &MergePipelineState) -> bool {
    state.in_flight_merges.values().all(|m| {
        m.inputs.iter().all(|id| {
            state
                .splits
                .get(id)
                .map(|info| info.merge_ops == m.level)
                .unwrap_or(false)
        })
    })
}

/// MP-4: total rows across published splits equal cumulative rows ever
/// ingested.
///
/// Mirrors `RowsConserved`. The strong "no data loss, no duplication"
/// invariant — survives all crash, restart, and shutdown paths.
pub fn rows_conserved(state: &MergePipelineState) -> bool {
    let published_rows: u64 = sum_rows(state, &state.published_splits);
    published_rows == state.total_ingested_rows
}

/// MP-5: every published split has `merge_ops <= max_merge_ops`.
///
/// Mirrors `BoundedWriteAmp`. Caps the number of times any data is
/// rewritten through compaction.
pub fn bounded_write_amp(state: &MergePipelineState, max_merge_ops: Level) -> bool {
    state.published_splits.iter().all(|id| {
        state
            .splits
            .get(id)
            .map(|info| info.merge_ops <= max_merge_ops)
            .unwrap_or(true)
    })
}

/// MP-6: every input of an in-flight merge is still in `published_splits`.
///
/// Mirrors `NoSplitLoss`. Inputs are durable in the metastore until the
/// publish step (`PublishMergeAndFeedback`) runs.
pub fn no_split_loss(state: &MergePipelineState) -> bool {
    state.in_flight_merges.values().all(|m| {
        m.inputs
            .iter()
            .all(|id| state.published_splits.contains(id))
    })
}

/// MP-7: no split appears in two concurrent in-flight merges.
///
/// Mirrors `NoDuplicateMerge`. Violated only by a planner bug.
pub fn no_duplicate_merge(state: &MergePipelineState) -> bool {
    let merges: Vec<&BTreeSet<SplitId>> =
        state.in_flight_merges.values().map(|m| &m.inputs).collect();
    for i in 0..merges.len() {
        for j in (i + 1)..merges.len() {
            if !merges[i].is_disjoint(merges[j]) {
                return false;
            }
        }
    }
    true
}

/// MP-8: no split is simultaneously in `cold_windows` and any in-flight
/// merge. The planner can't re-merge a split it has already locked.
///
/// Mirrors `NoOrphanInPlanner`.
pub fn no_orphan_in_planner(state: &MergePipelineState) -> bool {
    let in_flight = all_in_flight_input_ids(state);
    let in_planner = all_planner_split_ids(state);
    in_flight.is_disjoint(&in_planner)
}

/// MP-9: while the planner is alive and connected, every immature
/// published split is reachable from `cold_windows` or `in_flight_merges`.
///
/// Mirrors `NoOrphanWhenConnected`. Disconnect (graceful shutdown) and
/// crash both relax this; Restart re-establishes it via re-seeding.
pub fn no_orphan_when_connected(state: &MergePipelineState, max_merge_ops: Level) -> bool {
    if !(state.planner_alive && state.planner_connected) {
        return true;
    }
    let in_flight = all_in_flight_input_ids(state);
    let in_planner = all_planner_split_ids(state);
    state.published_splits.iter().all(|id| {
        let info = match state.splits.get(id) {
            Some(info) => info,
            None => return true,
        };
        info.merge_ops >= max_merge_ops || in_planner.contains(id) || in_flight.contains(id)
    })
}

/// MP-10: orphan_outputs and published_splits are disjoint.
///
/// Mirrors `LeakIsObjectStoreOnly`. Confirms that crash-induced orphans
/// only leak storage, never data — they're never durable in the metastore.
pub fn leak_is_object_store_only(state: &MergePipelineState) -> bool {
    state.orphan_outputs.is_disjoint(&state.published_splits)
}

/// MP-11: in any state where `restart_re_seeds_all_immature` is checked
/// (i.e. immediately after a Restart action), every immature published
/// split is in `cold_windows`.
///
/// Mirrors the action property `RestartReSeedsAllImmature` from the TLA+
/// spec. This is a *post-condition* of the Restart transition rather than
/// a state invariant: it must hold in the post-state of every Restart, but
/// is not required to hold continuously.
pub fn restart_re_seeds_all_immature(state: &MergePipelineState, max_merge_ops: Level) -> bool {
    let in_planner = all_planner_split_ids(state);
    state.published_splits.iter().all(|id| {
        let info = match state.splits.get(id) {
            Some(info) => info,
            None => return true,
        };
        info.merge_ops >= max_merge_ops || in_planner.contains(id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with_split(id: SplitId, info: SplitInfo, published: bool) -> MergePipelineState {
        let mut s = MergePipelineState::initial();
        s.splits.insert(id, info);
        if published {
            s.published_splits.insert(id);
        }
        s
    }

    #[test]
    fn rows_conserved_initial_state() {
        let s = MergePipelineState::initial();
        assert!(rows_conserved(&s));
    }

    #[test]
    fn rows_conserved_after_ingest() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 5,
                merge_ops: 0,
                window: 0,
            },
        );
        s.published_splits.insert(1);
        s.total_ingested_rows = 5;
        assert!(rows_conserved(&s));
    }

    #[test]
    fn rows_conserved_detects_loss() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 5,
                merge_ops: 0,
                window: 0,
            },
        );
        // Forgot to add 1 to published_splits.
        s.total_ingested_rows = 5;
        assert!(!rows_conserved(&s));
    }

    #[test]
    fn bounded_write_amp_holds_at_max() {
        let s = state_with_split(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 2,
                window: 0,
            },
            true,
        );
        assert!(bounded_write_amp(&s, 2));
        assert!(!bounded_write_amp(&s, 1));
    }

    #[test]
    fn no_split_loss_when_inputs_published() {
        let mut s = MergePipelineState::initial();
        s.published_splits.insert(1);
        s.published_splits.insert(2);
        s.in_flight_merges.insert(
            10,
            InFlightMerge {
                id: 10,
                inputs: [1, 2].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        assert!(no_split_loss(&s));
    }

    #[test]
    fn no_split_loss_detects_missing_input() {
        let mut s = MergePipelineState::initial();
        s.published_splits.insert(1); // 2 is missing
        s.in_flight_merges.insert(
            10,
            InFlightMerge {
                id: 10,
                inputs: [1, 2].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        assert!(!no_split_loss(&s));
    }

    #[test]
    fn no_duplicate_merge_disjoint_inputs() {
        let mut s = MergePipelineState::initial();
        s.in_flight_merges.insert(
            10,
            InFlightMerge {
                id: 10,
                inputs: [1, 2].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        s.in_flight_merges.insert(
            11,
            InFlightMerge {
                id: 11,
                inputs: [3, 4].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        assert!(no_duplicate_merge(&s));
    }

    #[test]
    fn no_duplicate_merge_detects_overlap() {
        let mut s = MergePipelineState::initial();
        s.in_flight_merges.insert(
            10,
            InFlightMerge {
                id: 10,
                inputs: [1, 2].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        s.in_flight_merges.insert(
            11,
            InFlightMerge {
                id: 11,
                inputs: [2, 3].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        assert!(!no_duplicate_merge(&s));
    }

    #[test]
    fn leak_is_object_store_only_disjoint() {
        let mut s = MergePipelineState::initial();
        s.published_splits.insert(1);
        s.orphan_outputs.insert(99);
        assert!(leak_is_object_store_only(&s));
    }

    #[test]
    fn leak_is_object_store_only_detects_overlap() {
        let mut s = MergePipelineState::initial();
        s.published_splits.insert(1);
        s.orphan_outputs.insert(1);
        assert!(!leak_is_object_store_only(&s));
    }

    #[test]
    fn mp1_level_homogeneity_same_level() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 0,
            },
        );
        s.splits.insert(
            2,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 0,
            },
        );
        s.in_flight_merges.insert(
            10,
            InFlightMerge {
                id: 10,
                inputs: [1, 2].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        assert!(mp1_level_homogeneity(&s));
    }

    #[test]
    fn mp1_level_homogeneity_detects_mismatch() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 0,
            },
        );
        s.splits.insert(
            2,
            SplitInfo {
                rows: 1,
                merge_ops: 1, // different level!
                window: 0,
            },
        );
        s.in_flight_merges.insert(
            10,
            InFlightMerge {
                id: 10,
                inputs: [1, 2].iter().copied().collect(),
                level: 0,
                window: 0,
                uploaded: false,
                output_id: None,
            },
        );
        assert!(!mp1_level_homogeneity(&s));
    }

    #[test]
    fn no_orphan_when_connected_passes_when_tracked() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 7,
            },
        );
        s.published_splits.insert(1);
        s.cold_windows.insert(7, [1].iter().copied().collect());
        s.total_ingested_rows = 1;
        assert!(no_orphan_when_connected(&s, 2));
    }

    #[test]
    fn no_orphan_when_connected_passes_when_disconnected() {
        let mut s = MergePipelineState::initial();
        s.planner_connected = false;
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 7,
            },
        );
        s.published_splits.insert(1);
        // 1 is NOT in cold_windows but planner is disconnected, so OK.
        s.total_ingested_rows = 1;
        assert!(no_orphan_when_connected(&s, 2));
    }

    #[test]
    fn no_orphan_when_connected_detects_orphan() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 7,
            },
        );
        s.published_splits.insert(1);
        // Planner is connected but split 1 is not in cold_windows.
        s.total_ingested_rows = 1;
        assert!(!no_orphan_when_connected(&s, 2));
    }

    #[test]
    fn orphan_set_finds_disconnected_publish() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 7,
            },
        );
        s.published_splits.insert(1);
        s.planner_connected = false;
        // Split 1 is published, immature, not in cold_windows, not in flight.
        let orphans = orphan_set(&s, 2);
        assert_eq!(orphans, [1].iter().copied().collect());
    }

    #[test]
    fn restart_re_seeds_all_immature_passes_when_all_in_planner() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 7,
            },
        );
        s.published_splits.insert(1);
        s.cold_windows.insert(7, [1].iter().copied().collect());
        assert!(restart_re_seeds_all_immature(&s, 2));
    }

    #[test]
    fn restart_re_seeds_all_immature_ignores_mature() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 2, // mature
                window: 7,
            },
        );
        s.published_splits.insert(1);
        // Mature splits don't need to be in cold_windows.
        assert!(restart_re_seeds_all_immature(&s, 2));
    }

    #[test]
    fn restart_re_seeds_all_immature_detects_missed_immature() {
        let mut s = MergePipelineState::initial();
        s.splits.insert(
            1,
            SplitInfo {
                rows: 1,
                merge_ops: 0,
                window: 7,
            },
        );
        s.published_splits.insert(1);
        // Immature, but cold_windows is empty.
        assert!(!restart_re_seeds_all_immature(&s, 2));
    }
}
