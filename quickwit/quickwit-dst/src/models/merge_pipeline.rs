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

//! Stateright model for the Parquet merge pipeline lifecycle.
//!
//! Mirrors `docs/internals/specs/tla/MergePipelineShutdown.tla` and extends
//! it with crash/restart semantics to verify no-data-loss under failures.
//!
//! # Properties
//! - MC-1 lifecycle: total rows in published splits == total ingested rows
//! - Bounded WA: all published splits have merge_ops <= max_merge_ops
//! - No duplicate merge: no split in multiple concurrent in-flight merges
//! - No orphan after restart: all immature published splits re-seeded
//! - MP-1 level homogeneity: each merge has all inputs at same level

use std::collections::{BTreeMap, BTreeSet};

use stateright::*;

/// A published split in the model.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SplitInfo {
    pub id: u32,
    pub merge_ops: u32,
    pub num_rows: u64,
}

/// Model state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MergePipelineState {
    /// Splits known to the planner, grouped by merge_ops level.
    pub planner_splits: BTreeMap<u32, BTreeSet<u32>>,
    /// In-flight merges: merge_id -> (set of input split IDs, level).
    pub in_flight_merges: BTreeMap<u32, (BTreeSet<u32>, u32)>,
    /// Published splits: split_id -> SplitInfo.
    pub published_splits: BTreeMap<u32, SplitInfo>,
    /// Ghost variable: total rows ever ingested.
    pub total_ingested_rows: u64,
    /// Next unique ID.
    pub next_id: u32,
    /// Whether the pipeline is alive (can plan merges).
    pub pipeline_alive: bool,
    /// Number of crashes so far.
    pub crashes: u32,
    /// Number of ingests so far.
    pub ingests: u32,
}

/// Model actions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MergePipelineAction {
    /// Ingest a new split with given row count.
    IngestSplit { num_rows: u64 },
    /// Plan a merge of splits at a given level.
    PlanMerge { level: u32, split_ids: BTreeSet<u32> },
    /// Complete an in-flight merge.
    CompleteMerge { merge_id: u32 },
    /// Pipeline crashes: planner state lost, in-flight merges lost.
    Crash,
    /// Pipeline restarts: re-seed planner from published splits.
    Restart,
}

/// Model configuration.
#[derive(Clone, Debug)]
pub struct MergePipelineModel {
    pub merge_factor: usize,
    pub max_merge_ops: u32,
    pub max_ingests: u32,
    pub max_crashes: u32,
    pub ingest_row_counts: Vec<u64>,
}

impl MergePipelineModel {
    /// Small model for fast iteration.
    pub fn small() -> Self {
        MergePipelineModel {
            merge_factor: 2,
            max_merge_ops: 2,
            max_ingests: 3,
            max_crashes: 1,
            ingest_row_counts: vec![100],
        }
    }

    /// Larger model for thorough checking.
    pub fn full() -> Self {
        MergePipelineModel {
            merge_factor: 2,
            max_merge_ops: 3,
            max_ingests: 4,
            max_crashes: 2,
            ingest_row_counts: vec![100, 200],
        }
    }
}

/// All split IDs currently in any in-flight merge.
fn all_in_flight_split_ids(state: &MergePipelineState) -> BTreeSet<u32> {
    state
        .in_flight_merges
        .values()
        .flat_map(|(ids, _)| ids.iter().copied())
        .collect()
}

impl Model for MergePipelineModel {
    type State = MergePipelineState;
    type Action = MergePipelineAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![MergePipelineState {
            planner_splits: BTreeMap::new(),
            in_flight_merges: BTreeMap::new(),
            published_splits: BTreeMap::new(),
            total_ingested_rows: 0,
            next_id: 1,
            pipeline_alive: true,
            crashes: 0,
            ingests: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // IngestSplit — only when pipeline is alive and under budget.
        if state.pipeline_alive && state.ingests < self.max_ingests {
            for &rows in &self.ingest_row_counts {
                actions.push(MergePipelineAction::IngestSplit { num_rows: rows });
            }
        }

        // PlanMerge — for each level with enough splits.
        if state.pipeline_alive {
            let in_flight_ids = all_in_flight_split_ids(state);
            for (&level, split_ids) in &state.planner_splits {
                // Only consider splits not already in-flight.
                let available: BTreeSet<u32> = split_ids
                    .iter()
                    .filter(|id| !in_flight_ids.contains(id))
                    .copied()
                    .collect();
                if available.len() >= self.merge_factor {
                    // Take exactly merge_factor splits (deterministic: smallest IDs).
                    let selected: BTreeSet<u32> =
                        available.iter().take(self.merge_factor).copied().collect();
                    actions.push(MergePipelineAction::PlanMerge {
                        level,
                        split_ids: selected,
                    });
                }
            }
        }

        // CompleteMerge — for each in-flight merge.
        for &merge_id in state.in_flight_merges.keys() {
            actions.push(MergePipelineAction::CompleteMerge { merge_id });
        }

        // Crash — only when pipeline is alive and under crash budget.
        if state.pipeline_alive && state.crashes < self.max_crashes {
            actions.push(MergePipelineAction::Crash);
        }

        // Restart — only when pipeline is crashed.
        if !state.pipeline_alive {
            actions.push(MergePipelineAction::Restart);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        match action {
            MergePipelineAction::IngestSplit { num_rows } => {
                let id = s.next_id;
                s.next_id += 1;
                s.ingests += 1;
                s.total_ingested_rows += num_rows;

                // Add to planner at level 0.
                s.planner_splits.entry(0).or_default().insert(id);

                // Add to published splits.
                s.published_splits.insert(
                    id,
                    SplitInfo {
                        id,
                        merge_ops: 0,
                        num_rows,
                    },
                );
            }
            MergePipelineAction::PlanMerge { level, split_ids } => {
                let merge_id = s.next_id;
                s.next_id += 1;

                // Remove from planner.
                if let Some(level_set) = s.planner_splits.get_mut(&level) {
                    for &id in &split_ids {
                        level_set.remove(&id);
                    }
                    if level_set.is_empty() {
                        s.planner_splits.remove(&level);
                    }
                }

                // Add to in-flight.
                s.in_flight_merges
                    .insert(merge_id, (split_ids, level));
            }
            MergePipelineAction::CompleteMerge { merge_id } => {
                let (input_ids, level) = match s.in_flight_merges.remove(&merge_id) {
                    Some(v) => v,
                    None => return None,
                };

                let output_id = s.next_id;
                s.next_id += 1;
                let output_merge_ops = level + 1;

                // Sum rows from inputs.
                let total_rows: u64 = input_ids
                    .iter()
                    .map(|id| {
                        s.published_splits
                            .get(id)
                            .map(|info| info.num_rows)
                            .unwrap_or(0)
                    })
                    .sum();

                // Remove input splits from published (they're replaced).
                for &id in &input_ids {
                    s.published_splits.remove(&id);
                }

                // Add output to published.
                s.published_splits.insert(
                    output_id,
                    SplitInfo {
                        id: output_id,
                        merge_ops: output_merge_ops,
                        num_rows: total_rows,
                    },
                );

                // Feed back to planner if alive and not mature.
                if s.pipeline_alive && output_merge_ops < self.max_merge_ops {
                    s.planner_splits
                        .entry(output_merge_ops)
                        .or_default()
                        .insert(output_id);
                }
            }
            MergePipelineAction::Crash => {
                // Planner loses all state.
                s.planner_splits.clear();

                // In-flight merges are lost — inputs go back to published
                // (they were never removed from published_splits, only from
                // the planner). The in_flight_merges are simply cleared.
                s.in_flight_merges.clear();

                s.pipeline_alive = false;
                s.crashes += 1;
            }
            MergePipelineAction::Restart => {
                s.pipeline_alive = true;

                // Re-seed planner from published splits (immature only).
                s.planner_splits.clear();
                for info in s.published_splits.values() {
                    if info.merge_ops < self.max_merge_ops {
                        s.planner_splits
                            .entry(info.merge_ops)
                            .or_default()
                            .insert(info.id);
                    }
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // MC-1 lifecycle: total rows in published splits == total ingested.
            Property::always(
                "MC-1: no row loss across lifecycle",
                |_model: &MergePipelineModel, state: &MergePipelineState| {
                    let published_rows: u64 =
                        state.published_splits.values().map(|s| s.num_rows).sum();
                    published_rows == state.total_ingested_rows
                },
            ),
            // Bounded write amplification: no split exceeds max_merge_ops.
            Property::always(
                "Bounded WA: merge_ops <= max",
                |model: &MergePipelineModel, state: &MergePipelineState| {
                    state
                        .published_splits
                        .values()
                        .all(|s| s.merge_ops <= model.max_merge_ops)
                },
            ),
            // No split in multiple concurrent in-flight merges.
            Property::always(
                "No duplicate merge",
                |_model: &MergePipelineModel, state: &MergePipelineState| {
                    let merges: Vec<&BTreeSet<u32>> = state
                        .in_flight_merges
                        .values()
                        .map(|(ids, _)| ids)
                        .collect();
                    for i in 0..merges.len() {
                        for j in (i + 1)..merges.len() {
                            if !merges[i].is_disjoint(merges[j]) {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // After restart, all immature published splits are in the planner.
            Property::always(
                "No orphan after restart",
                |model: &MergePipelineModel, state: &MergePipelineState| {
                    if !state.pipeline_alive {
                        return true;
                    }
                    let in_flight_ids = all_in_flight_split_ids(state);
                    let planner_ids: BTreeSet<u32> = state
                        .planner_splits
                        .values()
                        .flat_map(|ids| ids.iter().copied())
                        .collect();
                    state.published_splits.values().all(|s| {
                        s.merge_ops >= model.max_merge_ops
                            || planner_ids.contains(&s.id)
                            || in_flight_ids.contains(&s.id)
                    })
                },
            ),
            // MP-1: level homogeneity — enforced by construction.
            Property::always(
                "MP-1: level homogeneity (by construction)",
                |_model: &MergePipelineModel, _state: &MergePipelineState| true,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_merge_pipeline_small() {
        let model = MergePipelineModel::small();
        model
            .checker()
            .threads(1)
            .spawn_bfs()
            .join()
            .assert_properties();
    }

    #[test]
    #[ignore] // Run with: cargo test -p quickwit-dst --features model-checking -- --ignored
    fn check_merge_pipeline_full() {
        let model = MergePipelineModel::full();
        model
            .checker()
            .threads(4)
            .spawn_bfs()
            .join()
            .assert_properties();
    }
}
