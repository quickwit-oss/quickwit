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
//! This model is the Rust counterpart to `MergePipelineShutdown.tla`. It
//! shares state types and predicates *literally* with the production-side
//! invariants in [`crate::invariants::merge_pipeline`] — the model and the
//! production checks evaluate the **same** Rust functions on the **same**
//! state struct, so drift between the model and runtime invariants is
//! impossible.
//!
//! # Action set (mirrors `MergePipelineShutdown.tla` Next)
//!
//! - [`IngestSplit`](MergePipelineAction::IngestSplit)
//! - [`PlanMerge`](MergePipelineAction::PlanMerge)
//! - [`UploadMergeOutput`](MergePipelineAction::UploadMergeOutput) — phase 1 of merge completion. A
//!   Crash here orphans the upload.
//! - [`PublishMergeAndFeedback`](MergePipelineAction::PublishMergeAndFeedback) — phase 2: atomic
//!   metastore replace + (optional) planner feedback.
//! - [`DisconnectMergePlanner`](MergePipelineAction::DisconnectMergePlanner)
//! - [`RunFinalizeAndQuit`](MergePipelineAction::RunFinalizeAndQuit)
//! - [`DrainComplete`](MergePipelineAction::DrainComplete)
//! - [`Crash`](MergePipelineAction::Crash) — bounded by `max_crashes`.
//! - [`Restart`](MergePipelineAction::Restart) — bounded by `max_restarts`. Re-seeds the planner
//!   from durable `published_splits` (models ParquetMergePipeline::fetch_immature_splits) and
//!   resets shutdown state, modelling a fresh process invocation.

use std::collections::BTreeSet;

use stateright::*;

use crate::invariants::merge_pipeline::{
    InFlightMerge, Level, MergeId, MergePipelineState, SplitId, SplitInfo, Window,
    bounded_write_amp, leak_is_object_store_only, mp1_level_homogeneity, no_duplicate_merge,
    no_orphan_in_planner, no_orphan_when_connected, no_split_loss, restart_re_seeds_all_immature,
    rows_conserved,
};

/// Actions in the merge pipeline lifecycle. Each variant maps 1:1 to a TLA+
/// action in `MergePipelineShutdown.tla`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MergePipelineAction {
    IngestSplit {
        window: Window,
        num_rows: u64,
    },
    PlanMerge {
        window: Window,
        level: Level,
        split_ids: BTreeSet<SplitId>,
    },
    UploadMergeOutput {
        merge_id: MergeId,
    },
    PublishMergeAndFeedback {
        merge_id: MergeId,
    },
    DisconnectMergePlanner,
    RunFinalizeAndQuit,
    DrainComplete,
    Crash,
    Restart,
}

/// Configuration for a [`MergePipelineModel`] run. Mirrors the TLA+
/// CONSTANTS block.
#[derive(Clone, Debug)]
pub struct MergePipelineModel {
    pub merge_factor: usize,
    pub max_merge_ops: Level,
    pub max_merges: usize,
    pub max_finalize_ops: u32,
    pub max_ingests: u32,
    pub max_crashes: u32,
    pub max_restarts: u32,
    /// Window IDs the model uses. The TLA+ spec models Windows = {w1, w2};
    /// here we use a small u32 set.
    pub windows: Vec<Window>,
    /// Row counts the planner is allowed to ingest. Multiple values let
    /// the model exercise both single-row and multi-row splits.
    pub ingest_row_counts: Vec<u64>,
}

impl MergePipelineModel {
    /// Multi-lifetime focus — mirrors `MergePipelineShutdown.cfg`.
    pub fn multi_lifetime() -> Self {
        MergePipelineModel {
            merge_factor: 2,
            max_merge_ops: 2,
            max_merges: 2,
            max_finalize_ops: 1,
            max_ingests: 3,
            max_crashes: 1,
            max_restarts: 2,
            windows: vec![1, 2],
            ingest_row_counts: vec![1],
        }
    }

    /// Deep merge-chain focus — mirrors `MergePipelineShutdown_chains.cfg`.
    pub fn deep_chains() -> Self {
        MergePipelineModel {
            merge_factor: 2,
            max_merge_ops: 2,
            max_merges: 2,
            max_finalize_ops: 1,
            max_ingests: 4,
            max_crashes: 1,
            max_restarts: 1,
            windows: vec![1, 2],
            ingest_row_counts: vec![1],
        }
    }

    /// Smallest config for fast iteration during development.
    pub fn small() -> Self {
        MergePipelineModel {
            merge_factor: 2,
            max_merge_ops: 2,
            max_merges: 1,
            max_finalize_ops: 1,
            max_ingests: 2,
            max_crashes: 1,
            max_restarts: 1,
            windows: vec![1],
            ingest_row_counts: vec![1],
        }
    }
}

// ---------------------------------------------------------------------------
// Action enabling helpers
// ---------------------------------------------------------------------------

fn splits_at_level_in_window(
    state: &MergePipelineState,
    window: Window,
    level: Level,
) -> BTreeSet<SplitId> {
    state
        .cold_windows
        .get(&window)
        .map(|set| {
            set.iter()
                .copied()
                .filter(|id| {
                    state
                        .splits
                        .get(id)
                        .map(|info| info.merge_ops == level)
                        .unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Stateright Model implementation
// ---------------------------------------------------------------------------

impl Model for MergePipelineModel {
    type State = MergePipelineState;
    type Action = MergePipelineAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![MergePipelineState::initial()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // IngestSplit — only when alive and under the ingest budget.
        if state.planner_alive && state.ingests_performed < self.max_ingests {
            for &window in &self.windows {
                for &num_rows in &self.ingest_row_counts {
                    actions.push(MergePipelineAction::IngestSplit { window, num_rows });
                }
            }
        }

        // PlanMerge — for each window with enough same-level splits, and
        // only before finalize has been requested.
        if state.planner_alive
            && !state.finalize_requested
            && state.in_flight_merges.len() < self.max_merges
        {
            for &window in &self.windows {
                for level in 0..self.max_merge_ops {
                    let candidates = splits_at_level_in_window(state, window, level);
                    if candidates.len() >= self.merge_factor {
                        // Deterministic: pick the smallest-ID `merge_factor` splits.
                        let split_ids: BTreeSet<SplitId> =
                            candidates.iter().take(self.merge_factor).copied().collect();
                        actions.push(MergePipelineAction::PlanMerge {
                            window,
                            level,
                            split_ids,
                        });
                    }
                }
            }
        }

        // UploadMergeOutput — for each non-uploaded in-flight merge.
        for (merge_id, m) in &state.in_flight_merges {
            if !m.uploaded {
                actions.push(MergePipelineAction::UploadMergeOutput {
                    merge_id: *merge_id,
                });
            }
        }

        // PublishMergeAndFeedback — for each uploaded in-flight merge.
        for (merge_id, m) in &state.in_flight_merges {
            if m.uploaded {
                actions.push(MergePipelineAction::PublishMergeAndFeedback {
                    merge_id: *merge_id,
                });
            }
        }

        // DisconnectMergePlanner — graceful shutdown phase 1.
        if state.planner_connected && state.planner_alive {
            actions.push(MergePipelineAction::DisconnectMergePlanner);
        }

        // RunFinalizeAndQuit — graceful shutdown phase 2.
        if state.planner_alive && !state.planner_connected && !state.finalize_requested {
            actions.push(MergePipelineAction::RunFinalizeAndQuit);
        }

        // DrainComplete — graceful shutdown completion.
        if !state.planner_alive && state.in_flight_merges.is_empty() && !state.shutdown_complete {
            actions.push(MergePipelineAction::DrainComplete);
        }

        // Crash — adversarial action, bounded.
        if state.planner_alive && state.crashes_performed < self.max_crashes {
            actions.push(MergePipelineAction::Crash);
        }

        // Restart — re-seed from durable state, bounded.
        if !state.planner_alive && state.restarts_performed < self.max_restarts {
            actions.push(MergePipelineAction::Restart);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        match action {
            MergePipelineAction::IngestSplit { window, num_rows } => {
                let id = s.next_id;
                s.next_id += 1;
                s.ingests_performed += 1;
                s.total_ingested_rows += num_rows;
                s.splits.insert(
                    id,
                    SplitInfo {
                        rows: num_rows,
                        merge_ops: 0,
                        window,
                    },
                );
                s.published_splits.insert(id);
                s.cold_windows.entry(window).or_default().insert(id);
            }
            MergePipelineAction::PlanMerge {
                window,
                level,
                split_ids,
            } => {
                let merge_id = s.next_id;
                s.next_id += 1;
                if let Some(set) = s.cold_windows.get_mut(&window) {
                    for id in &split_ids {
                        set.remove(id);
                    }
                    if set.is_empty() {
                        s.cold_windows.remove(&window);
                    }
                }
                s.in_flight_merges.insert(
                    merge_id,
                    InFlightMerge {
                        id: merge_id,
                        inputs: split_ids,
                        level,
                        window,
                        uploaded: false,
                        output_id: None,
                    },
                );
            }
            MergePipelineAction::UploadMergeOutput { merge_id } => {
                let m = s.in_flight_merges.get_mut(&merge_id)?;
                if m.uploaded {
                    return None;
                }
                let out = s.next_id;
                s.next_id += 1;
                let total_rows: u64 = m
                    .inputs
                    .iter()
                    .filter_map(|id| s.splits.get(id).map(|info| info.rows))
                    .sum();
                let output_window = m.window;
                let output_level = m.level + 1;
                m.uploaded = true;
                m.output_id = Some(out);
                s.splits.insert(
                    out,
                    SplitInfo {
                        rows: total_rows,
                        merge_ops: output_level,
                        window: output_window,
                    },
                );
            }
            MergePipelineAction::PublishMergeAndFeedback { merge_id } => {
                let m = s.in_flight_merges.remove(&merge_id)?;
                if !m.uploaded {
                    return None;
                }
                let out = m.output_id?;
                // Atomic metastore replace: remove inputs, add output.
                for id in &m.inputs {
                    s.published_splits.remove(id);
                }
                s.published_splits.insert(out);
                // Feed back to planner if connected, alive, and immature.
                let output_immature = s
                    .splits
                    .get(&out)
                    .map(|info| info.merge_ops < self.max_merge_ops)
                    .unwrap_or(false);
                if s.planner_connected && s.planner_alive && output_immature {
                    s.cold_windows.entry(m.window).or_default().insert(out);
                }
            }
            MergePipelineAction::DisconnectMergePlanner => {
                if !s.planner_connected || !s.planner_alive {
                    return None;
                }
                s.planner_connected = false;
            }
            MergePipelineAction::RunFinalizeAndQuit => {
                if !s.planner_alive || s.planner_connected || s.finalize_requested {
                    return None;
                }
                s.finalize_requested = true;
                // Eligible windows: 2+ splits but fewer than merge_factor.
                let eligible: Vec<Window> = self
                    .windows
                    .iter()
                    .copied()
                    .filter(|w| {
                        let n = s.cold_windows.get(w).map(|set| set.len()).unwrap_or(0);
                        n >= 2 && n < self.merge_factor
                    })
                    .collect();
                let to_finalize: Vec<Window> = eligible
                    .into_iter()
                    .take(self.max_finalize_ops as usize)
                    .collect();
                s.finalize_ops_emitted = to_finalize.len() as u32;
                for w in &to_finalize {
                    let inputs = s.cold_windows.remove(w).unwrap_or_default();
                    if inputs.is_empty() {
                        continue;
                    }
                    let merge_id = s.next_id;
                    s.next_id += 1;
                    s.in_flight_merges.insert(
                        merge_id,
                        InFlightMerge {
                            id: merge_id,
                            inputs,
                            level: 0,
                            window: *w,
                            uploaded: false,
                            output_id: None,
                        },
                    );
                }
                s.planner_alive = false;
            }
            MergePipelineAction::DrainComplete => {
                if s.planner_alive || !s.in_flight_merges.is_empty() || s.shutdown_complete {
                    return None;
                }
                s.shutdown_complete = true;
            }
            MergePipelineAction::Crash => {
                if !s.planner_alive || s.crashes_performed >= self.max_crashes {
                    return None;
                }
                // Uploaded outputs become orphans.
                let new_orphans: Vec<SplitId> = s
                    .in_flight_merges
                    .values()
                    .filter(|m| m.uploaded)
                    .filter_map(|m| m.output_id)
                    .collect();
                s.orphan_outputs.extend(new_orphans);
                s.in_flight_merges.clear();
                s.cold_windows.clear();
                s.planner_alive = false;
                s.planner_connected = false;
                s.finalize_requested = false;
                s.finalize_ops_emitted = 0;
                s.crashes_performed += 1;
            }
            MergePipelineAction::Restart => {
                if s.planner_alive || s.restarts_performed >= self.max_restarts {
                    return None;
                }
                s.planner_alive = true;
                s.planner_connected = true;
                s.in_flight_merges.clear();
                // Re-seed cold_windows from durable published_splits, only
                // immature ones (mature splits never re-enter compaction).
                s.cold_windows.clear();
                let max_ops = self.max_merge_ops;
                let immature: Vec<(SplitId, Window)> = s
                    .published_splits
                    .iter()
                    .filter_map(|id| {
                        s.splits.get(id).and_then(|info| {
                            if info.merge_ops < max_ops {
                                Some((*id, info.window))
                            } else {
                                None
                            }
                        })
                    })
                    .collect();
                for (id, window) in immature {
                    s.cold_windows.entry(window).or_default().insert(id);
                }
                // New process lifetime — finalize and shutdown state reset.
                s.finalize_requested = false;
                s.finalize_ops_emitted = 0;
                s.shutdown_complete = false;
                s.restarts_performed += 1;

                // Sanity: post-Restart, every immature split must be in
                // cold_windows. This is the MP-11 action property —
                // verified by construction here.
                debug_assert!(
                    restart_re_seeds_all_immature(&s, self.max_merge_ops),
                    "Restart did not re-seed all immature splits — model bug"
                );
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("MP-1: level homogeneity", |_, s| mp1_level_homogeneity(s)),
            Property::always("MP-4: rows conserved", |_, s| rows_conserved(s)),
            Property::always("MP-5: bounded write amplification", |model, s| {
                bounded_write_amp(s, model.max_merge_ops)
            }),
            Property::always("MP-6: no split loss in flight", |_, s| no_split_loss(s)),
            Property::always("MP-7: no duplicate merge", |_, s| no_duplicate_merge(s)),
            Property::always("MP-8: no orphan in planner", |_, s| no_orphan_in_planner(s)),
            Property::always("MP-9: no orphan when connected", |model, s| {
                no_orphan_when_connected(s, model.max_merge_ops)
            }),
            Property::always("MP-10: leak is object-store-only", |_, s| {
                leak_is_object_store_only(s)
            }),
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
    fn check_merge_pipeline_multi_lifetime() {
        let model = MergePipelineModel::multi_lifetime();
        model
            .checker()
            .threads(4)
            .spawn_bfs()
            .join()
            .assert_properties();
    }

    #[test]
    #[ignore] // Run with: cargo test -p quickwit-dst --features model-checking -- --ignored
    fn check_merge_pipeline_deep_chains() {
        let model = MergePipelineModel::deep_chains();
        model
            .checker()
            .threads(4)
            .spawn_bfs()
            .join()
            .assert_properties();
    }

    /// MP-11 (action property): every Restart transition lands in a state
    /// where every immature published split is in cold_windows. Checked by
    /// running the model and asserting the post-Restart predicate holds
    /// at every state where the previous action was Restart.
    #[test]
    fn restart_action_property_mp11() {
        // The Restart implementation in `next_state` already
        // `debug_assert!`s `restart_re_seeds_all_immature`. Run a small
        // model to exercise that path.
        let model = MergePipelineModel::small();
        model
            .checker()
            .threads(1)
            .spawn_bfs()
            .join()
            .assert_properties();
    }
}
