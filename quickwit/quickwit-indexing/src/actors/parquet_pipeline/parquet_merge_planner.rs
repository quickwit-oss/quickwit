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

//! Parquet merge planner actor.
//!
//! Receives [`ParquetNewSplits`] notifications, groups splits by
//! [`CompactionScope`], invokes [`ParquetMergePolicy::operations()`], and
//! dispatches merge tasks to the [`MergeSchedulerService`].
//!
//! Follows the same pattern as the Tantivy `MergePlanner` but uses
//! Parquet-specific types:
//!
//! - `CompactionScope` instead of `MergePartition`
//! - `ParquetMergePolicy` instead of `MergePolicy`
//! - `ParquetMergeOperation` instead of `MergeOperation`

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_dst::check_invariant;
use quickwit_dst::events::merge_pipeline::{MergePipelineEvent, record_merge_pipeline_event};
use quickwit_dst::invariants::InvariantId;
use quickwit_parquet_engine::merge::policy::{
    CompactionScope, ParquetMergeOperation, ParquetMergePolicy, ParquetSplitMaturity,
};
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use tantivy::Inventory;
use tracing::{info, warn};

use super::{ParquetMergeSplitDownloader, ParquetNewSplits};
use crate::actors::MergeSchedulerService;
use crate::actors::merge_scheduler_service::schedule_parquet_merge;

/// Signal the planner to run `finalize_operations()` for cold windows and
/// then exit.
///
/// Sent by `ParquetMergePipeline` during graceful shutdown to ensure
/// cold-window splits get a final merge before the pipeline stops.
#[derive(Debug)]
pub struct RunFinalizeMergePolicyAndQuit;

/// Internal message to trigger merge planning after initialization.
#[derive(Debug)]
struct PlanParquetMerge {
    incarnation_started_at: Instant,
}

/// Parquet merge planner: decides when to start merge operations.
///
/// Receives split notifications, groups them by [`CompactionScope`], applies
/// the merge policy, and dispatches merge operations to the scheduler.
///
/// Grouping by `CompactionScope` ensures splits from different time windows,
/// partitions, or sort schemas are never merged together — doing so would
/// violate temporal pruning (TW-3) or sort order (MC-3) guarantees.
///
/// Follows the same pattern as the Tantivy `MergePlanner` but with
/// `CompactionScope` instead of `MergePartition` and Parquet-specific types.
pub struct ParquetMergePlanner {
    /// Splits grouped by compaction scope that are eligible for merging.
    scoped_young_splits: HashMap<CompactionScope, Vec<ParquetSplitMetadata>>,

    /// Tracks known split IDs to deduplicate `ParquetNewSplits` messages.
    ///
    /// The publisher's feedback loop can re-send splits the planner already
    /// knows about (e.g., after a mailbox recycle during pipeline restart).
    /// Without deduplication, the same split would be counted twice and
    /// could trigger incorrect merge planning. Periodically rebuilt from
    /// `scoped_young_splits` + `ongoing_merge_operations_inventory` to
    /// avoid unbounded growth.
    known_split_ids: HashSet<String>,
    known_split_ids_recompute_attempt_id: usize,

    merge_policy: Arc<dyn ParquetMergePolicy>,

    merge_split_downloader_mailbox: Mailbox<ParquetMergeSplitDownloader>,
    merge_scheduler_service: Mailbox<MergeSchedulerService>,

    /// Inventory of ongoing merge operations. Tracked objects are dropped
    /// after the merged split is published, which lets us distinguish
    /// "in-flight" splits from stale IDs during known_split_ids rebuild.
    ongoing_merge_operations_inventory: Inventory<ParquetMergeOperation>,

    /// Incarnation timestamp — ignores stale `PlanParquetMerge` messages
    /// from a previous planner instance when the mailbox is recycled during
    /// pipeline restart. Without this, old messages would trigger premature
    /// merge planning before the new planner has a consolidated view of
    /// published splits. See Tantivy MergePlanner #3847.
    incarnation_started_at: Instant,
}

#[async_trait]
impl Actor for ParquetMergePlanner {
    type ObservableState = ();

    fn observable_state(&self) {}

    fn name(&self) -> String {
        "ParquetMergePlanner".to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        // Same capacity as Tantivy planner — low to avoid stale backlogs.
        QueueCapacity::Bounded(1)
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        // Queue a PlanParquetMerge to consolidate any splits from a recycled
        // mailbox before planning. See Tantivy MergePlanner #3847.
        let _ = ctx.try_send_self_message(PlanParquetMerge {
            incarnation_started_at: self.incarnation_started_at,
        });
        Ok(())
    }
}

#[async_trait]
impl Handler<ParquetNewSplits> for ParquetMergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        new_splits: ParquetNewSplits,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.record_splits_if_necessary(new_splits.new_splits);
        self.send_merge_ops(false, ctx).await?;
        self.recompute_known_splits_if_necessary();
        Ok(())
    }
}

#[async_trait]
impl Handler<RunFinalizeMergePolicyAndQuit> for ParquetMergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: RunFinalizeMergePolicyAndQuit,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.send_merge_ops(true, ctx).await?;
        Err(ActorExitStatus::Success)
    }
}

#[async_trait]
impl Handler<PlanParquetMerge> for ParquetMergePlanner {
    type Reply = ();

    async fn handle(
        &mut self,
        plan_merge: PlanParquetMerge,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if plan_merge.incarnation_started_at == self.incarnation_started_at {
            self.send_merge_ops(false, ctx).await?;
        }
        self.recompute_known_splits_if_necessary();
        Ok(())
    }
}

impl ParquetMergePlanner {
    pub fn new(
        immature_splits: Vec<ParquetSplitMetadata>,
        merge_policy: Arc<dyn ParquetMergePolicy>,
        merge_split_downloader_mailbox: Mailbox<ParquetMergeSplitDownloader>,
        merge_scheduler_service: Mailbox<MergeSchedulerService>,
    ) -> Self {
        // MP-11 (RestartReSeedsAllImmature): every split the metastore
        // reports as still-immature should land in scoped_young_splits,
        // unless filtered by the merge policy as mature/expired or by being
        // pre-Phase-31. We compute the expected count, then verify the
        // recorded count matches after seeding.
        let now = std::time::SystemTime::now();
        let expected_immature: Vec<ParquetSplitMetadata> = immature_splits
            .iter()
            .filter(|split| {
                match merge_policy.split_maturity(split.size_bytes, split.num_merge_ops) {
                    ParquetSplitMaturity::Mature => false,
                    ParquetSplitMaturity::Immature {
                        maturation_period, ..
                    } => split.created_at + maturation_period > now,
                }
            })
            .filter(|split| CompactionScope::from_split(split).is_some())
            .cloned()
            .collect();
        let expected_immature_ids: HashSet<String> = expected_immature
            .iter()
            .map(|s| s.split_id.as_str().to_string())
            .collect();

        let mut planner = Self {
            scoped_young_splits: HashMap::new(),
            known_split_ids: HashSet::new(),
            known_split_ids_recompute_attempt_id: 0,
            merge_policy,
            merge_split_downloader_mailbox,
            merge_scheduler_service,
            ongoing_merge_operations_inventory: Inventory::default(),
            incarnation_started_at: Instant::now(),
        };
        planner.record_splits_if_necessary(immature_splits);

        // After re-seeding: every expected-immature split must be
        // acknowledged (in known_split_ids) AND placed in a scope bucket.
        // Anything missing means a split fell through the cracks during
        // recovery — exactly the failure mode MP-11 protects against.
        let recorded_in_scope: HashSet<String> = planner
            .scoped_young_splits
            .values()
            .flatten()
            .map(|s| s.split_id.as_str().to_string())
            .collect();
        let all_recorded = expected_immature_ids
            .iter()
            .all(|id| planner.known_split_ids.contains(id) && recorded_in_scope.contains(id));
        check_invariant!(
            InvariantId::MP11,
            all_recorded,
            ": planner re-seed dropped immature splits (expected={}, recorded_in_scope={})",
            expected_immature_ids.len(),
            recorded_in_scope.len()
        );

        planner
    }

    /// Filters and records incoming splits, skipping:
    /// - Mature splits (already at max merge ops or target size)
    /// - Time-matured splits (created_at + maturation_period has elapsed)
    /// - Splits we've already seen (dedup via `known_split_ids`)
    /// - Pre-Phase-31 splits without a window (can't participate in compaction)
    ///
    /// TODO(CS-3): when `ParquetMergePolicyConfig` grows a
    /// `compaction_start_time: Option<i64>` field (and the merge policy
    /// exposes it), filter here on `split.window.start >= compaction_start_time`
    /// and add a `check_invariant!(InvariantId::CS3, ...)` over
    /// `scoped_young_splits` to verify no pre-threshold split slipped
    /// through. The TLA+ model in `time_windowed_compaction.rs` and the
    /// `CS-3` registry entry already define the invariant; production
    /// just lacks the configurable threshold today.
    fn record_splits_if_necessary(&mut self, splits: Vec<ParquetSplitMetadata>) {
        let now = std::time::SystemTime::now();
        for split in splits {
            match self
                .merge_policy
                .split_maturity(split.size_bytes, split.num_merge_ops)
            {
                ParquetSplitMaturity::Mature => continue,
                ParquetSplitMaturity::Immature {
                    maturation_period, ..
                } => {
                    // A split that has lived past its maturation period is
                    // effectively mature — no further merges needed. This
                    // mirrors the Tantivy merge planner's `is_mature(now)`.
                    if split.created_at + maturation_period <= now {
                        continue;
                    }
                }
            }
            if !self.acknowledge_split(split.split_id.as_str()) {
                continue;
            }
            self.record_split(split);
        }
    }

    /// Returns `true` if this split ID was not previously known, and records
    /// it. Returns `false` (and does nothing) if we've already seen it.
    fn acknowledge_split(&mut self, split_id: &str) -> bool {
        if self.known_split_ids.contains(split_id) {
            return false;
        }
        self.known_split_ids.insert(split_id.to_string());
        true
    }

    /// Places a split into the appropriate compaction scope bucket.
    fn record_split(&mut self, split: ParquetSplitMetadata) {
        let Some(scope) = CompactionScope::from_split(&split) else {
            // Pre-Phase-31 splits have no window — can't compact them.
            return;
        };
        self.scoped_young_splits
            .entry(scope)
            .or_default()
            .push(split);
    }

    /// Amortized GC for `known_split_ids`.
    ///
    /// The set grows monotonically (we add IDs but never remove inline).
    /// Every 100 calls, we rebuild from only the splits that still matter
    /// (young splits + in-flight merges), shrinking the set back down.
    /// Same pattern as Tantivy's `MergePlanner`.
    fn recompute_known_splits_if_necessary(&mut self) {
        self.known_split_ids_recompute_attempt_id += 1;
        if self
            .known_split_ids_recompute_attempt_id
            .is_multiple_of(100)
        {
            self.known_split_ids = self.rebuild_known_split_ids();
            self.known_split_ids_recompute_attempt_id = 0;
        }
    }

    /// Rebuilds `known_split_ids` from current state: young splits still
    /// waiting for merge + splits currently in-flight in merge operations.
    fn rebuild_known_split_ids(&self) -> HashSet<String> {
        let mut known = HashSet::new();
        for splits in self.scoped_young_splits.values() {
            for split in splits {
                known.insert(split.split_id.as_str().to_string());
            }
        }
        for op in self.ongoing_merge_operations_inventory.list() {
            for split in &op.splits {
                known.insert(split.split_id.as_str().to_string());
            }
        }
        // If rebuild didn't shrink the set by at least half, something may be
        // leaking IDs (splits not being dropped from the inventory).
        if known.len() * 2 >= self.known_split_ids.len() {
            warn!(
                known_split_ids_len_after = known.len(),
                known_split_ids_len_before = self.known_split_ids.len(),
                "rebuilding known_split_ids did not halve the set — potential leak"
            );
        }
        known
    }

    /// Asks the merge policy which splits should be merged, per scope.
    ///
    /// `operations()` drains participating splits from each scope's vec;
    /// splits not selected remain for future rounds. `finalize_operations()`
    /// uses a lower merge factor for cold-window cleanup at shutdown.
    async fn compute_merge_ops(
        &mut self,
        is_finalize: bool,
        ctx: &ActorContext<Self>,
    ) -> Result<Vec<ParquetMergeOperation>, ActorExitStatus> {
        let mut merge_operations = Vec::new();
        for young_splits in self.scoped_young_splits.values_mut() {
            if !young_splits.is_empty() {
                let operations = if is_finalize {
                    self.merge_policy.finalize_operations(young_splits)
                } else {
                    self.merge_policy.operations(young_splits)
                };
                merge_operations.extend(operations);
            }
            ctx.record_progress();
            ctx.yield_now().await;
        }
        self.scoped_young_splits
            .retain(|_, splits| !splits.is_empty());
        Ok(merge_operations)
    }

    /// Computes merge operations and dispatches each to the scheduler.
    ///
    /// Each operation is tracked in the inventory so we know which splits are
    /// in-flight (for known_split_ids rebuild and for the pipeline to observe).
    async fn send_merge_ops(
        &mut self,
        is_finalize: bool,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let merge_ops = self.compute_merge_ops(is_finalize, ctx).await?;
        for merge_operation in merge_ops {
            info!(
                merge_split_id = %merge_operation.merge_split_id,
                num_inputs = merge_operation.splits.len(),
                total_bytes = merge_operation.total_size_bytes(),
                "scheduling parquet merge operation"
            );

            // Trace event: a merge operation is being dispatched. The
            // index_uid + window are taken from the first input (all
            // inputs share scope by MP-3).
            if let Some(first) = merge_operation.splits.first() {
                let level = first.num_merge_ops;
                let window = first.window.clone().unwrap_or(
                    first.time_range.start_secs as i64..first.time_range.end_secs as i64,
                );
                record_merge_pipeline_event(&MergePipelineEvent::PlanMerge {
                    index_uid: first.index_uid.clone(),
                    merge_id: merge_operation.merge_split_id.as_str().to_string(),
                    input_split_ids: merge_operation
                        .splits
                        .iter()
                        .map(|s| s.split_id.as_str().to_string())
                        .collect(),
                    level,
                    window,
                });
            }

            let tracked = self
                .ongoing_merge_operations_inventory
                .track(merge_operation);
            schedule_parquet_merge(
                &self.merge_scheduler_service,
                tracked,
                self.merge_split_downloader_mailbox.clone(),
            )
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_parquet_engine::merge::policy::{
        ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig,
    };
    use quickwit_parquet_engine::split::{ParquetSplitId, ParquetSplitMetadata, TimeRange};

    use super::*;
    use crate::actors::parquet_pipeline::ParquetMergeTask;

    fn make_split(split_id: &str, size_bytes: u64, num_merge_ops: u32) -> ParquetSplitMetadata {
        ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new(split_id))
            .index_uid("test-index:00000000000000000000000001")
            .partition_id(0)
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(size_bytes)
            .sort_fields("metric_name|host|timestamp_secs/V2")
            .window_start_secs(0)
            .window_duration_secs(3600)
            .num_merge_ops(num_merge_ops)
            .build()
    }

    fn make_policy(merge_factor: usize) -> Arc<dyn ParquetMergePolicy> {
        Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
            ParquetMergePolicyConfig {
                merge_factor,
                max_merge_factor: merge_factor,
                max_merge_ops: 5,
                target_split_size_bytes: 256 * 1024 * 1024,
                maturation_period: Duration::from_secs(3600),
                max_finalize_merge_operations: 3,
            },
        ))
    }

    #[tokio::test]
    async fn test_planner_plans_merge_when_enough_splits() {
        let universe = Universe::with_accelerated_time();
        let (downloader_mailbox, downloader_inbox) = universe.create_test_mailbox();
        let policy = make_policy(3);

        let planner = ParquetMergePlanner::new(
            Vec::new(),
            policy,
            downloader_mailbox,
            universe.get_or_spawn_one(),
        );
        let (planner_mailbox, planner_handle) = universe.spawn_builder().spawn(planner);

        // Send 2 splits — not enough for merge_factor=3.
        planner_mailbox
            .send_message(ParquetNewSplits {
                new_splits: vec![
                    make_split("s0", 1_000_000, 0),
                    make_split("s1", 1_000_000, 0),
                ],
            })
            .await
            .unwrap();
        planner_handle.process_pending_and_observe().await;
        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert!(
            tasks.is_empty(),
            "should not merge with < merge_factor splits"
        );

        // Send 1 more — now we have 3 splits at level 0, should trigger merge.
        planner_mailbox
            .send_message(ParquetNewSplits {
                new_splits: vec![make_split("s2", 1_000_000, 0)],
            })
            .await
            .unwrap();
        planner_handle.process_pending_and_observe().await;
        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].merge_operation.splits.len(), 3);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_planner_deduplicates_known_splits() {
        let universe = Universe::with_accelerated_time();
        let (downloader_mailbox, downloader_inbox) = universe.create_test_mailbox();
        let policy = make_policy(2);

        let planner = ParquetMergePlanner::new(
            Vec::new(),
            policy,
            downloader_mailbox,
            universe.get_or_spawn_one(),
        );
        let (planner_mailbox, planner_handle) = universe.spawn_builder().spawn(planner);

        // Send 2 splits — should trigger a merge.
        planner_mailbox
            .send_message(ParquetNewSplits {
                new_splits: vec![
                    make_split("s0", 1_000_000, 0),
                    make_split("s1", 1_000_000, 0),
                ],
            })
            .await
            .unwrap();
        planner_handle.process_pending_and_observe().await;
        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert_eq!(tasks.len(), 1);

        // Re-send the same splits — should be deduped, no new merge.
        planner_mailbox
            .send_message(ParquetNewSplits {
                new_splits: vec![
                    make_split("s0", 1_000_000, 0),
                    make_split("s1", 1_000_000, 0),
                ],
            })
            .await
            .unwrap();
        planner_handle.process_pending_and_observe().await;
        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert!(tasks.is_empty(), "duplicate splits should be deduped");

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_planner_seeds_immature_splits_on_start() {
        let universe = Universe::with_accelerated_time();
        let (downloader_mailbox, downloader_inbox) = universe
            .spawn_ctx()
            .create_mailbox("downloader", QueueCapacity::Bounded(2));
        let policy = make_policy(2);

        // Seed with 2 immature splits — should trigger merge on startup.
        let immature = vec![
            make_split("seed-0", 1_000_000, 0),
            make_split("seed-1", 1_000_000, 0),
        ];
        let planner = ParquetMergePlanner::new(
            immature,
            policy,
            downloader_mailbox,
            universe.get_or_spawn_one(),
        );
        let (_planner_mailbox, _planner_handle) = universe.spawn_builder().spawn(planner);

        // Wait for the initial PlanParquetMerge to be processed.
        let task_res = downloader_inbox
            .recv_typed_message::<ParquetMergeTask>()
            .await;
        assert!(task_res.is_ok(), "should merge seeded splits on startup");
        assert_eq!(task_res.unwrap().merge_operation.splits.len(), 2);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_planner_skips_mature_splits() {
        let universe = Universe::with_accelerated_time();
        let (downloader_mailbox, downloader_inbox) = universe.create_test_mailbox();
        // Policy with max_merge_ops=2: splits with num_merge_ops >= 2 are mature.
        let policy = Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
            ParquetMergePolicyConfig {
                merge_factor: 2,
                max_merge_factor: 2,
                max_merge_ops: 2,
                target_split_size_bytes: 256 * 1024 * 1024,
                maturation_period: Duration::from_secs(3600),
                max_finalize_merge_operations: 3,
            },
        ));

        let planner = ParquetMergePlanner::new(
            Vec::new(),
            policy,
            downloader_mailbox,
            universe.get_or_spawn_one(),
        );
        let (planner_mailbox, planner_handle) = universe.spawn_builder().spawn(planner);

        // Send 2 splits at num_merge_ops=2 — both mature, should not merge.
        planner_mailbox
            .send_message(ParquetNewSplits {
                new_splits: vec![
                    make_split("mature-0", 1_000_000, 2),
                    make_split("mature-1", 1_000_000, 2),
                ],
            })
            .await
            .unwrap();
        planner_handle.process_pending_and_observe().await;
        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert!(tasks.is_empty(), "mature splits should not be merged");

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_planner_groups_by_scope() {
        let universe = Universe::with_accelerated_time();
        let (downloader_mailbox, downloader_inbox) = universe.create_test_mailbox();
        let policy = make_policy(2);

        let planner = ParquetMergePlanner::new(
            Vec::new(),
            policy,
            downloader_mailbox,
            universe.get_or_spawn_one(),
        );
        let (planner_mailbox, planner_handle) = universe.spawn_builder().spawn(planner);

        // Send 2 splits in window 0 and 1 split in window 3600.
        // Only window 0 should trigger a merge.
        let mut w0_s0 = make_split("w0-s0", 1_000_000, 0);
        w0_s0.window = Some(0..3600);
        let mut w0_s1 = make_split("w0-s1", 1_000_000, 0);
        w0_s1.window = Some(0..3600);
        let mut w1_s0 = make_split("w1-s0", 1_000_000, 0);
        w1_s0.window = Some(3600..7200);

        planner_mailbox
            .send_message(ParquetNewSplits {
                new_splits: vec![w0_s0, w0_s1, w1_s0],
            })
            .await
            .unwrap();
        planner_handle.process_pending_and_observe().await;
        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert_eq!(tasks.len(), 1, "only window 0 has enough splits");
        assert_eq!(tasks[0].merge_operation.splits.len(), 2);

        universe.assert_quit().await;
    }

    // -----------------------------------------------------------------------
    // Property tests
    // -----------------------------------------------------------------------

    /// Generate a split with random maturity-relevant properties.
    fn make_split_with_maturity(
        split_id: &str,
        size_bytes: u64,
        num_merge_ops: u32,
        created_secs_ago: u64,
        has_window: bool,
    ) -> ParquetSplitMetadata {
        let mut split = ParquetSplitMetadata::metrics_builder()
            .split_id(ParquetSplitId::new(split_id))
            .index_uid("test-index:00000000000000000000000001")
            .partition_id(0)
            .time_range(TimeRange::new(1000, 2000))
            .num_rows(100)
            .size_bytes(size_bytes)
            .sort_fields("metric_name|host|timestamp_secs/V2")
            .num_merge_ops(num_merge_ops)
            .build();

        // Override created_at to control time-based maturity.
        split.created_at =
            std::time::SystemTime::now() - std::time::Duration::from_secs(created_secs_ago);

        if has_window {
            split.window = Some(0..3600);
        } else {
            split.window = None;
        }
        split
    }

    proptest::proptest! {
        /// Verify that no merge task ever contains a split that should have
        /// been filtered: mature by ops, mature by size, mature by time, or
        /// missing a window.
        #[test]
        fn proptest_planner_never_merges_mature_or_windowless_splits(
            splits_data in proptest::collection::vec(
                (
                    1u64..300_000_000,   // size_bytes
                    0u32..6,             // num_merge_ops
                    0u64..7200,          // created_secs_ago
                    proptest::bool::ANY, // has_window
                ),
                2..20,
            )
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let universe = Universe::with_accelerated_time();
                let (downloader_mailbox, downloader_inbox) =
                    universe.create_test_mailbox();
                // max_merge_ops=5, target=256MiB, maturation=3600s
                let policy = make_policy(2);

                let splits: Vec<ParquetSplitMetadata> = splits_data
                    .iter()
                    .enumerate()
                    .map(|(i, &(size, ops, age, has_w))| {
                        make_split_with_maturity(
                            &format!("prop-{i}"),
                            size,
                            ops,
                            age,
                            has_w,
                        )
                    })
                    .collect();

                let planner = ParquetMergePlanner::new(
                    splits.clone(),
                    policy.clone(),
                    downloader_mailbox,
                    universe.get_or_spawn_one(),
                );
                let (_planner_mailbox, _planner_handle) =
                    universe.spawn_builder().spawn(planner);

                // Give the planner time to process initial splits and plan.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                let tasks = downloader_inbox
                    .drain_for_test_typed::<ParquetMergeTask>();

                let now = std::time::SystemTime::now();
                for task in &tasks {
                    for split in &task.merge_operation.splits {
                        // No mature-by-ops split.
                        assert!(
                            split.num_merge_ops < 5,
                            "merge task contains ops-mature split: {} has ops={}",
                            split.split_id, split.num_merge_ops
                        );
                        // No mature-by-size split.
                        assert!(
                            split.size_bytes < 256 * 1024 * 1024,
                            "merge task contains size-mature split: {} has size={}",
                            split.split_id, split.size_bytes
                        );
                        // No time-matured split.
                        let age = now
                            .duration_since(split.created_at)
                            .unwrap_or_default();
                        assert!(
                            age < std::time::Duration::from_secs(3600),
                            "merge task contains time-mature split: {} age={:?}",
                            split.split_id, age
                        );
                        // No windowless split.
                        assert!(
                            split.window.is_some(),
                            "merge task contains windowless split: {}",
                            split.split_id
                        );
                    }
                }
                universe.assert_quit().await;
            });
        }
    }

    #[tokio::test]
    async fn test_planner_finalize_and_quit() {
        let universe = Universe::with_accelerated_time();
        let (downloader_mailbox, downloader_inbox) = universe.create_test_mailbox();
        // Finalize should merge even with fewer than merge_factor splits.
        let policy = make_policy(10); // High threshold — operations() won't fire.

        let planner = ParquetMergePlanner::new(
            Vec::new(),
            policy,
            downloader_mailbox,
            universe.get_or_spawn_one(),
        );
        let (planner_mailbox, planner_handle) = universe.spawn_builder().spawn(planner);

        // Send 3 splits — not enough for merge_factor=10.
        planner_mailbox
            .send_message(ParquetNewSplits {
                new_splits: vec![
                    make_split("f0", 1_000_000, 0),
                    make_split("f1", 1_000_000, 0),
                    make_split("f2", 1_000_000, 0),
                ],
            })
            .await
            .unwrap();
        planner_handle.process_pending_and_observe().await;
        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert!(tasks.is_empty(), "normal operations should not fire");

        // Send RunFinalizeMergePolicyAndQuit — should trigger finalize.
        planner_mailbox
            .send_message(RunFinalizeMergePolicyAndQuit)
            .await
            .unwrap();
        let (exit_status, _) = planner_handle.join().await;
        assert!(
            matches!(exit_status, ActorExitStatus::Success),
            "planner should exit with Success after finalize"
        );

        let tasks = downloader_inbox.drain_for_test_typed::<ParquetMergeTask>();
        assert_eq!(tasks.len(), 1, "finalize should merge the 3 splits");

        universe.assert_quit().await;
    }
}
