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

mod change_tracker;
mod scheduling;

use std::cmp::Ordering;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use quickwit_common::is_parquet_pipeline_index;
use quickwit_common::pretty::PrettySample;
use quickwit_config::{
    FileSourceParams, SourceParams, disable_ingest_v1, indexing_pipeline_params_fingerprint,
};
use quickwit_proto::indexing::{
    ApplyIndexingPlanRequest, CpuCapacity, IndexingService, IndexingTask, PIPELINE_FULL_CAPACITY,
    PIPELINE_THROUGHPUT,
};
use quickwit_proto::ingest::ingester::IngesterStatus;
use quickwit_proto::types::NodeId;
use scheduling::{SourceToSchedule, SourceToScheduleType};
use serde::Serialize;
use tracing::{debug, info, warn};

use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::change_tracker::{NotifyChangeOnDrop, RebuildNotifier};
use crate::indexing_scheduler::scheduling::build_physical_indexing_plan;
use crate::metrics::{APPLY_PLAN_TOTAL, SCHEDULE_TOTAL, ShardLocalityMetrics};
use crate::model::{ControlPlaneModel, ShardEntry, ShardLocations};
use crate::{IndexerNodeInfo, IndexerPool};

const DEFAULT_ENABLE_VARIABLE_SHARD_LOAD: bool = false;

pub(crate) const MIN_DURATION_BETWEEN_SCHEDULING: Duration =
    if cfg!(any(test, feature = "testsuite")) {
        Duration::from_millis(50)
    } else {
        Duration::from_secs(30)
    };

pub(crate) const APPLY_INDEXING_PLAN_TIMEOUT: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(2)
};

/// Number of consecutive control-loop cycles in which the running plan keeps differing from the
/// last applied plan (each triggering a reapply of the identical plan) before logging escalates
/// from `info` to `warn`.
///
/// At `MIN_DURATION_BETWEEN_SCHEDULING` of 30s this corresponds to ~2.5 minutes of non-convergence,
/// long enough to exclude the normal transient case where an indexer has not yet reported
/// its freshly applied tasks, while surfacing a persistently stuck loop.
const REAPPLY_LOOP_WARN_THRESHOLD: usize = 5;

/// Number of consecutive reapplies of the identical plan after which the scheduler stops re-sending
/// it and instead rebuilds a fresh plan from the current model.
///
/// Re-sending helps only when an indexer transiently lagged behind. If the plan itself is
/// un-runnable (e.g. it assigns a shard that no longer exists on the indexer after a
/// crash/restart), re-sending can never converge. After this threshold the plan is recomputed from
/// the model so a corrected model produces a runnable plan.
///
/// Set above `REAPPLY_LOOP_WARN_THRESHOLD` so the non-convergence warning fires before the behavior
/// changes. A rebuild is attempted only once every `REAPPLY_LOOP_RESCHEDULE_THRESHOLD` cycles (see
/// `control_running_plan`) to avoid rebuilding every cycle while the model still describes an
/// unreachable state.
const REAPPLY_LOOP_RESCHEDULE_THRESHOLD: usize = 10;

#[derive(Debug, Clone, Default, Serialize)]
pub struct IndexingSchedulerState {
    pub num_applied_physical_indexing_plan: usize,
    pub num_schedule_indexing_plan: usize,
    pub last_applied_physical_plan: Option<PhysicalIndexingPlan>,
    #[serde(skip)]
    pub last_applied_plan_timestamp: Option<Instant>,
    /// Consecutive control-loop cycles that reapplied the identical plan
    /// without the cluster converging. Reset to 0 on convergence or reschedule;
    /// once it reaches `REAPPLY_LOOP_WARN_THRESHOLD`, logging escalates from `info` to `warn`.
    pub num_consecutive_reapplies: usize,
}

/// The [`IndexingScheduler`] is responsible for listing indexing tasks and assigning them to
/// indexers.
/// We call this duty `scheduling`. Contrary to what the name suggests, most indexing tasks are
/// ever running. We just borrowed the terminology to Kubernetes.
///
/// Scheduling executes the following steps:
/// 1. Builds a [`PhysicalIndexingPlan`] from the list of logical indexing tasks. See
///    `build_physical_indexing_plan` for the implementation details.
/// 2. Apply the [`PhysicalIndexingPlan`]: for each indexer, the scheduler send the indexing tasks
///    by gRPC. An indexer immediately returns an Ok and apply asynchronously the received plan. Any
///    errors (network) happening in this step are ignored. The scheduler runs a control loop that
///    regularly checks if indexers are effectively running their plans (more details in the next
///    section).
///
/// All events altering the list of indexes and sources are proxied through
/// through the control plane. The control plane model is therefore guaranteed to be up-to-date
/// (at the cost of making the control plane a single point of failure).
///
/// Each change to the model triggers the production of a new `PhysicalIndexingPlan`.
///
/// A `ControlPlanLoop` event is scheduled every `CONTROL_PLAN_LOOP_INTERVAL` and steers
/// the cluster toward the last applied [`PhysicalIndexingPlan`].
///
/// This physical plan is a desired state. Even after that state is reached, it can be altered due
/// to faulty server for instance.
///
/// We then need to detect deviation, possibly recompute the desired `PhysicalIndexingPlan`
/// and steer back the cluster to the right state.
///
/// First to detect deviation, the control plan gathers an eventually consistent view of what is
/// running on the different nodes of the cluster: the `running plan`. This is done via `chitchat`.
///
/// If the list of node ids has changed, the scheduler will retrigger a scheduling.
/// If the indexing tasks do not match, the scheduler will apply again the last applied plan.
/// Concretely, it will send the faulty nodes of the plan they are supposed to follow.
//
/// Finally, in order to give the time for each indexer to run their indexing tasks, the control
/// plane will wait at least `MIN_DURATION_BETWEEN_SCHEDULING` before comparing the desired
/// plan with the running plan.
pub struct IndexingScheduler {
    cluster_id: String,
    self_node_id: NodeId,
    indexer_pool: IndexerPool,
    state: IndexingSchedulerState,
    pub(crate) next_rebuild_tracker: RebuildNotifier,
}

impl fmt::Debug for IndexingScheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexingScheduler")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.self_node_id)
            .field(
                "last_applied_plan_ts",
                &self.state.last_applied_plan_timestamp,
            )
            .finish()
    }
}

fn enable_variable_shard_load() -> bool {
    static IS_SHARD_LOAD_CP_ENABLED: LazyLock<bool> = LazyLock::new(|| {
        if let Some(enable_flag) =
            quickwit_common::get_bool_from_env_opt("QW_ENABLE_VARIABLE_SHARD_LOAD")
        {
            return enable_flag;
        }
        // For backward compatibility, if QW_DISABLE_VARIABLE_SHARD_LOAD is set, we accept this
        // value too.
        if let Some(disable_flag) =
            quickwit_common::get_bool_from_env_opt("QW_DISABLE_VARIABLE_SHARD_LOAD")
        {
            warn!(
                disable = disable_flag,
                "QW_DISABLE_VARIABLE_SHARD_LOAD is deprecated. Please use \
                 QW_ENABLE_VARIABLE_SHARD_LOAD instead. We will use your setting in this version, \
                 but will likely ignore it in future versions."
            );
            return !disable_flag;
        }
        // Defaulting to false
        info!(
            "QW_ENABLE_VARIABLE_SHARD_LOAD not set, defaulting to {}",
            DEFAULT_ENABLE_VARIABLE_SHARD_LOAD
        );
        DEFAULT_ENABLE_VARIABLE_SHARD_LOAD
    });
    *IS_SHARD_LOAD_CP_ENABLED
}

/// Computes the CPU load associated to a single shard of a given index.
///
/// The array passed contains all of data we have about the shard of the index.
/// This function averages their statistics.
///
/// For the moment, this function only takes in account the measured throughput,
/// and assumes a constant CPU usage of 4 vCPU = 20mb/s.
///
/// It does not take in account the variation that could raise from the different
/// doc mapping / nature of the data, etc.
fn compute_load_per_shard(shard_entries: &[&ShardEntry]) -> NonZeroU32 {
    if enable_variable_shard_load() {
        let num_shards = shard_entries.len().max(1) as u64;
        let average_throughput_per_shard_bytes: u64 = shard_entries
            .iter()
            .map(|shard_entry| shard_entry.long_term_ingestion_rate.0 as u64 * bytesize::MIB)
            .sum::<u64>()
            .div_ceil(num_shards)
            // A shard throughput cannot exceed PIPELINE_THROUGHPUT in the long term (this is
            // enforced by the configuration).
            .min(PIPELINE_THROUGHPUT.as_u64());
        let num_cpu_millis = (PIPELINE_FULL_CAPACITY.cpu_millis() as u64
            * average_throughput_per_shard_bytes)
            / PIPELINE_THROUGHPUT.as_u64();
        const MIN_CPU_LOAD_PER_SHARD: u32 = 50u32;
        NonZeroU32::new((num_cpu_millis as u32).max(MIN_CPU_LOAD_PER_SHARD)).unwrap()
    } else {
        get_default_load_per_shard()
    }
}

fn get_default_load_per_shard() -> NonZeroU32 {
    let default_load_per_shard = quickwit_common::get_from_env_cached!(
        u32,
        "QW_DEFAULT_LOAD_PER_SHARD",
        PIPELINE_FULL_CAPACITY.cpu_millis() / 4,
        false
    );
    NonZeroU32::new(default_load_per_shard).unwrap()
}

fn get_sources_to_schedule(
    model: &ControlPlaneModel,
    disable_ingest_v1: bool,
) -> Vec<SourceToSchedule> {
    if disable_ingest_v1 {
        debug!("skipping scheduling of ingest API sources because ingest v1 is disabled");
    }
    let mut sources = Vec::new();

    for (source_uid, source_config) in model.source_configs() {
        if !source_config.enabled {
            continue;
        }
        let params_fingerprint = model
            .index_metadata(&source_uid.index_uid)
            .map(|index_meta| {
                indexing_pipeline_params_fingerprint(&index_meta.index_config, source_config)
            })
            .unwrap_or_default();
        match source_config.source_params {
            SourceParams::File(FileSourceParams::Filepath(_))
            | SourceParams::IngestCli
            | SourceParams::Stdin
            | SourceParams::Void(_)
            | SourceParams::Vec(_) => { // We don't need to schedule those.
            }

            SourceParams::IngestApi => {
                if disable_ingest_v1 {
                    continue;
                }
                // Metrics indexes should use IngestV2 only, not IngestV1.
                // The ParquetSourceLoader doesn't support IngestV1.
                if is_parquet_pipeline_index(&source_uid.index_uid.index_id) {
                    continue;
                }
                // TODO ingest v1 is scheduled differently
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::IngestV1,
                    params_fingerprint,
                });
            }
            SourceParams::Ingest => {
                // Expect: the source should exist since we just read it from `get_source_configs`.
                // Note that we keep all shards, including Closed shards:
                // A closed shards still needs to be indexed.
                let shard_entries: Vec<&ShardEntry> = model
                    .get_shards_for_source(&source_uid)
                    .expect("source should exist")
                    .values()
                    .collect();
                if shard_entries.is_empty() {
                    continue;
                }
                let shard_ids = shard_entries
                    .iter()
                    .map(|shard_entry| shard_entry.shard_id().clone())
                    .collect();
                let load_per_shard = compute_load_per_shard(&shard_entries[..]);
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::Sharded {
                        shard_ids,
                        load_per_shard,
                    },
                    params_fingerprint,
                });
            }
            SourceParams::Kafka(_)
            | SourceParams::Kinesis(_)
            | SourceParams::PubSub(_)
            | SourceParams::Pulsar(_)
            | SourceParams::File(FileSourceParams::Notifications(_)) => {
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::NonSharded {
                        num_pipelines: source_config.num_pipelines.get() as u32,
                        // FIXME
                        load_per_pipeline: NonZeroU32::new(PIPELINE_FULL_CAPACITY.cpu_millis())
                            .unwrap(),
                    },
                    params_fingerprint,
                });
            }
        }
    }
    sources
}

impl IndexingScheduler {
    pub fn new(cluster_id: String, self_node_id: NodeId, indexer_pool: IndexerPool) -> Self {
        IndexingScheduler {
            cluster_id,
            self_node_id,
            indexer_pool,
            state: IndexingSchedulerState::default(),
            next_rebuild_tracker: RebuildNotifier::default(),
        }
    }

    pub fn observable_state(&self) -> IndexingSchedulerState {
        self.state.clone()
    }

    // Should be called whenever a change in the list of index/shard
    // has happened.
    //
    // Prefer not calling this method directly, and instead call
    // `ControlPlane::rebuild_indexing_plan_debounced`.
    pub(crate) fn rebuild_plan(&mut self, model: &ControlPlaneModel) {
        // The regular rebuild seeds from the last applied plan to keep placement stable.
        self.rebuild_plan_impl(model, None);
    }

    /// Rebuilds the physical plan from the current model.
    ///
    /// `seed_override` controls the plan the placement solver starts from:
    /// - `None` seeds from `last_applied_physical_plan` (the default, keeps placement stable).
    /// - `Some(seed)` seeds from the provided plan instead. The escalation path in
    ///   `control_running_plan` uses this to strip the un-runnable tasks from the seed, so their
    ///   shards are re-placed from the current model (following any relocation) rather than pinned
    ///   to their stale indexer.
    fn rebuild_plan_impl(
        &mut self,
        model: &ControlPlaneModel,
        seed_override: Option<&PhysicalIndexingPlan>,
    ) {
        SCHEDULE_TOTAL.inc();

        let notify_on_drop = self.next_rebuild_tracker.start_rebuild();

        let sources = get_sources_to_schedule(model, disable_ingest_v1());

        let indexers: Vec<IndexerNodeInfo> = self.select_available_indexers_for_scheduling();

        let indexer_id_to_cpu_capacities: FnvHashMap<String, CpuCapacity> = indexers
            .iter()
            .filter_map(|indexer| {
                if indexer.indexing_capacity.cpu_millis() > 0 {
                    Some((indexer.node_id.to_string(), indexer.indexing_capacity))
                } else {
                    None
                }
            })
            .collect();

        if indexer_id_to_cpu_capacities.is_empty() {
            if !sources.is_empty() {
                warn!("no indexing capacity available, cannot schedule an indexing plan");
            }
            return;
        };

        let previous_plan_opt = match seed_override {
            Some(seed) => Some(seed),
            None => self.state.last_applied_physical_plan.as_ref(),
        };
        let shard_locations = model.shard_locations();
        let new_physical_plan = build_physical_indexing_plan(
            &sources,
            &indexer_id_to_cpu_capacities,
            previous_plan_opt,
            &shard_locations,
        );
        let shard_locality_metrics =
            get_shard_locality_metrics(&new_physical_plan, &shard_locations);
        shard_locality_metrics.publish();
        if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
            let plans_diff = get_indexing_plans_diff(
                last_applied_plan.indexing_tasks_per_indexer(),
                new_physical_plan.indexing_tasks_per_indexer(),
            );
            // No need to apply the new plan as it is the same as the old one.
            if plans_diff.is_empty() {
                return;
            }
        }
        self.apply_physical_indexing_plan(new_physical_plan, Some(notify_on_drop));
        self.state.num_schedule_indexing_plan += 1;
    }

    /// Returns a copy of `last_applied_plan` with the tasks that indexers are not currently running
    /// removed (per `plans_diff.missing_tasks_by_node_id`). Dropping those tasks leaves their
    /// shards unassigned, so a rebuild seeded with this plan re-places them from the current model
    /// (following any relocation) instead of pinning them back to their stale indexer. Healthy
    /// pipelines keep their tasks - and pipeline UIDs - so they are not needlessly restarted.
    fn seed_without_diverging_tasks(
        last_applied_plan: &PhysicalIndexingPlan,
        plans_diff: &IndexingPlansDiff,
    ) -> PhysicalIndexingPlan {
        let indexer_ids: Vec<String> = last_applied_plan
            .indexing_tasks_per_indexer()
            .keys()
            .cloned()
            .collect();
        let mut seed = PhysicalIndexingPlan::with_indexer_ids(&indexer_ids);
        for (indexer_id, tasks) in last_applied_plan.indexing_tasks_per_indexer() {
            let diverging_tasks = plans_diff
                .missing_tasks_by_node_id
                .get(indexer_id.as_str())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for task in tasks {
                if !diverging_tasks.contains(&task) {
                    seed.add_indexing_task(indexer_id, task.clone());
                }
            }
        }
        seed
    }

    /// Checks if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// - If node IDs differ, schedule a new indexing plan.
    /// - If indexing tasks differ, apply again the last plan.
    pub(crate) fn control_running_plan(&mut self, model: &ControlPlaneModel) {
        let last_applied_plan =
            if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
                last_applied_plan
            } else {
                // If there is no plan, the node is probably starting and the scheduler did not find
                // indexers yet. In this case, we want to schedule as soon as possible to find new
                // indexers.
                self.rebuild_plan(model);
                return;
            };
        if let Some(last_applied_plan_timestamp) = self.state.last_applied_plan_timestamp
            && Instant::now().duration_since(last_applied_plan_timestamp)
                < MIN_DURATION_BETWEEN_SCHEDULING
        {
            return;
        }
        let indexers: Vec<IndexerNodeInfo> = self.select_available_indexers_for_scheduling();
        let running_indexing_tasks_by_node_id: FnvHashMap<String, Vec<IndexingTask>> = indexers
            .iter()
            .map(|indexer| (indexer.node_id.to_string(), indexer.indexing_tasks.clone()))
            .collect();

        let indexing_plans_diff = get_indexing_plans_diff(
            &running_indexing_tasks_by_node_id,
            last_applied_plan.indexing_tasks_per_indexer(),
        );
        if !indexing_plans_diff.has_same_nodes() {
            // `rebuild_plan` installs a new plan, which resets the reapply counter in
            // `apply_physical_indexing_plan`.
            info!(plans_diff=?indexing_plans_diff, "running plan and last applied plan node IDs differ: schedule an indexing plan");
            self.rebuild_plan(model);
        } else if !indexing_plans_diff.has_same_tasks() {
            // Some nodes may have not received their tasks, apply it again.
            self.state.num_consecutive_reapplies += 1;
            let num_consecutive_reapplies = self.state.num_consecutive_reapplies;
            // Once the identical plan has failed to converge for many cycles it is almost
            // certainly un-runnable; re-sending it again cannot help. Rebuild a fresh plan from the
            // current model instead, so that, if the model has since been corrected, the new plan
            // becomes runnable and the loop converges. Throttled to once every
            // `REAPPLY_LOOP_RESCHEDULE_THRESHOLD` cycles so a still-unreachable model does not
            // trigger a rebuild every cycle.
            if num_consecutive_reapplies >= REAPPLY_LOOP_RESCHEDULE_THRESHOLD
                && num_consecutive_reapplies.is_multiple_of(REAPPLY_LOOP_RESCHEDULE_THRESHOLD)
            {
                warn!(
                    num_consecutive_reapplies,
                    plans_diff=?indexing_plans_diff,
                    "reapplying the last plan is not converging: rebuilding the plan from the model"
                );
                // Seed the rebuild with the diverging tasks stripped out so their shards are
                // re-placed from the current model rather than pinned to their stale indexer (a
                // plain rebuild reuses the previous assignment and can reproduce the same
                // un-runnable plan).
                let seed =
                    Self::seed_without_diverging_tasks(last_applied_plan, &indexing_plans_diff);
                self.rebuild_plan_impl(model, Some(&seed));
            } else if num_consecutive_reapplies >= REAPPLY_LOOP_WARN_THRESHOLD {
                // A single reapply is the normal transient case (an indexer has not reported its
                // freshly applied tasks yet). Escalation to `warn` happens only once the loop
                // persists, since a plan that never converges indicates a stuck cluster rather
                // than propagation lag.
                warn!(
                    num_consecutive_reapplies,
                    plans_diff=?indexing_plans_diff,
                    "running tasks still differ from last applied plan after repeated reapplies: \
                     cluster is not converging"
                );
                self.apply_physical_indexing_plan(last_applied_plan.clone(), None);
            } else {
                info!(plans_diff=?indexing_plans_diff, "running tasks and last applied tasks differ: reapply last plan");
                self.apply_physical_indexing_plan(last_applied_plan.clone(), None);
            }
        } else {
            // Running plan matches the last applied plan: the cluster has converged.
            self.state.num_consecutive_reapplies = 0;
        }
    }

    fn select_available_indexers_for_scheduling(&self) -> Vec<IndexerNodeInfo> {
        let (ready, retiring): (Vec<IndexerNodeInfo>, Vec<IndexerNodeInfo>) = self
            .indexer_pool
            .values()
            .into_iter()
            .filter(|indexer| {
                matches!(
                    indexer.ingester_status,
                    IngesterStatus::Ready | IngesterStatus::Retiring
                )
            })
            .partition(|indexer| indexer.ingester_status == IngesterStatus::Ready);

        if ready.is_empty() {
            // Allow scheduling on retiring indexers to drain shards
            // and avoid decommission timeouts (e.g. single-node cluster).
            warn!(
                "no ready indexer available, falling back to retiring indexers for shard draining"
            );
            retiring
        } else {
            ready
        }
    }

    fn apply_physical_indexing_plan(
        &mut self,
        new_physical_plan: PhysicalIndexingPlan,
        notify_on_drop: Option<Arc<NotifyChangeOnDrop>>,
    ) {
        debug!(new_physical_plan=?new_physical_plan, "apply physical indexing plan");
        APPLY_PLAN_TOTAL.inc();
        // Installing a genuinely new plan (rebuild, rebalance, startup) resets the reapply counter,
        // regardless of which path scheduled it. A reapply of the identical plan (from
        // `control_running_plan`) leaves the counter untouched so it keeps growing while the
        // cluster fails to converge.
        let reapplies_same_plan =
            self.state.last_applied_physical_plan.as_ref() == Some(&new_physical_plan);
        if !reapplies_same_plan {
            self.state.num_consecutive_reapplies = 0;
        }
        // Retiring and decommissioning indexers still receive the plan so they can gracefully shut
        // down dropped pipelines; other states (initializing, decommissioned, failed) are skipped.
        for indexer in self.indexer_pool.values().into_iter().filter(|indexer| {
            matches!(
                indexer.ingester_status,
                IngesterStatus::Ready | IngesterStatus::Retiring | IngesterStatus::Decommissioning
            )
        }) {
            let indexing_tasks = new_physical_plan
                .indexer(indexer.node_id.as_str())
                .unwrap_or(&[])
                .to_vec();
            // We don't want to block on a slow indexer so we apply this change asynchronously.
            // Bound the apply only for retiring/decommissioning indexers, so a slow or unreachable
            // draining node can't hold the change-notification guard; ready indexers get no
            // timeout.
            let apply_deadline = matches!(
                indexer.ingester_status,
                IngesterStatus::Retiring | IngesterStatus::Decommissioning
            )
            .then_some(APPLY_INDEXING_PLAN_TIMEOUT);
            let notify_on_drop = notify_on_drop.clone();
            tokio::spawn(async move {
                let client = indexer.client.clone();
                let apply_plan_fut =
                    client.apply_indexing_plan(ApplyIndexingPlanRequest { indexing_tasks });
                let apply_result = match apply_deadline {
                    Some(timeout) => tokio::time::timeout(timeout, apply_plan_fut).await,
                    None => Ok(apply_plan_fut.await),
                };
                match apply_result {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        warn!(
                            %error,
                            node_id=%indexer.node_id,
                            generation_id=indexer.generation_id,
                            "failed to apply indexing plan to indexer"
                        );
                    }
                    Err(_elapsed) => {
                        warn!(
                            node_id=%indexer.node_id,
                            generation_id=indexer.generation_id,
                            "timed out applying indexing plan to indexer"
                        );
                    }
                }
                drop(notify_on_drop);
            });
        }
        self.state.num_applied_physical_indexing_plan += 1;
        self.state.last_applied_plan_timestamp = Some(Instant::now());
        self.state.last_applied_physical_plan = Some(new_physical_plan);
    }
}

struct IndexingPlansDiff<'a> {
    pub missing_node_ids: FnvHashSet<&'a str>,
    pub unplanned_node_ids: FnvHashSet<&'a str>,
    pub missing_tasks_by_node_id: FnvHashMap<&'a str, Vec<&'a IndexingTask>>,
    pub unplanned_tasks_by_node_id: FnvHashMap<&'a str, Vec<&'a IndexingTask>>,
}

impl IndexingPlansDiff<'_> {
    pub fn has_same_nodes(&self) -> bool {
        self.missing_node_ids.is_empty() && self.unplanned_node_ids.is_empty()
    }

    pub fn has_same_tasks(&self) -> bool {
        self.missing_tasks_by_node_id
            .values()
            .map(Vec::len)
            .sum::<usize>()
            == 0
            && self
                .unplanned_tasks_by_node_id
                .values()
                .map(Vec::len)
                .sum::<usize>()
                == 0
    }

    pub fn is_empty(&self) -> bool {
        self.has_same_nodes() && self.has_same_tasks()
    }
}

fn get_shard_locality_metrics(
    physical_plan: &PhysicalIndexingPlan,
    shard_locations: &ShardLocations,
) -> ShardLocalityMetrics {
    let mut num_local_shards = 0;
    let mut num_remote_shards = 0;
    for (indexer, tasks) in physical_plan.indexing_tasks_per_indexer() {
        for task in tasks {
            for shard_id in &task.shard_ids {
                if shard_locations
                    .get_shard_locations(shard_id)
                    .iter()
                    .any(|node| node.as_str() == indexer)
                {
                    num_local_shards += 1;
                } else {
                    num_remote_shards += 1;
                }
            }
        }
    }
    ShardLocalityMetrics {
        num_remote_shards,
        num_local_shards,
    }
}

impl fmt::Debug for IndexingPlansDiff<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.has_same_nodes() && self.has_same_tasks() {
            return write!(formatter, "EmptyIndexingPlansDiff");
        }
        write!(formatter, "IndexingPlansDiff(")?;
        let mut separator = "";
        if !self.missing_node_ids.is_empty() {
            write!(
                formatter,
                "missing_node_ids={:?}",
                PrettySample::new(&self.missing_node_ids, 10)
            )?;
            separator = ", "
        }
        if !self.unplanned_node_ids.is_empty() {
            write!(
                formatter,
                "{separator}unplanned_node_ids={:?}",
                PrettySample::new(&self.unplanned_node_ids, 10)
            )?;
            separator = ", "
        }
        if !self.missing_tasks_by_node_id.is_empty() {
            write!(formatter, "{separator}missing_tasks_by_node_id=",)?;
            format_indexing_task_map(formatter, &self.missing_tasks_by_node_id)?;
            separator = ", "
        }
        if !self.unplanned_tasks_by_node_id.is_empty() {
            write!(formatter, "{separator}unplanned_tasks_by_node_id=",)?;
            format_indexing_task_map(formatter, &self.unplanned_tasks_by_node_id)?;
        }
        write!(formatter, ")")
    }
}

fn format_indexing_task_map(
    formatter: &mut std::fmt::Formatter,
    indexing_tasks: &FnvHashMap<&str, Vec<&IndexingTask>>,
) -> std::fmt::Result {
    // we show at most 5 nodes, and aggregate the results for the other.
    // we show at most 10 indexes, but aggregate results after.
    // we always aggregate shard ids
    // we hide pipeline id and incarnation id, they are not very useful in most case, but take a
    // lot of place
    const MAX_NODE: usize = 5;
    const MAX_INDEXES: usize = 10;
    let mut index_displayed = 0;
    write!(formatter, "{{")?;
    let mut indexer_iter = indexing_tasks.iter().enumerate();
    for (i, (index_name, tasks)) in &mut indexer_iter {
        if i != 0 {
            write!(formatter, ", ")?;
        }
        if index_displayed != MAX_INDEXES - 1 {
            write!(formatter, "{index_name:?}: [")?;
            let mut tasks_iter = tasks.iter().enumerate();
            for (i, task) in &mut tasks_iter {
                if i != 0 {
                    write!(formatter, ", ")?;
                }
                write!(
                    formatter,
                    r#"(index_id: "{}", source_id: "{}", shard_count: {})"#,
                    task.index_uid.as_ref().unwrap().index_id,
                    task.source_id,
                    task.shard_ids.len()
                )?;
                index_displayed += 1;
                if index_displayed == MAX_INDEXES - 1 {
                    let (task_count, shard_count) = tasks_iter.fold((0, 0), |(t, s), (_, task)| {
                        (t + 1, s + task.shard_ids.len())
                    });
                    if task_count > 0 {
                        write!(
                            formatter,
                            " and {task_count} tasks and {shard_count} shards"
                        )?;
                    }
                    break;
                }
            }
            write!(formatter, "]")?;
        } else {
            write!(
                formatter,
                "{index_name:?}: [with {} tasks and {} shards]",
                tasks.len(),
                tasks.iter().map(|task| task.shard_ids.len()).sum::<usize>()
            )?;
        }
        if i == MAX_NODE - 1 {
            break;
        }
    }
    let (indexer, tasks, shards) = indexer_iter.fold((0, 0, 0), |(i, t, s), (_, (_, task))| {
        (
            i + 1,
            t + task.len(),
            s + task.iter().map(|task| task.shard_ids.len()).sum::<usize>(),
        )
    });
    if indexer > 0 {
        write!(
            formatter,
            " and {indexer} more indexers, handling {tasks} tasks and {shards} shards}}"
        )
    } else {
        write!(formatter, "}}")
    }
}

/// Returns the difference between the `running_plan` retrieved from the chitchat state and
/// the last plan applied by the scheduler.
fn get_indexing_plans_diff<'a>(
    running_plan: &'a FnvHashMap<String, Vec<IndexingTask>>,
    last_applied_plan: &'a FnvHashMap<String, Vec<IndexingTask>>,
) -> IndexingPlansDiff<'a> {
    // Nodes diff.
    let running_node_ids: FnvHashSet<&str> = running_plan
        .keys()
        .map(|node_id| node_id.as_str())
        .collect();
    let planned_node_ids: FnvHashSet<&str> = last_applied_plan
        .keys()
        .map(|node_id| node_id.as_str())
        .collect();
    let missing_node_ids: FnvHashSet<&str> = planned_node_ids
        .difference(&running_node_ids)
        .copied()
        .collect();
    let unplanned_node_ids: FnvHashSet<&str> = running_node_ids
        .difference(&planned_node_ids)
        .copied()
        .collect();
    // Tasks diff.
    let mut missing_tasks_by_node_id: FnvHashMap<&str, Vec<&IndexingTask>> = FnvHashMap::default();
    let mut unplanned_tasks_by_node_id: FnvHashMap<&str, Vec<&IndexingTask>> =
        FnvHashMap::default();
    for node_id in running_node_ids.iter().chain(planned_node_ids.iter()) {
        let running_tasks = running_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let last_applied_tasks = last_applied_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let (missing_tasks, unplanned_tasks) =
            get_indexing_tasks_diff(running_tasks, last_applied_tasks);
        missing_tasks_by_node_id.insert(*node_id, missing_tasks);
        unplanned_tasks_by_node_id.insert(*node_id, unplanned_tasks);
    }
    IndexingPlansDiff {
        missing_node_ids,
        unplanned_node_ids,
        missing_tasks_by_node_id,
        unplanned_tasks_by_node_id,
    }
}

/// Computes the difference between `running_tasks` and `last_applied_tasks` and returns a tuple
/// of `missing_tasks` and `unplanned_tasks`.
/// Note: we need to handle duplicate tasks in each array, so we count them and make the diff.
fn get_indexing_tasks_diff<'a>(
    running_tasks: &'a [IndexingTask],
    last_applied_tasks: &'a [IndexingTask],
) -> (Vec<&'a IndexingTask>, Vec<&'a IndexingTask>) {
    let mut missing_tasks: Vec<&IndexingTask> = Vec::new();
    let mut unplanned_tasks: Vec<&IndexingTask> = Vec::new();
    let grouped_running_tasks: FnvHashMap<&IndexingTask, usize> = running_tasks
        .iter()
        .chunk_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let grouped_last_applied_tasks: FnvHashMap<&IndexingTask, usize> = last_applied_tasks
        .iter()
        .chunk_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let all_tasks: FnvHashSet<&IndexingTask> =
        FnvHashSet::from_iter(running_tasks.iter().chain(last_applied_tasks.iter()));
    for task in all_tasks {
        let running_task_count = grouped_running_tasks.get(task).unwrap_or(&0);
        let desired_task_count = grouped_last_applied_tasks.get(task).unwrap_or(&0);
        match running_task_count.cmp(desired_task_count) {
            Ordering::Greater => {
                unplanned_tasks
                    .extend_from_slice(&vec![task; running_task_count - desired_task_count]);
            }
            Ordering::Less => {
                missing_tasks
                    .extend_from_slice(&vec![task; desired_task_count - running_task_count])
            }
            _ => {}
        }
    }

    (missing_tasks, unplanned_tasks)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use proptest::{prop_compose, proptest};
    use quickwit_config::{IndexConfig, KafkaSourceParams, SourceConfig, SourceParams};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::types::{IndexUid, PipelineUid, ShardId, SourceUid};

    use super::*;
    use crate::model::ShardLocations;
    #[test]
    fn test_indexing_plans_diff() {
        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let index_uid2 = IndexUid::from_str("index-2:11111111111111111111111111").unwrap();
        {
            let running_plan = FnvHashMap::default();
            let desired_plan = FnvHashMap::default();
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(10u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(11u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(20u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1b.clone(), task_2.clone()],
            );
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_2, task_1.clone(), task_1b.clone()],
            );
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);
            desired_plan.insert("indexer-1".to_string(), vec![task_2.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1])])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_2])])
            );
        }
        {
            // Task assigned to indexer-1 in desired plan but another one running.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(1u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_2 = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(2u128)),
                index_uid: Some(index_uid2.clone()),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert("indexer-2".to_string(), vec![task_2.clone()]);
            desired_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(!indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_node_ids,
                FnvHashSet::from_iter(["indexer-1"])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_node_ids,
                FnvHashSet::from_iter(["indexer-2"])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1]), ("indexer-2", Vec::new())])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-2", vec![&task_2]), ("indexer-1", Vec::new())])
            );
        }
        {
            // Diff with 3 same tasks running but only one on the desired plan.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1a = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(10u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_1b = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(11u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            let task_1c = IndexingTask {
                pipeline_uid: Some(PipelineUid::for_test(12u128)),
                index_uid: Some(index_uid.clone()),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
                params_fingerprint: 0,
            };
            running_plan.insert("indexer-1".to_string(), vec![task_1a.clone()]);
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_1a.clone(), task_1b.clone(), task_1c.clone()],
            );

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1b, &task_1c])])
            );
        }
    }

    #[test]
    fn test_get_sources_to_schedule() {
        let mut model = ControlPlaneModel::default();
        let kafka_source_params = KafkaSourceParams {
            topic: "kafka-topic".to_string(),
            client_log_level: None,
            client_params: serde_json::json!({}),
            enable_backfill_mode: false,
        };
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        model.add_index(index_metadata);
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "source_disabled".to_string(),
                    num_pipelines: NonZeroUsize::new(3).unwrap(),
                    enabled: false,
                    source_params: SourceParams::Kafka(kafka_source_params.clone()),
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "source_enabled".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    source_params: SourceParams::Kafka(kafka_source_params.clone()),
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v1".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
                    source_params: SourceParams::IngestApi,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v2".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v2
                    source_params: SourceParams::Ingest,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        // ingest v2 without any open shard is skipped.
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v2_without_shard".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v2
                    source_params: SourceParams::Ingest,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_cli".to_string(),
                    num_pipelines: NonZeroUsize::new(2).unwrap(),
                    enabled: true,
                    // ingest v1
                    source_params: SourceParams::IngestCli,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "ingest_v2".to_string(),
            shard_id: Some(ShardId::from(17)),
            shard_state: ShardState::Open as i32,
            ..Default::default()
        };
        model.insert_shards(&index_uid, &"ingest_v2".to_string(), vec![shard]);

        let disable_ingest_v1 = false;
        let sources: Vec<SourceToSchedule> = get_sources_to_schedule(&model, disable_ingest_v1);
        assert_eq!(sources.len(), 3);

        let disable_ingest_v1 = true;
        let sources: Vec<SourceToSchedule> = get_sources_to_schedule(&model, disable_ingest_v1);
        assert_eq!(sources.len(), 2);

        let contains_any_ingest_v1_source = sources
            .iter()
            .any(|source| matches!(source.source_type, SourceToScheduleType::IngestV1));
        assert!(!contains_any_ingest_v1_source);
    }

    #[test]
    fn test_build_physical_indexing_plan_simple() {
        let source_1 = SourceUid {
            index_uid: IndexUid::for_test("index-1", 0),
            source_id: "source1".to_string(),
        };
        let source_2 = SourceUid {
            index_uid: IndexUid::for_test("index-2", 0),
            source_id: "source2".to_string(),
        };
        let sources = [
            SourceToSchedule {
                source_uid: source_1.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 3,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            },
            SourceToSchedule {
                source_uid: source_2.clone(),
                source_type: SourceToScheduleType::NonSharded {
                    num_pipelines: 2,
                    load_per_pipeline: NonZeroU32::new(1_000).unwrap(),
                },
                params_fingerprint: 0,
            },
        ];
        let mut indexer_max_loads = FnvHashMap::default();
        indexer_max_loads.insert("indexer1".to_string(), mcpu(3_000));
        indexer_max_loads.insert("indexer2".to_string(), mcpu(3_000));
        let shard_locations = ShardLocations::default();
        let physical_plan =
            build_physical_indexing_plan(&sources[..], &indexer_max_loads, None, &shard_locations);
        assert_eq!(physical_plan.indexing_tasks_per_indexer().len(), 2);
        let indexing_tasks_1 = physical_plan.indexer("indexer1").unwrap();
        assert_eq!(indexing_tasks_1.len(), 2);
        let indexer_2_tasks = physical_plan.indexer("indexer2").unwrap();
        assert_eq!(indexer_2_tasks.len(), 3);
    }

    #[test]
    fn test_debug_indexing_task_map() {
        let mut map = FnvHashMap::default();
        let task1 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index1", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard1".into()],
            params_fingerprint: 0,
        };
        let task2 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index2", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard2".into(), "shard3".into()],
            params_fingerprint: 0,
        };
        let task3 = IndexingTask {
            index_uid: Some(IndexUid::for_test("index3", 123)),
            source_id: "my-source".to_string(),
            pipeline_uid: Some(PipelineUid::random()),
            shard_ids: vec!["shard6".into()],
            params_fingerprint: 0,
        };
        // order made to map with the debug for lisibility
        map.insert("indexer5", vec![&task2]);
        map.insert("indexer4", vec![&task1]);
        map.insert("indexer3", vec![&task1, &task3]);
        map.insert("indexer2", vec![&task2, &task3, &task1, &task2]);
        map.insert("indexer1", vec![&task1, &task2, &task3, &task1]);
        map.insert("indexer6", vec![&task1, &task2, &task3]);
        let plan = IndexingPlansDiff {
            missing_node_ids: FnvHashSet::default(),
            unplanned_node_ids: FnvHashSet::default(),
            missing_tasks_by_node_id: map,
            unplanned_tasks_by_node_id: FnvHashMap::default(),
        };

        let debug = format!("{plan:?}");
        assert_eq!(
            debug,
            r#"IndexingPlansDiff(missing_tasks_by_node_id={"indexer5": [(index_id: "index2", source_id: "my-source", shard_count: 2)], "indexer4": [(index_id: "index1", source_id: "my-source", shard_count: 1)], "indexer3": [(index_id: "index1", source_id: "my-source", shard_count: 1), (index_id: "index3", source_id: "my-source", shard_count: 1)], "indexer2": [(index_id: "index2", source_id: "my-source", shard_count: 2), (index_id: "index3", source_id: "my-source", shard_count: 1), (index_id: "index1", source_id: "my-source", shard_count: 1), (index_id: "index2", source_id: "my-source", shard_count: 2)], "indexer1": [(index_id: "index1", source_id: "my-source", shard_count: 1) and 3 tasks and 4 shards] and 1 more indexers, handling 3 tasks and 4 shards})"#
        );
    }

    proptest! {
        #[test]
        fn test_building_indexing_tasks_and_physical_plan(num_indexers in 1usize..50usize, index_id_sources in proptest::collection::vec(gen_kafka_source(), 1..20)) {
            let index_uids: fnv::FnvHashSet<IndexUid> =
                index_id_sources.iter()
                    .map(|(index_uid, _)| index_uid.clone())
                    .collect();
            let mut model = ControlPlaneModel::default();
            for index_uid in index_uids {
                let index_config = IndexConfig::for_test(&index_uid.index_id, &format!("ram://test/{index_uid}"));
                model.add_index(IndexMetadata::new_with_index_uid(index_uid, index_config));
            }
            for (index_uid, source_config) in &index_id_sources {
                model.add_source(index_uid, source_config.clone()).unwrap();
            }

            let sources: Vec<SourceToSchedule> = get_sources_to_schedule(&model, false);
            let mut indexer_max_loads = FnvHashMap::default();
            for i in 0..num_indexers {
                let indexer_id = format!("indexer-{i}");
                indexer_max_loads.insert(indexer_id, mcpu(4_000));
            }
            let shard_locations = ShardLocations::default();
            let _physical_indexing_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, None, &shard_locations);
        }
    }

    use quickwit_config::SourceInputFormat;
    use quickwit_proto::indexing::{
        ApplyIndexingPlanResponse, CpuCapacity, IndexingServiceClient, MockIndexingService, mcpu,
    };
    use quickwit_proto::ingest::{Shard, ShardState};

    fn mock_indexer_node_info(node_id: &str, status: IngesterStatus) -> IndexerNodeInfo {
        let mock_indexer = MockIndexingService::new();
        let client = IndexingServiceClient::from_mock(mock_indexer);
        IndexerNodeInfo {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
        }
    }

    #[test]
    fn test_select_available_indexers_returns_only_ready_when_available() {
        let indexer_pool = IndexerPool::default();
        let ready_indexer = mock_indexer_node_info("indexer-ready-1", IngesterStatus::Ready);
        let ready_indexer_2 = mock_indexer_node_info("indexer-ready-2", IngesterStatus::Ready);
        let retiring_indexer = mock_indexer_node_info("indexer-retiring", IngesterStatus::Retiring);
        indexer_pool.insert(ready_indexer.node_id.clone(), ready_indexer);
        indexer_pool.insert(ready_indexer_2.node_id.clone(), ready_indexer_2);
        indexer_pool.insert(retiring_indexer.node_id.clone(), retiring_indexer);

        let scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let selected = scheduler.select_available_indexers_for_scheduling();

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|i| i.ingester_status == IngesterStatus::Ready)
        );
    }

    #[test]
    fn test_select_available_indexers_falls_back_to_retiring_when_no_ready() {
        let indexer_pool = IndexerPool::default();
        let retiring_1 = mock_indexer_node_info("indexer-retiring-1", IngesterStatus::Retiring);
        let retiring_2 = mock_indexer_node_info("indexer-retiring-2", IngesterStatus::Retiring);
        let decommissioned_1 =
            mock_indexer_node_info("indexer-decommissioned-1", IngesterStatus::Decommissioned);
        let decommissioning_1 =
            mock_indexer_node_info("indexer-decommissioning-1", IngesterStatus::Decommissioning);
        let initializing_1 =
            mock_indexer_node_info("indexer-initializing-1", IngesterStatus::Initializing);

        indexer_pool.insert(retiring_1.node_id.clone(), retiring_1);
        indexer_pool.insert(retiring_2.node_id.clone(), retiring_2);
        indexer_pool.insert(decommissioned_1.node_id.clone(), decommissioned_1);
        indexer_pool.insert(decommissioning_1.node_id.clone(), decommissioning_1);
        indexer_pool.insert(initializing_1.node_id.clone(), initializing_1);

        let scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let selected = scheduler.select_available_indexers_for_scheduling();

        assert_eq!(selected.len(), 2);
        assert!(
            selected
                .iter()
                .all(|i| i.ingester_status == IngesterStatus::Retiring)
        );
    }

    #[test]
    fn test_select_available_indexers_returns_empty_when_pool_is_empty() {
        let indexer_pool = IndexerPool::default();
        let scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let selected = scheduler.select_available_indexers_for_scheduling();
        assert!(selected.is_empty());
    }

    // Only ready, retiring, and decommissioning indexers receive a plan; indexers in any other
    // state must be skipped entirely. See `apply_physical_indexing_plan`.
    #[tokio::test]
    async fn test_apply_plan_skips_non_eligible_indexers() {
        let indexer_pool = IndexerPool::default();
        let eligible_indexers = [
            asserting_indexer_node_info("indexer-ready", IngesterStatus::Ready, true),
            asserting_indexer_node_info("indexer-retiring", IngesterStatus::Retiring, true),
            asserting_indexer_node_info(
                "indexer-decommissioning",
                IngesterStatus::Decommissioning,
                true,
            ),
        ];
        let skipped_indexers = [
            never_applied_indexer_node_info("indexer-unspecified", IngesterStatus::Unspecified),
            never_applied_indexer_node_info("indexer-initializing", IngesterStatus::Initializing),
            never_applied_indexer_node_info(
                "indexer-decommissioned",
                IngesterStatus::Decommissioned,
            ),
            never_applied_indexer_node_info("indexer-failed", IngesterStatus::Failed),
        ];
        for indexer in eligible_indexers.into_iter().chain(skipped_indexers) {
            indexer_pool.insert(indexer.node_id.clone(), indexer);
        }

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let physical_plan = PhysicalIndexingPlan::with_indexer_ids(&[]);
        let waiter = scheduler.next_rebuild_tracker.next_rebuild_waiter();
        let notify_on_drop = scheduler.next_rebuild_tracker.start_rebuild();
        scheduler.apply_physical_indexing_plan(physical_plan, Some(notify_on_drop));
        waiter.await;
    }

    // A node the planner dropped from the plan (e.g. a retiring indexer) must still receive an
    // empty plan so it shuts down its now-orphaned pipelines.
    #[tokio::test]
    async fn test_apply_plan_sends_empty_plan_to_dropped_indexer() {
        let indexer_pool = IndexerPool::default();
        let ready_indexer =
            asserting_indexer_node_info("indexer-ready", IngesterStatus::Ready, false);
        // Dropped from the plan (retiring): must receive an empty plan.
        let retiring_indexer =
            asserting_indexer_node_info("indexer-retiring", IngesterStatus::Retiring, true);
        indexer_pool.insert(ready_indexer.node_id.clone(), ready_indexer);
        indexer_pool.insert(retiring_indexer.node_id.clone(), retiring_indexer);

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );

        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            index_uid: Some(index_uid),
            source_id: "source-1".to_string(),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };
        let mut physical_plan =
            PhysicalIndexingPlan::with_indexer_ids(&["indexer-ready".to_string()]);
        physical_plan.add_indexing_task("indexer-ready", task);

        // `apply_physical_indexing_plan` dispatches the RPCs on spawned tasks; the rebuild waiter
        // resolves once every spawned task has dropped its `notify_on_drop` clone, i.e. after all
        // `apply_indexing_plan` calls have completed.
        let waiter = scheduler.next_rebuild_tracker.next_rebuild_waiter();
        let notify_on_drop = scheduler.next_rebuild_tracker.start_rebuild();
        scheduler.apply_physical_indexing_plan(physical_plan, Some(notify_on_drop));
        waiter.await;
    }

    // Builds an `IndexerNodeInfo` whose client asserts the exact `ApplyIndexingPlanRequest` it
    // receives (via `withf`) and that it is called exactly once (via `times(1)`, verified on drop).
    fn asserting_indexer_node_info(
        node_id: &str,
        status: IngesterStatus,
        expect_empty_plan: bool,
    ) -> IndexerNodeInfo {
        let mut mock_indexer = MockIndexingService::new();
        mock_indexer
            .expect_apply_indexing_plan()
            .times(1)
            .withf(move |request| request.indexing_tasks.is_empty() == expect_empty_plan)
            .returning(|_| Ok(ApplyIndexingPlanResponse {}));
        let client = IndexingServiceClient::from_mock(mock_indexer);
        IndexerNodeInfo {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
        }
    }

    // Builds an `IndexerNodeInfo` whose client asserts it is never asked to apply a plan (via
    // `never()`, verified on drop). The shared mock is `Arc`-cloned across the client, so a wrong
    // call from a spawned task is seen when the pool's copy drops on the main thread.
    fn never_applied_indexer_node_info(node_id: &str, status: IngesterStatus) -> IndexerNodeInfo {
        let mut mock_indexer = MockIndexingService::new();
        mock_indexer.expect_apply_indexing_plan().never();
        let client = IndexingServiceClient::from_mock(mock_indexer);
        IndexerNodeInfo {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
        }
    }

    // An `IndexingService` whose apply RPC never returns, so the spawned apply task can only
    // finish if a timeout cancels it.
    #[derive(Debug)]
    struct HangingIndexingService;

    #[async_trait::async_trait]
    impl IndexingService for HangingIndexingService {
        async fn apply_indexing_plan(
            &self,
            _request: ApplyIndexingPlanRequest,
        ) -> quickwit_proto::indexing::IndexingResult<ApplyIndexingPlanResponse> {
            std::future::pending().await
        }
    }

    fn hanging_indexer_node_info(status: IngesterStatus) -> IndexerNodeInfo {
        let client = IndexingServiceClient::tower().build(HangingIndexingService);
        IndexerNodeInfo {
            node_id: NodeId::from_str("indexer"),
            generation_id: 0,
            client,
            indexing_tasks: Vec::new(),
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
        }
    }

    // Applies a plan to a single indexer whose apply RPC hangs forever, then reports whether the
    // apply task finished within `observe` — i.e. whether a timeout cancelled it.
    async fn hanging_apply_is_cancelled_within(status: IngesterStatus, observe: Duration) -> bool {
        let indexer_pool = IndexerPool::default();
        let indexer = hanging_indexer_node_info(status);
        indexer_pool.insert(indexer.node_id.clone(), indexer);
        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let physical_plan = PhysicalIndexingPlan::with_indexer_ids(&[]);
        let waiter = scheduler.next_rebuild_tracker.next_rebuild_waiter();
        let notify_on_drop = scheduler.next_rebuild_tracker.start_rebuild();
        scheduler.apply_physical_indexing_plan(physical_plan, Some(notify_on_drop));
        // The waiter resolves only once the spawned apply task drops its `notify_on_drop`, which
        // for a hanging RPC happens only if a timeout fires.
        tokio::time::timeout(observe, waiter).await.is_ok()
    }

    #[tokio::test]
    async fn test_apply_plan_times_out_only_for_draining_indexers() {
        // A ready indexer is unbounded: the hanging apply is never cancelled, so its task never
        // finishes (a wrongly-applied timeout would fire well within 500ms and flip this).
        assert!(
            !hanging_apply_is_cancelled_within(IngesterStatus::Ready, Duration::from_millis(500))
                .await
        );
        // Retiring/decommissioning indexers are bounded, so the hanging apply is cancelled and the
        // task finishes (resolves in ~APPLY_INDEXING_PLAN_TIMEOUT, far within the window).
        assert!(
            hanging_apply_is_cancelled_within(IngesterStatus::Retiring, Duration::from_secs(5))
                .await
        );
        assert!(
            hanging_apply_is_cancelled_within(
                IngesterStatus::Decommissioning,
                Duration::from_secs(5)
            )
            .await
        );
    }

    // Builds an `IndexerNodeInfo` whose client accepts any number of `apply_indexing_plan` calls
    // (including zero) and reports `indexing_tasks` as its running tasks.
    fn accepting_indexer_node_info(
        node_id: &str,
        status: IngesterStatus,
        indexing_tasks: Vec<IndexingTask>,
    ) -> IndexerNodeInfo {
        let mut mock_indexer = MockIndexingService::new();
        mock_indexer
            .expect_apply_indexing_plan()
            .times(0..)
            .returning(|_| Ok(ApplyIndexingPlanResponse {}));
        let client = IndexingServiceClient::from_mock(mock_indexer);
        IndexerNodeInfo {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client,
            indexing_tasks,
            indexing_capacity: CpuCapacity::from_cpu_millis(4_000),
            ingester_status: status,
        }
    }

    // When the running tasks keep differing from the last applied plan, the control loop reapplies
    // the identical plan every cycle. Each consecutive reapply must be counted so that a cluster
    // which never converges becomes observable, and the counter must reset to 0 as soon as the
    // cluster converges.
    #[tokio::test]
    async fn test_control_running_plan_counts_consecutive_reapplies() {
        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            index_uid: Some(index_uid),
            source_id: "source-1".to_string(),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };

        // Same node in the running plan and the last applied plan, but the indexer runs no task
        // while the plan expects one: this drives the "reapply last plan" branch every cycle.
        let indexer_pool = IndexerPool::default();
        let indexer = accepting_indexer_node_info("indexer-1", IngesterStatus::Ready, Vec::new());
        indexer_pool.insert(indexer.node_id.clone(), indexer);

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool.clone(),
        );
        let mut physical_plan = PhysicalIndexingPlan::with_indexer_ids(&["indexer-1".to_string()]);
        physical_plan.add_indexing_task("indexer-1", task.clone());
        scheduler.state.last_applied_physical_plan = Some(physical_plan);
        // Leaving `last_applied_plan_timestamp` at `None` bypasses the min-interval guard so every
        // call actually evaluates the diff.

        let model = ControlPlaneModel::default();

        // Reapplying a plan stamps `last_applied_plan_timestamp`, which would otherwise make the
        // next call short-circuit on the min-interval guard; clearing it each cycle simulates that
        // enough time has elapsed and isolates the counter logic under test.
        let num_cycles = REAPPLY_LOOP_WARN_THRESHOLD + 1;
        for expected in 1..=num_cycles {
            scheduler.state.last_applied_plan_timestamp = None;
            scheduler.control_running_plan(&model);
            assert_eq!(scheduler.state.num_consecutive_reapplies, expected);
        }

        // The indexer now runs exactly the planned task: the cluster has converged (`Pool` is a
        // shared handle, so re-inserting updates the scheduler's view) and the counter resets.
        let converged_indexer =
            accepting_indexer_node_info("indexer-1", IngesterStatus::Ready, vec![task]);
        indexer_pool.insert(converged_indexer.node_id.clone(), converged_indexer);
        scheduler.state.last_applied_plan_timestamp = None;
        scheduler.control_running_plan(&model);
        assert_eq!(scheduler.state.num_consecutive_reapplies, 0);
    }

    // The reapply counter must be reset whenever a genuinely new plan is installed, no matter which
    // path scheduled it (control loop, `RebuildPlan` handler, rebalance). Installing a different
    // plan resets it; reapplying the identical plan does not. An empty pool keeps
    // `apply_physical_indexing_plan` from issuing RPCs.
    #[tokio::test]
    async fn test_apply_physical_indexing_plan_resets_reapplies_for_new_plan_only() {
        let indexer_pool = IndexerPool::default();
        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );

        let plan_a = PhysicalIndexingPlan::with_indexer_ids(&["indexer-1".to_string()]);
        scheduler.apply_physical_indexing_plan(plan_a.clone(), None);
        // Simulate a loop that has been stuck reapplying `plan_a` for a while.
        scheduler.state.num_consecutive_reapplies = 7;

        // Reapplying the identical plan must not reset the counter.
        scheduler.apply_physical_indexing_plan(plan_a, None);
        assert_eq!(scheduler.state.num_consecutive_reapplies, 7);

        // Installing a different plan (as rebuild/rebalance would) resets the counter.
        let plan_b = PhysicalIndexingPlan::with_indexer_ids(&[
            "indexer-1".to_string(),
            "indexer-2".to_string(),
        ]);
        scheduler.apply_physical_indexing_plan(plan_b, None);
        assert_eq!(scheduler.state.num_consecutive_reapplies, 0);
    }

    // Reapplying the identical plan can never converge when the plan is un-runnable (here, it
    // assigns a task the indexer never reports running while the model has no source to back it).
    // After `REAPPLY_LOOP_RESCHEDULE_THRESHOLD` cycles the control loop must stop reapplying and
    // rebuild from the model, which drops the un-runnable task and lets the cluster converge.
    #[tokio::test]
    async fn test_control_running_plan_rebuilds_after_persistent_reapplies() {
        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            index_uid: Some(index_uid),
            source_id: "source-1".to_string(),
            shard_ids: Vec::new(),
            params_fingerprint: 0,
        };

        // The indexer runs no task, but the last applied plan expects one, so the control loop hits
        // the "reapply last plan" branch every cycle.
        let indexer_pool = IndexerPool::default();
        let indexer = accepting_indexer_node_info("indexer-1", IngesterStatus::Ready, Vec::new());
        indexer_pool.insert(indexer.node_id.clone(), indexer);

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );
        let mut physical_plan = PhysicalIndexingPlan::with_indexer_ids(&["indexer-1".to_string()]);
        physical_plan.add_indexing_task("indexer-1", task);
        scheduler.state.last_applied_physical_plan = Some(physical_plan);

        // The model has no sources, so a rebuild produces a plan with no tasks for indexer-1,
        // different from the stuck plan, which is what breaks the loop.
        let model = ControlPlaneModel::default();

        // Cycles below the reschedule threshold keep reapplying the identical (un-runnable) plan:
        // the counter grows and the stuck task stays in the applied plan.
        for expected in 1..REAPPLY_LOOP_RESCHEDULE_THRESHOLD {
            scheduler.state.last_applied_plan_timestamp = None;
            scheduler.control_running_plan(&model);
            assert_eq!(scheduler.state.num_consecutive_reapplies, expected);
            assert_eq!(
                scheduler
                    .state
                    .last_applied_physical_plan
                    .as_ref()
                    .unwrap()
                    .indexing_tasks_per_indexer()["indexer-1"]
                    .len(),
                1,
                "the un-runnable task should still be planned before the reschedule threshold"
            );
        }

        // On the threshold cycle the loop rebuilds from the model instead of reapplying: the
        // rebuilt plan drops the un-runnable task and installing it resets the counter.
        scheduler.state.last_applied_plan_timestamp = None;
        scheduler.control_running_plan(&model);
        assert_eq!(scheduler.state.num_consecutive_reapplies, 0);
        assert!(
            scheduler
                .state
                .last_applied_physical_plan
                .as_ref()
                .unwrap()
                .indexing_tasks_per_indexer()["indexer-1"]
                .is_empty(),
            "the rebuild should have dropped the un-runnable task"
        );
    }

    // The seed used for the escalation rebuild must drop exactly the diverging tasks (those the
    // indexers are not running) and keep the healthy ones untouched, so only the stuck shards get
    // re-placed and healthy pipelines are not needlessly restarted.
    #[test]
    fn test_seed_without_diverging_tasks_strips_only_diverging() {
        let index_uid = IndexUid::from_str("index-1:11111111111111111111111111").unwrap();
        let stuck_task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            index_uid: Some(index_uid.clone()),
            source_id: "source-1".to_string(),
            shard_ids: vec![ShardId::from(17)],
            params_fingerprint: 0,
        };
        let healthy_task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(2u128)),
            index_uid: Some(index_uid),
            source_id: "source-1".to_string(),
            shard_ids: vec![ShardId::from(9)],
            params_fingerprint: 0,
        };
        let mut last_applied_plan = PhysicalIndexingPlan::with_indexer_ids(&[
            "indexer-1".to_string(),
            "indexer-2".to_string(),
        ]);
        last_applied_plan.add_indexing_task("indexer-1", stuck_task);
        last_applied_plan.add_indexing_task("indexer-2", healthy_task.clone());

        // indexer-1 runs nothing (its task diverges); indexer-2 runs exactly its planned task.
        let mut running: FnvHashMap<String, Vec<IndexingTask>> = FnvHashMap::default();
        running.insert("indexer-1".to_string(), Vec::new());
        running.insert("indexer-2".to_string(), vec![healthy_task.clone()]);
        let plans_diff =
            get_indexing_plans_diff(&running, last_applied_plan.indexing_tasks_per_indexer());
        assert!(!plans_diff.has_same_tasks());

        let seed = IndexingScheduler::seed_without_diverging_tasks(&last_applied_plan, &plans_diff);
        let seed_tasks = seed.indexing_tasks_per_indexer();
        assert!(seed_tasks["indexer-1"].is_empty());
        assert_eq!(seed_tasks["indexer-2"], vec![healthy_task]);
    }

    // When the model correction relocates a shard (it still exists, but now on a different
    // indexer), a plain reseeded rebuild would pin it back to the old indexer and stay stuck.
    // The escalation must re-place the diverging shard from the current model, i.e. follow it
    // to its new host.
    #[tokio::test]
    async fn test_control_running_plan_rebuild_follows_relocated_shard() {
        let index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
        let index_uid = index_metadata.index_uid.clone();
        let mut model = ControlPlaneModel::default();
        model.add_index(index_metadata);
        model
            .add_source(
                &index_uid,
                SourceConfig {
                    source_id: "ingest_v2".to_string(),
                    num_pipelines: NonZeroUsize::new(1).unwrap(),
                    enabled: true,
                    source_params: SourceParams::Ingest,
                    transform_config: None,
                    input_format: Default::default(),
                },
            )
            .unwrap();
        // The model now locates shard 17 on indexer-2 (its leader).
        let shard = Shard {
            index_uid: Some(index_uid.clone()),
            source_id: "ingest_v2".to_string(),
            shard_id: Some(ShardId::from(17)),
            shard_state: ShardState::Open as i32,
            leader_id: "indexer-2".to_string(),
            ..Default::default()
        };
        model.insert_shards(&index_uid, &"ingest_v2".to_string(), vec![shard]);

        let indexer_pool = IndexerPool::default();
        for indexer_id in ["indexer-1", "indexer-2"] {
            let indexer =
                accepting_indexer_node_info(indexer_id, IngesterStatus::Ready, Vec::new());
            indexer_pool.insert(indexer.node_id.clone(), indexer);
        }

        let mut scheduler = IndexingScheduler::new(
            "test-cluster".to_string(),
            NodeId::from_str("control-plane"),
            indexer_pool,
        );

        // Stale plan: shard 17 assigned to indexer-1, which no longer hosts it, and which never
        // reports running it - so the loop diverges every cycle.
        let stale_task = IndexingTask {
            pipeline_uid: Some(PipelineUid::for_test(1u128)),
            index_uid: Some(index_uid.clone()),
            source_id: "ingest_v2".to_string(),
            shard_ids: vec![ShardId::from(17)],
            params_fingerprint: 0,
        };
        let mut stale_plan = PhysicalIndexingPlan::with_indexer_ids(&[
            "indexer-1".to_string(),
            "indexer-2".to_string(),
        ]);
        stale_plan.add_indexing_task("indexer-1", stale_task);
        scheduler.state.last_applied_physical_plan = Some(stale_plan);

        for _ in 0..REAPPLY_LOOP_RESCHEDULE_THRESHOLD {
            scheduler.state.last_applied_plan_timestamp = None;
            scheduler.control_running_plan(&model);
        }

        let last_applied_plan = scheduler.state.last_applied_physical_plan.as_ref().unwrap();
        let tasks_per_indexer = last_applied_plan.indexing_tasks_per_indexer();
        let shard_17 = ShardId::from(17);
        let indexer_1_has_shard = tasks_per_indexer["indexer-1"]
            .iter()
            .any(|task| task.shard_ids.contains(&shard_17));
        let indexer_2_has_shard = tasks_per_indexer["indexer-2"]
            .iter()
            .any(|task| task.shard_ids.contains(&shard_17));
        assert!(
            !indexer_1_has_shard,
            "the shard should have moved off the stale indexer"
        );
        assert!(
            indexer_2_has_shard,
            "the shard should follow to the indexer that now hosts it"
        );
    }

    fn kafka_source_params_for_test() -> SourceParams {
        SourceParams::Kafka(KafkaSourceParams {
            topic: "topic".to_string(),
            client_log_level: None,
            client_params: serde_json::json!({
                "bootstrap.servers": "localhost:9092",
            }),
            enable_backfill_mode: true,
        })
    }

    prop_compose! {
      fn gen_kafka_source()
        (index_idx in 0usize..100usize, num_pipelines in 1usize..51usize) -> (IndexUid, SourceConfig) {
          let index_uid = IndexUid::for_test(&format!("index-id-{index_idx}"), 0 /* this is the index uid */);
          let source_id = quickwit_common::rand::append_random_suffix("kafka-source");
          (index_uid, SourceConfig {
              source_id,
              num_pipelines: NonZeroUsize::new(num_pipelines).unwrap(),
              enabled: true,
              source_params: kafka_source_params_for_test(),
              transform_config: None,
              input_format: SourceInputFormat::Json,
          })
      }
    }
}
