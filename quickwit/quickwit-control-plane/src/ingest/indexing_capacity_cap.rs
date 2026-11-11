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

//! Caps shard scale-up by the indexing CPU capacity of the cluster.
//!
//! Under sudden load, the ingest controller can keep opening shards. Past a point, more shards
//! (and therefore more indexing pipelines) do not increase indexing throughput, because indexers
//! are saturated. This module computes how many extra shards a source can get before the
//! pipelines implied by all the shards of the cluster exceed what indexers can run.
//!
//! The model is an estimate:
//! - the pipeline budget is the advertised CPU capacity of all indexers divided by
//!   `QW_CPU_MILLIS_PER_PIPELINE` (default: one core per pipeline);
//! - the number of shards a pipeline can host for a source is computed with the same load model as
//!   the indexing scheduler (see `compute_max_num_shards_per_pipeline_for_shards`).
//!
//! Only sources that have shards in the model are counted. Non-sharded sources (Kafka, etc.)
//! are ignored.

use std::num::NonZeroU32;
use std::sync::LazyLock;

use quickwit_proto::types::SourceUid;

use crate::IndexerPool;
use crate::indexing_scheduler::compute_max_num_shards_per_pipeline_for_shards;
use crate::model::{ControlPlaneModel, ShardEntry};

const DEFAULT_CPU_MILLIS_PER_PIPELINE: NonZeroU32 = NonZeroU32::new(1_000).unwrap();

/// Returns true if shard scale-up should be capped by the indexing capacity of the cluster.
pub(crate) fn is_indexing_capacity_shard_cap_enabled() -> bool {
    static IS_ENABLED: LazyLock<bool> = LazyLock::new(|| {
        quickwit_common::get_bool_from_env("QW_ENABLE_INDEXING_CAPACITY_SHARD_CAP", false)
    });
    *IS_ENABLED
}

/// Value of `QW_CPU_MILLIS_PER_PIPELINE`. A value of 0 fails to parse and is reported as an error
/// by `get_from_env`, which then uses the default.
fn cpu_millis_per_pipeline() -> NonZeroU32 {
    quickwit_common::get_from_env_cached!(
        NonZeroU32,
        "QW_CPU_MILLIS_PER_PIPELINE",
        DEFAULT_CPU_MILLIS_PER_PIPELINE,
        false
    )
}

/// Returns the max number of pipelines the indexers of the cluster can run.
fn compute_pipeline_budget(
    indexer_pool: &IndexerPool,
    cpu_millis_per_pipeline: NonZeroU32,
) -> usize {
    let total_cpu_millis: u64 = indexer_pool
        .values()
        .iter()
        .map(|indexer| indexer.indexing_capacity.cpu_millis() as u64)
        .sum();
    (total_cpu_millis / cpu_millis_per_pipeline.get() as u64) as usize
}

fn shard_entries_for_source<'a>(
    model: &'a ControlPlaneModel,
    source_uid: &SourceUid,
) -> Vec<&'a ShardEntry> {
    let Some(shards) = model.get_shards_for_source(source_uid) else {
        return Vec::new();
    };
    shards.values().collect()
}

/// Returns the number of pipelines needed to index all the shards (open and closed) of all the
/// enabled sources of the model.
///
/// Closed shards are counted because they still need to be indexed.
fn compute_num_pipelines_used(model: &ControlPlaneModel) -> usize {
    let mut num_pipelines_used = 0;
    for (source_uid, source_config) in model.source_configs() {
        if !source_config.enabled {
            continue;
        }
        let shard_entries = shard_entries_for_source(model, &source_uid);
        if shard_entries.is_empty() {
            continue;
        }
        let max_num_shards_per_pipeline =
            compute_max_num_shards_per_pipeline_for_shards(&shard_entries).get() as usize;
        num_pipelines_used += shard_entries.len().div_ceil(max_num_shards_per_pipeline);
    }
    num_pipelines_used
}

/// Clamps the number of shards to open for `source_uid` so that the pipelines implied by all the
/// shards of the cluster do not exceed the pipeline budget.
///
/// The source can use the free slots of its last pipeline, plus
/// `max_num_shards_per_pipeline` slots for each pipeline left in the budget.
///
/// If the indexer pool is empty, the budget is 0 and this function returns 0.
pub(crate) fn clamp_num_shards_to_open(
    model: &ControlPlaneModel,
    indexer_pool: &IndexerPool,
    source_uid: &SourceUid,
    num_shards_to_open: usize,
) -> usize {
    clamp_num_shards_to_open_aux(
        model,
        indexer_pool,
        source_uid,
        num_shards_to_open,
        cpu_millis_per_pipeline(),
    )
}

fn clamp_num_shards_to_open_aux(
    model: &ControlPlaneModel,
    indexer_pool: &IndexerPool,
    source_uid: &SourceUid,
    num_shards_to_open: usize,
    cpu_millis_per_pipeline: NonZeroU32,
) -> usize {
    let pipeline_budget = compute_pipeline_budget(indexer_pool, cpu_millis_per_pipeline);
    let num_pipelines_used = compute_num_pipelines_used(model);

    let shard_entries = shard_entries_for_source(model, source_uid);
    let max_num_shards_per_pipeline =
        compute_max_num_shards_per_pipeline_for_shards(&shard_entries).get() as usize;
    let num_shards = shard_entries.len();
    let num_free_slots_in_last_pipeline =
        num_shards.div_ceil(max_num_shards_per_pipeline) * max_num_shards_per_pipeline - num_shards;

    let num_pipelines_left = pipeline_budget.saturating_sub(num_pipelines_used);
    let max_num_shards_to_open =
        num_free_slots_in_last_pipeline + num_pipelines_left * max_num_shards_per_pipeline;
    num_shards_to_open.min(max_num_shards_to_open)
}

#[cfg(test)]
mod tests {
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::indexing::{CpuCapacity, IndexingServiceClient};
    use quickwit_proto::ingest::ingester::IngesterStatus;
    use quickwit_proto::ingest::{Shard, ShardState};
    use quickwit_proto::types::{IndexUid, NodeId, ShardId};

    use super::*;
    use crate::IndexerPoolEntry;

    // With the default load model (`QW_ENABLE_VARIABLE_SHARD_LOAD` unset), a shard has a load of
    // 1000 millicpus, so a pipeline hosts `3200 / 1000 = 3` shards.
    const MAX_NUM_SHARDS_PER_PIPELINE: usize = 3;

    fn cpu(cpu_millis: u32) -> NonZeroU32 {
        NonZeroU32::new(cpu_millis).unwrap()
    }

    fn indexer_pool_with_capacities(cpu_millis: &[u32]) -> IndexerPool {
        let indexer_pool = IndexerPool::default();
        for (i, &indexer_cpu_millis) in cpu_millis.iter().enumerate() {
            let node_id = NodeId::from_str(&format!("indexer-{i}"));
            let indexer = IndexerPoolEntry {
                node_id: node_id.clone(),
                generation_id: 0,
                client: IndexingServiceClient::mocked(),
                indexing_tasks: Vec::new(),
                indexing_capacity: CpuCapacity::from_cpu_millis(indexer_cpu_millis),
                ingester_status: IngesterStatus::Ready,
                availability_zone: None,
            };
            indexer_pool.insert(node_id, indexer);
        }
        indexer_pool
    }

    fn add_source_with_shards(
        model: &mut ControlPlaneModel,
        index_id: &str,
        num_open_shards: usize,
        num_closed_shards: usize,
    ) -> SourceUid {
        let index_uid = IndexUid::for_test(index_id, 0);
        let mut index_metadata =
            IndexMetadata::for_test(index_id, &format!("ram://indexes/{index_id}"));
        let source_id = "test-source".to_string();
        index_metadata.sources.insert(
            source_id.clone(),
            SourceConfig::for_test(&source_id, SourceParams::Ingest),
        );
        model.add_index(index_metadata);
        let num_shards = num_open_shards + num_closed_shards;
        let mut shards = Vec::with_capacity(num_shards);
        for shard_ord in 0..num_shards {
            let shard_state = if shard_ord < num_open_shards {
                ShardState::Open
            } else {
                ShardState::Closed
            };
            shards.push(Shard {
                index_uid: Some(index_uid.clone()),
                source_id: source_id.clone(),
                shard_id: Some(ShardId::from(shard_ord as u64)),
                ingester_id: "test-ingester".to_string(),
                shard_state: shard_state as i32,
                ..Default::default()
            });
        }
        model.insert_shards(&index_uid, &source_id, shards);
        SourceUid {
            index_uid,
            source_id,
        }
    }

    #[test]
    fn test_compute_pipeline_budget() {
        let empty_pool = IndexerPool::default();
        assert_eq!(compute_pipeline_budget(&empty_pool, cpu(1_000)), 0);

        let indexer_pool = indexer_pool_with_capacities(&[4_000, 2_500]);
        assert_eq!(compute_pipeline_budget(&indexer_pool, cpu(1_000)), 6);
        assert_eq!(compute_pipeline_budget(&indexer_pool, cpu(2_000)), 3);
    }

    #[test]
    fn test_compute_num_pipelines_used() {
        let mut model = ControlPlaneModel::default();
        assert_eq!(compute_num_pipelines_used(&model), 0);

        // 4 shards -> 2 pipelines.
        add_source_with_shards(&mut model, "index-0", 4, 0);
        assert_eq!(compute_num_pipelines_used(&model), 2);

        // Closed shards are counted: 2 + 1 shards -> 1 pipeline.
        add_source_with_shards(&mut model, "index-1", 2, 1);
        assert_eq!(compute_num_pipelines_used(&model), 3);

        // A source without shards uses no pipeline.
        add_source_with_shards(&mut model, "index-2", 0, 0);
        assert_eq!(compute_num_pipelines_used(&model), 3);
    }

    #[test]
    fn test_clamp_num_shards_to_open() {
        let mut model = ControlPlaneModel::default();
        // 4 shards -> 2 pipelines, 2 free slots in the last pipeline.
        let source_uid_0 = add_source_with_shards(&mut model, "index-0", 4, 0);
        // 3 shards -> 1 pipeline, 0 free slot.
        let source_uid_1 = add_source_with_shards(&mut model, "index-1", 3, 0);

        // Budget of 4 pipelines, 3 used, 1 left.
        let indexer_pool = indexer_pool_with_capacities(&[4_000]);
        assert_eq!(
            clamp_num_shards_to_open_aux(&model, &indexer_pool, &source_uid_0, 1, cpu(1_000)),
            1
        );
        assert_eq!(
            clamp_num_shards_to_open_aux(&model, &indexer_pool, &source_uid_0, 100, cpu(1_000)),
            2 + MAX_NUM_SHARDS_PER_PIPELINE
        );
        assert_eq!(
            clamp_num_shards_to_open_aux(&model, &indexer_pool, &source_uid_1, 100, cpu(1_000)),
            MAX_NUM_SHARDS_PER_PIPELINE
        );

        // Budget of 3 pipelines, all used. Only free slots in the last pipeline can be used.
        let indexer_pool = indexer_pool_with_capacities(&[3_000]);
        assert_eq!(
            clamp_num_shards_to_open_aux(&model, &indexer_pool, &source_uid_0, 100, cpu(1_000)),
            2
        );
        assert_eq!(
            clamp_num_shards_to_open_aux(&model, &indexer_pool, &source_uid_1, 100, cpu(1_000)),
            0
        );

        // Budget already exceeded.
        let indexer_pool = indexer_pool_with_capacities(&[1_000]);
        assert_eq!(
            clamp_num_shards_to_open_aux(&model, &indexer_pool, &source_uid_1, 100, cpu(1_000)),
            0
        );

        // Empty indexer pool.
        let indexer_pool = IndexerPool::default();
        assert_eq!(
            clamp_num_shards_to_open_aux(&model, &indexer_pool, &source_uid_1, 100, cpu(1_000)),
            0
        );
    }
}
