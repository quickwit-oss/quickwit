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

//! Measured load of non-sharded sources (Kafka, NATS, Pulsar, ...).
//!
//! The scheduler historically weighs every pipeline of a non-sharded source as a full
//! pipeline ([`PIPELINE_FULL_CAPACITY`]), whatever its traffic. Pipelines are therefore
//! balanced by count, and an indexer that happens to host the busy tenants is overloaded while
//! its neighbors sit idle.
//!
//! Cooperative indexing pipelines measure the share of each commit period they spend holding an
//! indexing permit and gossip it as [`PipelineMetrics`]. The [`PipelineLoadTracker`] turns those
//! reports into a load per pipeline for each source, so that the scheduler can balance non-sharded
//! sources by measured load.
//!
//! The tracker is deliberately conservative:
//! - loads are quantized to [`PIPELINE_LOAD_QUANTUM`] so that small fluctuations do not trigger a
//!   plan rebuild;
//! - a source keeps its last known load while none of its pipelines reports metrics, which is the
//!   case for the first commit period after a pipeline is (re)started on another indexer;
//! - a source that never reported anything, or whose pipelines run without cooperative indexing, is
//!   weighed as a full pipeline, which is the legacy behavior.
//!
//! The feature is off by default and enabled with `QW_ENABLE_VARIABLE_PIPELINE_LOAD=true`.

use std::num::NonZeroU32;
use std::sync::LazyLock;

use fnv::FnvHashMap;
use quickwit_proto::indexing::{CpuCapacity, PIPELINE_FULL_CAPACITY};
use quickwit_proto::types::SourceUid;

use crate::IndexerPoolEntry;

/// Loads are rounded up to a multiple of this value (10% of a full pipeline) before being
/// compared and handed to the scheduler, to dampen plan churn.
const PIPELINE_LOAD_QUANTUM: CpuCapacity = CpuCapacity::from_cpu_millis(400u32);

/// A pipeline is never weighed below this value, so that an idle tenant still counts for the
/// actors, merge pipeline, and connection it keeps alive.
const MIN_PIPELINE_LOAD: CpuCapacity = PIPELINE_LOAD_QUANTUM;

pub fn is_variable_pipeline_load_enabled() -> bool {
    static IS_VARIABLE_PIPELINE_LOAD_ENABLED: LazyLock<bool> = LazyLock::new(|| {
        quickwit_common::get_bool_from_env_opt("QW_ENABLE_VARIABLE_PIPELINE_LOAD").unwrap_or(false)
    });
    *IS_VARIABLE_PIPELINE_LOAD_ENABLED
}

/// Rounds a measured load up to the next quantum and clamps it to
/// `[MIN_PIPELINE_LOAD, PIPELINE_FULL_CAPACITY]`.
fn quantize_load(cpu_load: CpuCapacity) -> NonZeroU32 {
    let quantum = PIPELINE_LOAD_QUANTUM.cpu_millis();
    let quantized_millis = cpu_load
        .cpu_millis()
        .div_ceil(quantum)
        .saturating_mul(quantum)
        .clamp(
            MIN_PIPELINE_LOAD.cpu_millis(),
            PIPELINE_FULL_CAPACITY.cpu_millis(),
        );
    NonZeroU32::new(quantized_millis).expect("MIN_PIPELINE_LOAD is positive")
}

/// Tracks the measured load per pipeline of non-sharded sources.
///
/// Hidden contract: the tracker is refreshed from the indexer pool before each plan rebuild and
/// on each control loop iteration. The loads it returns are the ones that were used to build the
/// last plan until the next refresh.
#[derive(Debug)]
pub struct PipelineLoadTracker {
    enabled: bool,
    load_per_pipeline: FnvHashMap<SourceUid, NonZeroU32>,
}

impl Default for PipelineLoadTracker {
    fn default() -> Self {
        PipelineLoadTracker::new(is_variable_pipeline_load_enabled())
    }
}

impl PipelineLoadTracker {
    pub fn new(enabled: bool) -> PipelineLoadTracker {
        PipelineLoadTracker {
            enabled,
            load_per_pipeline: FnvHashMap::default(),
        }
    }

    pub fn disabled() -> PipelineLoadTracker {
        PipelineLoadTracker::new(false)
    }

    /// Load to assign to each pipeline of the given source.
    ///
    /// Falls back to a full pipeline when the feature is disabled or when the source never
    /// reported metrics.
    pub fn load_per_pipeline(&self, source_uid: &SourceUid) -> NonZeroU32 {
        if !self.enabled {
            return full_pipeline_load();
        }
        self.load_per_pipeline
            .get(source_uid)
            .copied()
            .unwrap_or_else(full_pipeline_load)
    }

    /// Refreshes the loads from the metrics gossiped by the indexers.
    ///
    /// The load of a source is the average of the loads reported by the indexers running one of
    /// its pipelines. Sources without any report keep their last known load.
    ///
    /// Returns `true` if the load of at least one source changed.
    pub fn refresh(&mut self, indexers: &[IndexerPoolEntry]) -> bool {
        if !self.enabled {
            return false;
        }
        let mut cpu_load_sum_and_count: FnvHashMap<&SourceUid, (u64, u64)> = FnvHashMap::default();
        for indexer in indexers {
            for (source_uid, pipeline_metrics) in &indexer.pipeline_metrics {
                let (cpu_load_sum, count) = cpu_load_sum_and_count.entry(source_uid).or_default();
                *cpu_load_sum += pipeline_metrics.cpu_load.cpu_millis() as u64;
                *count += 1;
            }
        }
        let mut changed = false;
        for (source_uid, (cpu_load_sum, count)) in cpu_load_sum_and_count {
            let average_cpu_millis = (cpu_load_sum / count) as u32;
            let load = quantize_load(CpuCapacity::from_cpu_millis(average_cpu_millis));
            let previous_load = self.load_per_pipeline.insert(source_uid.clone(), load);
            if previous_load != Some(load) {
                changed = true;
            }
        }
        changed
    }

    /// Forgets the sources that are no longer scheduled.
    pub fn retain_sources(&mut self, is_scheduled: impl Fn(&SourceUid) -> bool) {
        self.load_per_pipeline
            .retain(|source_uid, _load| is_scheduled(source_uid));
    }
}

fn full_pipeline_load() -> NonZeroU32 {
    NonZeroU32::new(PIPELINE_FULL_CAPACITY.cpu_millis()).expect("full capacity is positive")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use quickwit_proto::indexing::{IndexingServiceClient, MockIndexingService, PipelineMetrics};
    use quickwit_proto::ingest::ingester::IngesterStatus;
    use quickwit_proto::types::{IndexUid, NodeId};

    use super::*;

    fn source_uid(index_uid: &IndexUid, source_id: &str) -> SourceUid {
        SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_id.to_string(),
        }
    }

    fn indexer_with_metrics(node_id: &str, metrics: Vec<(SourceUid, u32)>) -> IndexerPoolEntry {
        let pipeline_metrics: BTreeMap<SourceUid, PipelineMetrics> = metrics
            .into_iter()
            .map(|(source_uid, cpu_millis)| {
                let pipeline_metrics = PipelineMetrics {
                    cpu_load: CpuCapacity::from_cpu_millis(cpu_millis),
                    throughput_mb_per_sec: 0,
                };
                (source_uid, pipeline_metrics)
            })
            .collect();
        IndexerPoolEntry {
            node_id: NodeId::from_str(node_id),
            generation_id: 0,
            client: IndexingServiceClient::from_mock(MockIndexingService::new()),
            indexing_tasks: Vec::new(),
            indexing_capacity: PIPELINE_FULL_CAPACITY,
            ingester_status: IngesterStatus::Ready,
            availability_zone: None,
            pipeline_metrics,
        }
    }

    #[test]
    fn test_quantize_load() {
        assert_eq!(quantize_load(CpuCapacity::from_cpu_millis(0)).get(), 400);
        assert_eq!(quantize_load(CpuCapacity::from_cpu_millis(1)).get(), 400);
        assert_eq!(quantize_load(CpuCapacity::from_cpu_millis(400)).get(), 400);
        assert_eq!(quantize_load(CpuCapacity::from_cpu_millis(401)).get(), 800);
        assert_eq!(
            quantize_load(CpuCapacity::from_cpu_millis(2_500)).get(),
            2_800
        );
        assert_eq!(
            quantize_load(CpuCapacity::from_cpu_millis(4_000)).get(),
            4_000
        );
        assert_eq!(
            quantize_load(CpuCapacity::from_cpu_millis(9_999)).get(),
            4_000
        );
    }

    #[test]
    fn test_disabled_tracker_always_returns_full_pipeline() {
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let busy = source_uid(&index_uid, "busy");
        let mut tracker = PipelineLoadTracker::disabled();
        let indexers = vec![indexer_with_metrics("indexer-1", vec![(busy.clone(), 100)])];
        assert!(!tracker.refresh(&indexers));
        assert_eq!(tracker.load_per_pipeline(&busy).get(), 4_000);
    }

    #[test]
    fn test_tracker_averages_quantizes_and_remembers() {
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let busy = source_uid(&index_uid, "busy");
        let idle = source_uid(&index_uid, "idle");
        let unknown = source_uid(&index_uid, "unknown");
        let mut tracker = PipelineLoadTracker::new(true);

        // Unknown sources are weighed as full pipelines.
        assert_eq!(tracker.load_per_pipeline(&busy).get(), 4_000);

        let indexers = vec![
            indexer_with_metrics("indexer-1", vec![(busy.clone(), 3_000), (idle.clone(), 0)]),
            indexer_with_metrics("indexer-2", vec![(busy.clone(), 1_000)]),
        ];
        assert!(tracker.refresh(&indexers));
        // (3000 + 1000) / 2 = 2000, already a multiple of the quantum.
        assert_eq!(tracker.load_per_pipeline(&busy).get(), 2_000);
        assert_eq!(tracker.load_per_pipeline(&idle).get(), 400);
        assert_eq!(tracker.load_per_pipeline(&unknown).get(), 4_000);

        // Fluctuations within a quantum do not count as a change.
        let indexers = vec![
            indexer_with_metrics("indexer-1", vec![(busy.clone(), 2_900), (idle.clone(), 50)]),
            indexer_with_metrics("indexer-2", vec![(busy.clone(), 1_000)]),
        ];
        assert!(!tracker.refresh(&indexers));
        assert_eq!(tracker.load_per_pipeline(&busy).get(), 2_000);

        // A source that stops reporting (pipeline restarting elsewhere) keeps its last load.
        let indexers = vec![indexer_with_metrics("indexer-2", vec![(idle.clone(), 0)])];
        assert!(!tracker.refresh(&indexers));
        assert_eq!(tracker.load_per_pipeline(&busy).get(), 2_000);

        // Crossing a quantum boundary is a change.
        let indexers = vec![indexer_with_metrics(
            "indexer-2",
            vec![(busy.clone(), 2_001)],
        )];
        assert!(tracker.refresh(&indexers));
        assert_eq!(tracker.load_per_pipeline(&busy).get(), 2_400);

        // Deleted sources are forgotten.
        tracker.retain_sources(|source_uid| *source_uid == idle);
        assert_eq!(tracker.load_per_pipeline(&busy).get(), 4_000);
        assert_eq!(tracker.load_per_pipeline(&idle).get(), 400);
    }
}
