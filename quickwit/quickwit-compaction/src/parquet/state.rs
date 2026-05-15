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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use quickwit_parquet_engine::merge::policy::{
    CompactionScope, ParquetMergeOperation, ParquetMergePolicy,
};
use quickwit_parquet_engine::split::ParquetSplitMetadata;
use quickwit_proto::compaction::{CompactionFailure, CompactionInProgress, CompactionSuccess};
use quickwit_proto::types::{NodeId, SplitId};
use tracing::{error, info, warn};

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

struct InFlightParquetCompaction {
    split_ids: Vec<SplitId>,
    node_id: NodeId,
    last_heartbeat: Instant,
}

/// Tracks Parquet split-level state for the compaction planner.
///
/// This intentionally mirrors the Tantivy `CompactionState`, but keys splits
/// by Parquet `CompactionScope` so sort schema, partition, and time-window
/// invariants are preserved before merge policy execution.
pub struct ParquetCompactionState {
    needs_compaction: HashMap<CompactionScope, Vec<ParquetSplitMetadata>>,
    needs_compaction_split_ids: HashSet<SplitId>,
    in_flight: HashMap<String, InFlightParquetCompaction>,
    in_flight_split_ids: HashSet<SplitId>,
    pending_operations: VecDeque<(CompactionScope, ParquetMergeOperation)>,
}

impl ParquetCompactionState {
    pub fn new() -> Self {
        ParquetCompactionState {
            needs_compaction: HashMap::new(),
            needs_compaction_split_ids: HashSet::new(),
            in_flight: HashMap::new(),
            in_flight_split_ids: HashSet::new(),
            pending_operations: VecDeque::new(),
        }
    }

    pub fn is_split_known(&self, split_id: &str) -> bool {
        self.needs_compaction_split_ids.contains(split_id)
            || self.in_flight_split_ids.contains(split_id)
    }

    pub fn track_split(&mut self, split: ParquetSplitMetadata) {
        let Some(scope) = CompactionScope::from_split(&split) else {
            return;
        };
        let split_id = split.split_id.as_str().to_string();
        self.needs_compaction_split_ids.insert(split_id);
        self.needs_compaction.entry(scope).or_default().push(split);
    }

    pub fn scope_keys(&self) -> Vec<CompactionScope> {
        self.needs_compaction.keys().cloned().collect()
    }

    pub fn plan_scope(
        &mut self,
        scope: &CompactionScope,
        merge_policy: &Arc<dyn ParquetMergePolicy>,
    ) {
        let Some(splits) = self.needs_compaction.get_mut(scope) else {
            return;
        };
        for operation in merge_policy.operations(splits) {
            for split in operation.splits_as_slice() {
                let split_id = split.split_id.as_str();
                self.needs_compaction_split_ids.remove(split_id);
                self.in_flight_split_ids.insert(split_id.to_string());
            }
            self.pending_operations
                .push_back((scope.clone(), operation));
        }
        if splits.is_empty() {
            self.needs_compaction.remove(scope);
        }
    }

    pub fn process_successes(&mut self, successes: &[CompactionSuccess]) {
        for success in successes {
            if let Some(inflight) = self.in_flight.remove(&success.task_id) {
                info!(task_id=%success.task_id, "parquet compaction task completed");
                for split_id in &inflight.split_ids {
                    self.in_flight_split_ids.remove(split_id.as_str());
                }
            }
        }
    }

    pub fn process_failures(&mut self, failures: &[CompactionFailure]) {
        for failure in failures {
            if let Some(inflight) = self.in_flight.remove(&failure.task_id) {
                warn!(task_id=%failure.task_id, error=%failure.error_message, "parquet compaction task failed");
                for split_id in &inflight.split_ids {
                    self.in_flight_split_ids.remove(split_id.as_str());
                }
            }
        }
    }

    pub fn update_heartbeats(&mut self, node_id: &NodeId, in_progress: &[CompactionInProgress]) {
        for task in in_progress {
            if let Some(inflight) = self.in_flight.get_mut(&task.task_id) {
                inflight.last_heartbeat = Instant::now();
            } else {
                for split_id in &task.split_ids {
                    self.in_flight_split_ids.insert(split_id.clone());
                    self.needs_compaction_split_ids.remove(split_id.as_str());
                }
                self.in_flight.insert(
                    task.task_id.clone(),
                    InFlightParquetCompaction {
                        split_ids: task.split_ids.clone(),
                        node_id: node_id.clone(),
                        last_heartbeat: Instant::now(),
                    },
                );
            }
        }
    }

    pub fn check_heartbeat_timeouts(&mut self) {
        let now = Instant::now();
        let timed_out_task_ids: Vec<String> = self
            .in_flight
            .iter()
            .filter(|(_, inflight)| now.duration_since(inflight.last_heartbeat) > HEARTBEAT_TIMEOUT)
            .map(|(task_id, _)| task_id.clone())
            .collect();

        for task_id in timed_out_task_ids {
            if let Some(inflight) = self.in_flight.remove(&task_id) {
                error!(task_id=%task_id, node_id=%inflight.node_id, "parquet compaction task timed out");
                for split_id in &inflight.split_ids {
                    self.in_flight_split_ids.remove(split_id.as_str());
                }
            }
        }
    }

    pub fn pop_pending(&mut self, count: usize) -> Vec<(CompactionScope, ParquetMergeOperation)> {
        let count = count.min(self.pending_operations.len());
        let mut operations = Vec::with_capacity(count);
        for _ in 0..count {
            operations.push(self.pending_operations.pop_front().unwrap());
        }
        operations
    }

    pub fn record_assignment(&mut self, task_id: String, split_ids: Vec<SplitId>, node_id: NodeId) {
        self.in_flight.insert(
            task_id,
            InFlightParquetCompaction {
                split_ids,
                node_id,
                last_heartbeat: Instant::now(),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::{Duration, SystemTime};

    use quickwit_parquet_engine::merge::policy::{
        ConstWriteAmplificationParquetMergePolicy, ParquetMergePolicyConfig, ParquetSplitMaturity,
    };
    use quickwit_parquet_engine::split::{
        ParquetSplitId, ParquetSplitKind, ParquetSplitMetadata, TimeRange,
    };
    use quickwit_proto::compaction::CompactionTaskKind;

    use super::*;

    fn test_merge_policy() -> Arc<dyn ParquetMergePolicy> {
        Arc::new(ConstWriteAmplificationParquetMergePolicy::new(
            ParquetMergePolicyConfig {
                merge_factor: 2,
                max_merge_factor: 2,
                target_split_size_bytes: 256 * 1024 * 1024,
                maturation_period: Duration::from_secs(3600),
                ..Default::default()
            },
        ))
    }

    fn test_split(split_id: &str, window_start: Option<i64>) -> ParquetSplitMetadata {
        let mut split = ParquetSplitMetadata {
            kind: ParquetSplitKind::Metrics,
            split_id: ParquetSplitId::new(split_id),
            index_uid: "test-index:00000000000000000000000000".to_string(),
            partition_id: 0,
            time_range: TimeRange::new(1000, 2000),
            num_rows: 100,
            size_bytes: 1_000_000,
            metric_names: HashSet::new(),
            low_cardinality_tags: Default::default(),
            high_cardinality_tag_keys: Default::default(),
            created_at: SystemTime::now(),
            maturity: ParquetSplitMaturity::Mature,
            parquet_file: format!("{split_id}.parquet"),
            window: None,
            sort_fields: "metric_name|host|timestamp_secs/V2".to_string(),
            num_merge_ops: 0,
            row_keys_proto: None,
            zonemap_regexes: Default::default(),
        };
        split.window = window_start.map(|start| start..start + 3600);
        split
    }

    #[test]
    fn test_track_split_skips_splits_without_scope() {
        let mut state = ParquetCompactionState::new();

        state.track_split(test_split("s1", None));

        assert!(!state.is_split_known("s1"));
        assert!(state.scope_keys().is_empty());
    }

    #[test]
    fn test_plan_scope_moves_splits_to_in_flight() {
        let mut state = ParquetCompactionState::new();
        let merge_policy = test_merge_policy();

        state.track_split(test_split("s1", Some(0)));
        state.track_split(test_split("s2", Some(0)));

        let scopes = state.scope_keys();
        assert_eq!(scopes.len(), 1);
        state.plan_scope(&scopes[0], &merge_policy);

        assert_eq!(state.pending_operations.len(), 1);
        assert!(!state.needs_compaction_split_ids.contains("s1"));
        assert!(!state.needs_compaction_split_ids.contains("s2"));
        assert!(state.in_flight_split_ids.contains("s1"));
        assert!(state.in_flight_split_ids.contains("s2"));
    }

    #[test]
    fn test_process_success_clears_in_flight_ids() {
        let node_id = NodeId::from("worker-1");
        let mut state = ParquetCompactionState::new();
        state.in_flight_split_ids.insert("s1".to_string());
        state.record_assignment("task-1".to_string(), vec!["s1".to_string()], node_id);

        state.process_successes(&[CompactionSuccess {
            task_id: "task-1".to_string(),
            merged_split_id: "merged".to_string(),
            task_kind: CompactionTaskKind::Parquet as i32,
        }]);

        assert!(!state.is_split_known("s1"));
    }
}
