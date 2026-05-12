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

use quickwit_indexing::merge_policy::{MergeOperation, MergePolicy};
use quickwit_metastore::SplitMetadata;
use quickwit_proto::compaction::{CompactionFailure, CompactionInProgress, CompactionSuccess};
use quickwit_proto::types::{DocMappingUid, IndexUid, NodeId, SourceId, SplitId};
use tracing::{error, info, warn};

use crate::TaskId;

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompactionPartitionKey {
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub partition_id: u64,
    pub doc_mapping_uid: DocMappingUid,
}

impl CompactionPartitionKey {
    pub fn from_split(split: &SplitMetadata) -> Self {
        CompactionPartitionKey {
            index_uid: split.index_uid.clone(),
            source_id: split.source_id.clone(),
            partition_id: split.partition_id,
            doc_mapping_uid: split.doc_mapping_uid,
        }
    }
}

struct InFlightCompaction {
    task_id: TaskId,
    split_ids: Vec<SplitId>,
    node_id: NodeId,
    last_heartbeat: Instant,
}

/// Tracks all split-level state for the compaction planner:
/// which splits need compaction, which are in-flight, and which
/// operations are pending assignment to workers.
pub struct CompactionState {
    needs_compaction: HashMap<CompactionPartitionKey, Vec<SplitMetadata>>,
    needs_compaction_split_ids: HashSet<SplitId>,
    in_flight: HashMap<TaskId, InFlightCompaction>,
    in_flight_split_ids: HashSet<SplitId>,
    /// TODO: add index_uid and source_id to MergeOperation so we don't need the partition key
    /// here.
    pending_operations: VecDeque<(CompactionPartitionKey, MergeOperation)>,
}

impl CompactionState {
    pub fn new() -> Self {
        CompactionState {
            needs_compaction: HashMap::new(),
            needs_compaction_split_ids: HashSet::new(),
            in_flight: HashMap::new(),
            in_flight_split_ids: HashSet::new(),
            pending_operations: VecDeque::new(),
        }
    }

    /// Returns true if the split is already tracked (either awaiting compaction or in-flight).
    pub fn is_split_tracked(&self, split_id: &str) -> bool {
        self.needs_compaction_split_ids.contains(split_id)
            || self.in_flight_split_ids.contains(split_id)
    }

    /// Adds a split to the needs_compaction set.
    pub fn track_split(&mut self, split: SplitMetadata) {
        let split_id = split.split_id().to_string();
        let key = CompactionPartitionKey::from_split(&split);
        self.needs_compaction_split_ids.insert(split_id);
        self.needs_compaction.entry(key).or_default().push(split);
    }

    /// Returns the partition keys for iteration. The caller drives the loop.
    pub fn partition_keys(&self) -> Vec<CompactionPartitionKey> {
        self.needs_compaction.keys().cloned().collect()
    }

    /// Runs a merge policy on a single partition and queues the resulting operations.
    pub fn plan_partition(
        &mut self,
        partition_key: &CompactionPartitionKey,
        merge_policy: &Arc<dyn MergePolicy>,
    ) {
        let Some(splits) = self.needs_compaction.get_mut(partition_key) else {
            return;
        };
        for operation in merge_policy.operations(splits) {
            for split in operation.splits_as_slice() {
                self.needs_compaction_split_ids.remove(split.split_id());
                self.in_flight_split_ids
                    .insert(split.split_id().to_string());
            }
            self.pending_operations
                .push_back((partition_key.clone(), operation));
        }
        if splits.is_empty() {
            self.needs_compaction.remove(partition_key);
        }
    }

    pub fn process_successes(&mut self, successes: &[CompactionSuccess]) {
        for success in successes {
            if let Some(inflight) = self.in_flight.remove(&success.task_id) {
                info!(task_id=%success.task_id, "compaction task completed");
                for split_id in &inflight.split_ids {
                    self.in_flight_split_ids.remove(split_id.as_str());
                }
            }
        }
    }

    pub fn process_failures(&mut self, failures: &[CompactionFailure]) {
        for failure in failures {
            if let Some(inflight) = self.in_flight.remove(&failure.task_id) {
                warn!(task_id=%failure.task_id, error=%failure.error_message, "compaction task failed");
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
                // Task not tracked — start tracking it. Happens when
                // workers report tasks the planner doesn't know about yet
                // (e.g. after planner start).
                for split_id in &task.split_ids {
                    self.in_flight_split_ids.insert(split_id.clone());
                    self.needs_compaction_split_ids.remove(split_id.as_str());
                }
                self.in_flight.insert(
                    task.task_id.clone(),
                    InFlightCompaction {
                        task_id: task.task_id.clone(),
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
        let timed_out_task_ids: Vec<TaskId> = self
            .in_flight
            .iter()
            .filter(|(_, inflight)| now.duration_since(inflight.last_heartbeat) > HEARTBEAT_TIMEOUT)
            .map(|(task_id, _)| task_id.clone())
            .collect();

        for task_id in timed_out_task_ids {
            if let Some(inflight) = self.in_flight.remove(&task_id) {
                error!(%task_id, node_id=%inflight.node_id, "compaction task timed out");
                for split_id in &inflight.split_ids {
                    self.in_flight_split_ids.remove(split_id.as_str());
                }
            }
        }
    }

    /// Pops up to `count` pending operations for assignment.
    pub fn pop_pending(&mut self, count: usize) -> Vec<(CompactionPartitionKey, MergeOperation)> {
        let count = count.min(self.pending_operations.len());
        let mut operations = Vec::with_capacity(count);
        for _ in 0..count {
            operations.push(self.pending_operations.pop_front().unwrap());
        }
        operations
    }

    /// Records that an operation has been assigned to a worker.
    pub fn record_assignment(&mut self, task_id: TaskId, split_ids: Vec<SplitId>, node_id: NodeId) {
        self.in_flight.insert(
            task_id.clone(),
            InFlightCompaction {
                task_id,
                split_ids,
                node_id,
                last_heartbeat: Instant::now(),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quickwit_config::IndexingSettings;
    use quickwit_config::merge_policy_config::{
        ConstWriteAmplificationMergePolicyConfig, MergePolicyConfig,
    };
    use quickwit_indexing::merge_policy::merge_policy_from_settings;
    use quickwit_proto::types::IndexUid;

    use super::*;

    fn test_merge_policy() -> Arc<dyn MergePolicy> {
        let settings = IndexingSettings {
            merge_policy: MergePolicyConfig::ConstWriteAmplification(
                ConstWriteAmplificationMergePolicyConfig {
                    merge_factor: 2,
                    max_merge_factor: 2,
                    ..Default::default()
                },
            ),
            ..Default::default()
        };
        merge_policy_from_settings(&settings)
    }

    fn test_split(split_id: &str, index_uid: &IndexUid) -> SplitMetadata {
        SplitMetadata {
            split_id: split_id.to_string(),
            index_uid: index_uid.clone(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            num_docs: 1000,
            create_timestamp: time::OffsetDateTime::now_utc().unix_timestamp(),
            maturity: quickwit_metastore::SplitMaturity::Immature {
                maturation_period: Duration::from_secs(3600),
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_track_and_is_known() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let mut state = CompactionState::new();

        assert!(!state.is_split_tracked("s1"));

        state.track_split(test_split("s1", &index_uid));
        assert!(state.is_split_tracked("s1"));
        assert!(!state.is_split_tracked("s2"));
    }

    #[test]
    fn test_track_split_partitions_correctly() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let mut state = CompactionState::new();

        state.track_split(test_split("s1", &index_uid));
        state.track_split(test_split("s2", &index_uid));

        assert_eq!(state.partition_keys().len(), 1);
    }

    #[test]
    fn test_plan_partition_moves_splits_to_in_flight() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let merge_policy = test_merge_policy();
        let mut state = CompactionState::new();

        state.track_split(test_split("s0", &index_uid));
        state.track_split(test_split("s1", &index_uid));

        let keys = state.partition_keys();
        assert_eq!(keys.len(), 1);

        state.plan_partition(&keys[0], &merge_policy);

        // Splits moved from needs_compaction to in_flight.
        assert!(!state.pending_operations.is_empty());
        for (_, op) in &state.pending_operations {
            for split in op.splits_as_slice() {
                assert!(!state.needs_compaction_split_ids.contains(split.split_id()));
                assert!(state.in_flight_split_ids.contains(split.split_id()));
            }
        }
    }

    #[test]
    fn test_process_successes_and_failures_clear_in_flight() {
        let node_id = NodeId::from("worker-1");
        let mut state = CompactionState::new();

        // Simulate what plan_partition + record_assignment does:
        // split IDs go into in_flight_split_ids, then into InFlightCompaction.
        state.in_flight_split_ids.insert("s1".to_string());
        state.in_flight_split_ids.insert("s2".to_string());
        state.record_assignment(
            "task-1".to_string(),
            vec!["s1".to_string(), "s2".to_string()],
            node_id.clone(),
        );

        state.in_flight_split_ids.insert("s3".to_string());
        state.record_assignment("task-2".to_string(), vec!["s3".to_string()], node_id);

        assert!(state.is_split_tracked("s1"));
        assert!(state.is_split_tracked("s3"));

        state.process_successes(&[CompactionSuccess {
            task_id: "task-1".to_string(),
        }]);
        assert!(!state.is_split_tracked("s1"));
        assert!(!state.is_split_tracked("s2"));

        state.process_failures(&[CompactionFailure {
            task_id: "task-2".to_string(),
            error_message: "boom".to_string(),
        }]);
        assert!(!state.is_split_tracked("s3"));
    }

    #[test]
    fn test_update_heartbeats_adopts_unknown_tasks() {
        let node_id = NodeId::from("worker-1");
        let mut state = CompactionState::new();

        // Simulate a split that was tracked as needing compaction.
        let index_uid = IndexUid::for_test("test-index", 0);
        state.track_split(test_split("s1", &index_uid));
        assert!(state.needs_compaction_split_ids.contains("s1"));

        // Worker reports an in-progress task the planner doesn't know about.
        state.update_heartbeats(
            &node_id,
            &[CompactionInProgress {
                task_id: "task-1".to_string(),
                index_uid: Some(index_uid),
                source_id: "test-source".to_string(),
                split_ids: vec!["s1".to_string()],
            }],
        );

        // Split moved from needs_compaction to in_flight.
        assert!(state.in_flight_split_ids.contains("s1"));
        assert!(!state.needs_compaction_split_ids.contains("s1"));
    }

    #[test]
    fn test_pop_pending_and_record_assignment() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let merge_policy = test_merge_policy();
        let mut state = CompactionState::new();

        state.track_split(test_split("s0", &index_uid));
        state.track_split(test_split("s1", &index_uid));

        let keys = state.partition_keys();
        state.plan_partition(&keys[0], &merge_policy);

        let pending = state.pop_pending(1);
        assert_eq!(pending.len(), 1);

        let (_, operation) = &pending[0];
        let split_ids: Vec<String> = operation
            .splits_as_slice()
            .iter()
            .map(|s| s.split_id().to_string())
            .collect();

        // Splits are already in in_flight_split_ids from plan_partition.
        state.record_assignment("task-1".to_string(), split_ids.clone(), NodeId::from("w1"));

        for split_id in &split_ids {
            assert!(state.is_split_tracked(split_id));
        }
    }

    #[test]
    fn test_pop_pending_respects_count() {
        let index_uid = IndexUid::for_test("test-index", 0);
        let merge_policy = test_merge_policy();
        let mut state = CompactionState::new();

        // With merge_factor=2, 4 splits produces 2 operations.
        for i in 0..4 {
            state.track_split(test_split(&format!("s{i}"), &index_uid));
        }

        let keys = state.partition_keys();
        state.plan_partition(&keys[0], &merge_policy);

        let total_pending = state.pending_operations.len();
        assert!(total_pending >= 2);

        let popped = state.pop_pending(1);
        assert_eq!(popped.len(), 1);
        assert_eq!(state.pending_operations.len(), total_pending - 1);

        // Asking for more than available returns what's there.
        let rest = state.pop_pending(100);
        assert_eq!(rest.len(), total_pending - 1);
        assert!(state.pending_operations.is_empty());
    }
}
