// Copyright (C) 2022 Quickwit, Inc.
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

use std::cmp::Ordering;
use std::collections::HashMap;

use itertools::Itertools;
use quickwit_config::{SourceConfig, CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID};
use quickwit_proto::indexing_api::IndexingTask;
use quickwit_proto::ClusterMember;

pub(crate) type NodeId = String;

#[derive(Debug, PartialEq)]
pub(crate) struct PhysicalIndexingPlan {
    indexing_tasks_per_indexer: HashMap<NodeId, Vec<IndexingTask>>,
    // Metastore version.
    metastore_version: u64,
}

impl PhysicalIndexingPlan {
    pub fn new(node_ids: Vec<NodeId>) -> Self {
        assert!(!node_ids.is_empty());
        let indexing_tasks_per_indexer = node_ids
            .into_iter()
            .map(|node_id| (node_id, Vec::new()))
            .collect();
        Self {
            indexing_tasks_per_indexer,
            metastore_version: 0,
        }
    }

    pub fn assign_indexing_task(&mut self, node_id: &NodeId, indexing_task: IndexingTask) {
        let node_indexing_tasks = self
            .indexing_tasks_per_indexer
            .get_mut(node_id)
            .expect("The node is always present.");
        node_indexing_tasks.push(indexing_task);
    }

    pub fn num_indexing_tasks_for_node(&self, node_id: &NodeId) -> usize {
        self.indexing_tasks_per_indexer
            .get(node_id)
            .map(|tasks| tasks.len())
            .unwrap_or(0)
    }

    pub fn num_indexing_tasks_for(&self, node_id: &str, index_id: &str, source_id: &str) -> usize {
        self.indexing_tasks_per_indexer
            .get(node_id)
            .map(|tasks| {
                tasks
                    .iter()
                    .filter(|task| task.index_id == index_id && task.source_id == source_id)
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn num_indexing_tasks_mean(&self) -> f32 {
        let num_indexers = self.indexing_tasks_per_indexer.len();
        let num_indexing_tasks: usize = self
            .indexing_tasks_per_indexer
            .values()
            .map(|tasks| tasks.len())
            .sum();
        // This is safe as we make sure that we have at least on indexer when calling `new` method.
        num_indexing_tasks as f32 / num_indexers as f32
    }

    pub fn indexing_tasks_per_indexer(&self) -> &HashMap<NodeId, Vec<IndexingTask>> {
        &self.indexing_tasks_per_indexer
    }
}

#[derive(Debug)]
struct NodeScore<'a> {
    pub node_id: &'a String,
    pub score: f32,
}

impl<'a> PartialEq for NodeScore<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl<'a> Eq for NodeScore<'a> {}

// Sort by score and then node_id.
impl<'a> PartialOrd for NodeScore<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.score, self.node_id).partial_cmp(&(other.score, other.node_id))
    }
}

impl<'a> Ord for NodeScore<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal) // <- Should never happen as the score
                                                           // can never be NaN. TODO: use
                                                           // `.expect()` instead?
    }
}

/// Build a [`PhysicalIndexingPlan`] by assigning each indexing tasks to a node ID.
/// The algorithm follows these steps:
/// - For each indexing tasks: 1. Select node candidates that can run the task, see `get_candidates`
///   function. 2. For each node, compute a score for this task, the higher, the better, see
///   `compute_node_score` function. 3. Select the best node (highest score) and assign the task to
///   this node.
/// Additional notes: it's nice to have the cluster members as they contain the running tasks. We
/// can potentially use this info to assign an indexing task to a node running the same task.
pub(crate) fn build_physical_indexing_plan(
    indexers: &[ClusterMember],
    source_configs: &HashMap<IndexSourceId, SourceConfig>,
    mut indexing_tasks: Vec<IndexingTask>,
) -> PhysicalIndexingPlan {
    // Sort by (index_id, source_id)...
    indexing_tasks.sort_by(|left, right| {
        (&left.index_id, &left.source_id).cmp(&(&right.index_id, &right.source_id))
    });
    // ...and sort by node IDs to make the plan building deterministic.
    let node_ids = indexers
        .iter()
        .map(|indexer| indexer.node_id.to_string())
        .sorted()
        .collect_vec();

    // Build the plan.
    let mut plan = PhysicalIndexingPlan::new(node_ids.clone());
    for indexing_task in indexing_tasks {
        // Get candidates.
        let source_config = source_configs
            .get(&IndexSourceId::from(indexing_task.clone()))
            .expect("SourceConfig is always present.");
        let candidates = select_candidates(&node_ids, &plan, source_config, &indexing_task);

        if candidates.is_empty() {
            // TODO: prevent this case with an assert at the beginning of the function.
            tracing::warn!(indexing_task=?indexing_task, "Cannot schedule the task: no indexer available, this should never happen.");
            continue;
        }
        // Score each candidate and select the best node.
        let best_node_score = candidates
            .iter()
            .map(|node_id| NodeScore {
                node_id,
                score: -compute_node_score(node_id, &plan),
            })
            .sorted()
            .next()
            .expect("There is always at least one candidate");

        plan.assign_indexing_task(best_node_score.node_id, indexing_task);
    }

    plan
}

// TODO: add docs.
// We remove nodes where they max_num_pipelines_per_indexer is already reached.
// We assume that node_ids is sorted.
fn select_candidates<'a>(
    node_ids: &'a [NodeId],
    physical_plan: &PhysicalIndexingPlan,
    source_config: &SourceConfig,
    indexing_task: &IndexingTask,
) -> Vec<&'a NodeId> {
    node_ids
        .iter()
        .filter(|node_id| {
            physical_plan.num_indexing_tasks_for(
                node_id,
                &indexing_task.index_id,
                &indexing_task.source_id,
            ) < source_config.max_num_pipelines_per_indexer()
        })
        .collect_vec()
}

// TODO: add docs.
// Scoring function:
fn compute_node_score(node_id: &NodeId, physical_plan: &PhysicalIndexingPlan) -> f32 {
    // TODO: normalize the score?
    physical_plan.num_indexing_tasks_mean()
        - physical_plan.num_indexing_tasks_for_node(node_id) as f32
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IndexSourceId {
    pub index_id: String,
    pub source_id: String,
}

impl From<IndexingTask> for IndexSourceId {
    fn from(indexing_task: IndexingTask) -> Self {
        Self {
            index_id: indexing_task.index_id,
            source_id: indexing_task.source_id,
        }
    }
}

// TODO: add docs.
pub(crate) fn build_indexing_plan(
    indexers: &[ClusterMember],
    source_configs: &HashMap<IndexSourceId, SourceConfig>,
) -> Vec<IndexingTask> {
    let mut indexing_tasks: Vec<IndexingTask> = Vec::new();
    for (index_source_id, source_config) in source_configs
        .iter()
        .sorted_by(|(left, _), (right, _)| left.cmp(right))
    {
        if !source_config.enabled {
            continue;
        }
        if source_config.source_id == CLI_INGEST_SOURCE_ID {
            continue;
        }
        // We don't know where the file and assign an indexing task to an indexer will probably fail
        // as the file won't be present locally.
        // We thus ignore file sources.
        // TODO: what should we do with file source?
        if source_config.source_type() == "file" {
            continue;
        }
        let num_pipelines = if source_config.source_id == INGEST_API_SOURCE_ID {
            // We do not handle yet the forward of messages posted on the ingest API to indexers
            // that started the pipeline. In the meantime, we just start an ingest API
            // pipeline on each indexer.
            indexers.len()
        } else {
            std::cmp::min(
                source_config.desired_num_pipelines(),
                source_config.max_num_pipelines_per_indexer() * indexers.len(),
            )
        };
        for _ in 0..num_pipelines {
            indexing_tasks.push(IndexingTask {
                index_id: index_source_id.index_id.clone(),
                source_id: index_source_id.source_id.clone(),
            });
        }
    }
    indexing_tasks
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;

    use itertools::Itertools;
    use quickwit_common::service::QuickwitService;
    use quickwit_config::{KafkaSourceParams, SourceConfig, SourceParams, CLI_INGEST_SOURCE_ID};
    use quickwit_proto::indexing_api::IndexingTask;
    use quickwit_proto::ClusterMember;
    use serde_json::json;

    use super::{build_physical_indexing_plan, IndexSourceId};
    use crate::indexing_plan::build_indexing_plan;

    struct SourceConfigForTest {
        pub index_id: String,
        pub source_id: String,
        pub max_num_pipelines_per_indexer: usize,
        pub desired_num_pipelines: usize,
        pub enabled: bool,
        // pub source_type_str: Option<String>,
    }

    fn create_sources_for_test(
        source_configs_for_test: Vec<SourceConfigForTest>,
    ) -> HashMap<IndexSourceId, SourceConfig> {
        let mut source_configs: HashMap<IndexSourceId, SourceConfig> = HashMap::new();
        for source_config_for_test in source_configs_for_test.iter() {
            let index_id = source_config_for_test.index_id.to_string();
            let source_id = source_config_for_test.source_id.to_string();
            let index_source_id = IndexSourceId {
                index_id,
                source_id: source_id.to_string(),
            };
            let source_config = SourceConfig {
                enabled: source_config_for_test.enabled,
                source_id,
                max_num_pipelines_per_indexer: source_config_for_test.max_num_pipelines_per_indexer,
                desired_num_pipelines: source_config_for_test.desired_num_pipelines,
                source_params: SourceParams::Kafka(KafkaSourceParams {
                    topic: "topic".to_string(),
                    client_log_level: None,
                    client_params: json!({
                        "bootstrap.servers": "localhost:9092",
                    }),
                    enable_backfill_mode: true,
                }),
            };
            source_configs.insert(index_source_id, source_config);
        }
        source_configs
    }

    fn cluster_members_for_test(
        num_members: usize,
        quickwit_service: QuickwitService,
    ) -> Vec<ClusterMember> {
        let mut members = Vec::new();
        for idx in 0..num_members {
            let addr: SocketAddr = ([127, 0, 0, 1], 10).into();
            members.push(ClusterMember::new(
                idx.to_string(),
                0,
                HashSet::from_iter([quickwit_service].into_iter()),
                addr,
                addr,
                Vec::new(),
            ))
        }
        members
    }

    #[test]
    fn test_build_indexing_plan_one_source() {
        let indexers = cluster_members_for_test(4, QuickwitService::Indexer);
        let index_id = "one-source-index".to_string();
        let source_id = "source-0".to_string();
        let source_configs = create_sources_for_test(vec![SourceConfigForTest {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
            max_num_pipelines_per_indexer: 1,
            desired_num_pipelines: 3,
            enabled: true,
        }]);
        let indexing_tasks = build_indexing_plan(&indexers, &source_configs);

        assert_eq!(indexing_tasks.len(), 3);
        for indexing_task in indexing_tasks {
            assert_eq!(
                indexing_task,
                IndexingTask {
                    index_id: index_id.to_string(),
                    source_id: source_id.to_string(),
                }
            );
        }
    }

    #[test]
    fn test_build_indexing_plan_with_sources_to_ignore() {
        let indexers = cluster_members_for_test(4, QuickwitService::Indexer);
        let index_id = "one-source-index".to_string();
        let source_configs = create_sources_for_test(vec![
            SourceConfigForTest {
                index_id: index_id.to_string(),
                source_id: CLI_INGEST_SOURCE_ID.to_string(),
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 3,
                enabled: true,
            },
            SourceConfigForTest {
                index_id,
                source_id: CLI_INGEST_SOURCE_ID.to_string(),
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 3,
                enabled: false,
            },
        ]);
        let indexing_tasks = build_indexing_plan(&indexers, &source_configs);

        assert_eq!(indexing_tasks.len(), 0);
    }

    #[test]
    fn test_build_physical_indexing_plan_simple() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let index_2 = "test-indexing-plan-2";
        let source_2 = "source-2";
        let source_configs = create_sources_for_test(vec![
            SourceConfigForTest {
                index_id: index_1.to_string(),
                source_id: source_1.to_string(),
                max_num_pipelines_per_indexer: 2,
                desired_num_pipelines: 4,
                enabled: true,
            },
            SourceConfigForTest {
                index_id: index_2.to_string(),
                source_id: source_2.to_string(),
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 2,
                enabled: true,
            },
        ]);
        let mut indexing_tasks = Vec::new();
        for _ in 0..4 {
            indexing_tasks.push(IndexingTask {
                index_id: index_1.to_string(),
                source_id: source_1.to_string(),
            });
        }
        for _ in 0..2 {
            indexing_tasks.push(IndexingTask {
                index_id: index_2.to_string(),
                source_id: source_2.to_string(),
            });
        }

        let indexers = cluster_members_for_test(2, QuickwitService::Indexer);
        let physical_plan =
            build_physical_indexing_plan(&indexers, &source_configs, indexing_tasks.clone());
        assert_eq!(physical_plan.indexing_tasks_per_indexer.len(), 2);
        let indexer_1_tasks = physical_plan
            .indexing_tasks_per_indexer
            .get(&indexers[0].node_id)
            .unwrap();
        let indexer_2_tasks = physical_plan
            .indexing_tasks_per_indexer
            .get(&indexers[1].node_id)
            .unwrap();
        let expected_indexer_1_tasks = indexing_tasks
            .iter()
            .cloned()
            .enumerate()
            .filter(|(idx, _)| idx % 2 == 0)
            .map(|(_, task)| task)
            .collect_vec();
        assert_eq!(indexer_1_tasks, &expected_indexer_1_tasks);
        let expected_indexer_2_tasks = indexing_tasks
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| idx % 2 != 0)
            .map(|(_, task)| task)
            .collect_vec();
        assert_eq!(indexer_2_tasks, &expected_indexer_2_tasks);
        Ok(())
    }
}
