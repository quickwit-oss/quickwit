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
use quickwit_cluster::ClusterMember;
use quickwit_config::{SourceConfig, CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID};
use quickwit_proto::indexing_api::IndexingTask;
use serde::Serialize;

/// A [`PhysicalIndexingPlan`] defines the list of indexing tasks
/// each indexer, identified by its node ID, should run.
/// TODO(fmassot): a metastore version number will be attached to the plan
/// to identify if the plan is up to date with the metastore.
#[derive(Debug, PartialEq, Clone, Serialize)]
pub struct PhysicalIndexingPlan {
    indexing_tasks_per_node_id: HashMap<String, Vec<IndexingTask>>,
}

impl PhysicalIndexingPlan {
    fn new(node_ids: Vec<String>) -> Self {
        let indexing_tasks_per_node_id = node_ids
            .into_iter()
            .map(|node_id| (node_id, Vec::new()))
            .collect();
        Self {
            indexing_tasks_per_node_id,
        }
    }

    /// Returns the number of indexing tasks for the given node ID.
    pub fn num_indexing_tasks_for_node(&self, node_id: &str) -> usize {
        self.indexing_tasks_per_node_id
            .get(node_id)
            .map(|tasks| tasks.len())
            .unwrap_or(0)
    }

    pub fn assign_indexing_task(&mut self, node_id: String, indexing_task: IndexingTask) {
        self.indexing_tasks_per_node_id
            .entry(node_id)
            .or_default()
            .push(indexing_task);
    }

    /// Returns the number of indexing tasks for the given node ID, index ID and source ID.
    pub fn num_indexing_tasks_for(&self, node_id: &str, index_id: &str, source_id: &str) -> usize {
        self.indexing_tasks_per_node_id
            .get(node_id)
            .map(|tasks| {
                tasks
                    .iter()
                    .filter(|task| task.index_id == index_id && task.source_id == source_id)
                    .count()
            })
            .unwrap_or(0)
    }

    /// Returns the hashmap of (node ID, indexing tasks).
    pub fn indexing_tasks_per_node(&self) -> &HashMap<String, Vec<IndexingTask>> {
        &self.indexing_tasks_per_node_id
    }

    /// Returns the mean number of indexing tasks per node. It returns 0 if there is no node.
    pub fn num_indexing_tasks_mean_per_node(&self) -> f32 {
        let num_nodes = self.indexing_tasks_per_node_id.len();
        if num_nodes == 0 {
            return 0f32;
        }
        self.num_indexing_tasks() as f32 / num_nodes as f32
    }

    /// Returns the mean number of indexing tasks per node.
    fn num_indexing_tasks(&self) -> usize {
        self.indexing_tasks_per_node_id
            .values()
            .map(|tasks| tasks.len())
            .sum()
    }

    // For test only. Returns the min number of tasks on one node.
    #[cfg(test)]
    fn min_num_indexing_tasks_per_node(&self) -> usize {
        self.indexing_tasks_per_node_id
            .values()
            .map(|tasks| tasks.len())
            .min()
            .unwrap_or(0)
    }

    // For test only. Returns the max number of tasks on one node.
    #[cfg(test)]
    fn max_num_indexing_tasks_per_node(&self) -> usize {
        self.indexing_tasks_per_node_id
            .values()
            .map(|tasks| tasks.len())
            .max()
            .unwrap_or(0)
    }
}

#[derive(Debug)]
struct NodeScore<'a> {
    pub node_id: &'a str,
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
        // Score is never NaN.
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Builds a [`PhysicalIndexingPlan`] by assigning each indexing tasks to a node ID.
/// The algorithm first sort indexing tasks by (index_id, source_id) and sort indexers
/// by node ID to make the algorithm deterministic.
/// Then for each indexing tasks, it performs the following steps:
/// 1. Select node candidates that can run the task, see [`select_node_candidates`]
///    function.
/// 2. For each node, compute a score for this task, the higher, the better, see
///    `compute_node_score` function.
/// 3. Select the best node (highest score) and assign the task to
///    this node.
/// Additional notes(fmassot): it's nice to have the cluster members as they contain the running
/// tasks. We can potentially use this info to assign an indexing task to a node running the same
/// task.
pub(crate) fn build_physical_indexing_plan(
    indexers: &[ClusterMember],
    source_configs: &HashMap<IndexSourceId, SourceConfig>,
    mut indexing_tasks: Vec<IndexingTask>,
) -> PhysicalIndexingPlan {
    // Sort by (index_id, source_id)...
    indexing_tasks.sort_by(|left, right| {
        (&left.index_id, &left.source_id).cmp(&(&right.index_id, &right.source_id))
    });
    // ...and sort by node ID to make the algorithm deterministic.
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
            // TODO(fmassot): remove this lame allocation to access the source...
            .get(&IndexSourceId::from(indexing_task.clone()))
            .expect("SourceConfig should always be present.");
        let candidates = select_node_candidates(&node_ids, &plan, source_config, &indexing_task);

        // It's theoritically possible to have no candidate as all indexers can already
        // have more than `max_num_pipelines_per_indexer` assigned for a given source.
        // But, when building the list of indexing tasks to run on the cluster in
        // `build_indexing_plan`, we make sure to always respect the constraint
        // `max_num_pipelines_per_indexer` by limiting the number of indexing tasks per
        // source.
        let best_node_opt: Option<&str> = candidates
            .iter()
            // NodeScore implements Ord and can be used easily for sorting.
            .map(|node_id| NodeScore {
                node_id,
                score: -compute_node_score(node_id, &plan),
            })
            .sorted()
            .next()
            .map(|node_score| node_score.node_id);
        if let Some(best_node) = best_node_opt {
            plan.assign_indexing_task(best_node.to_string(), indexing_task);
        } else {
            tracing::warn!(indexing_task=?indexing_task, "No indexer candidate available for the indexing task, cannot assign it to an indexer. This should not happen.");
        };
    }

    plan
}

/// Returns node candidates IDs that can run the given [`IndexingTask`].
/// The selection is overly simple: a node will match unless it has
/// been already assigned the `max_num_pipelines_per_indexer` of tasks for
/// the given (index ID, source ID).
fn select_node_candidates<'a>(
    node_ids: &'a [String],
    physical_plan: &PhysicalIndexingPlan,
    source_config: &SourceConfig,
    indexing_task: &IndexingTask,
) -> Vec<&'a str> {
    node_ids
        .iter()
        .map(String::as_str)
        .filter(|node_id| {
            physical_plan.num_indexing_tasks_for(
                node_id,
                &indexing_task.index_id,
                &indexing_task.source_id,
            ) < source_config.max_num_pipelines_per_indexer()
        })
        .collect_vec()
}

/// Lame scoring function for a given node ID defined by
/// `score = indexing_tasks_per_node_mean - num_indexing_tasks_for_given_node`.
/// The lower the number of tasks, the higher the score will be.
// TODO: introduce other criteria.
fn compute_node_score(node_id: &str, physical_plan: &PhysicalIndexingPlan) -> f32 {
    physical_plan.num_indexing_tasks_mean_per_node()
        - physical_plan.num_indexing_tasks_for_node(node_id) as f32
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
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

/// Builds the indexing plan = `[Vec<IndexingTask]` from the given sources,
/// that should run on the cluster.
/// The plan building algorithm follows these rules:
/// - For each source, `num_tasks` are added to the plan with `num_tasks =
///   min(desired_num_pipelines`, `max_num_pipelines_per_indexer * num_indexers)`. The `min` ensures
///   that a cluster is always able to run all the tasks.
/// - For the ingest API source, it creates one indexing task per indexer. Indeed, an indexer is not
///   able to forward documents received by the ingest API to the indexer that is running the
///   corresponding indexing pipeline. To make ingestion easier for the user, we starts ingest
///   pipelines on all indexers. TODO(fmassot): remove this rule once Quickwit has the ability to
///   forward documents to the right indexers.
/// - Ignore disabled sources, `CLI_INGEST_SOURCE_ID` and files sources (Quickwit is not aware of
///   the files locations and thus are ignored).
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
        // Ignore file sources as we don't know the file location.
        if source_config.source_type() == "file" {
            continue;
        }
        let num_pipelines = if source_config.source_id == INGEST_API_SOURCE_ID {
            indexers.len()
        } else {
            // The num desired pipelines is constrained by the number of indexer and the maximum
            // of pipelines that can run on each indexer.
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
    use proptest::prelude::*;
    use quickwit_cluster::ClusterMember;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::service::QuickwitService;
    use quickwit_config::{
        FileSourceParams, KafkaSourceParams, SourceConfig, SourceParams, CLI_INGEST_SOURCE_ID,
        INGEST_API_SOURCE_ID,
    };
    use quickwit_proto::indexing_api::IndexingTask;
    use rand::seq::SliceRandom;
    use serde_json::json;

    use super::{build_physical_indexing_plan, IndexSourceId};
    use crate::indexing_plan::build_indexing_plan;

    fn kafka_source_params_for_test() -> SourceParams {
        SourceParams::Kafka(KafkaSourceParams {
            topic: "topic".to_string(),
            client_log_level: None,
            client_params: json!({
                "bootstrap.servers": "localhost:9092",
            }),
            enable_backfill_mode: true,
        })
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
                None,
            ))
        }
        members
    }

    fn count_indexing_tasks_count_for_test(
        num_indexers: usize,
        source_configs: &HashMap<IndexSourceId, SourceConfig>,
    ) -> usize {
        source_configs
            .iter()
            .map(|(_, source_config)| {
                std::cmp::min(
                    num_indexers * source_config.max_num_pipelines_per_indexer(),
                    source_config.desired_num_pipelines(),
                )
            })
            .sum()
    }

    #[test]
    fn test_build_indexing_plan_one_source() {
        let indexers = cluster_members_for_test(4, QuickwitService::Indexer);
        let mut source_configs_map = HashMap::new();
        let index_source_id = IndexSourceId {
            index_id: "one-source-index".to_string(),
            source_id: "source-0".to_string(),
        };
        source_configs_map.insert(
            index_source_id.clone(),
            SourceConfig {
                source_id: index_source_id.source_id.to_string(),
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 3,
                enabled: true,
                source_params: kafka_source_params_for_test(),
                transform_config: None,
            },
        );

        let indexing_tasks = build_indexing_plan(&indexers, &source_configs_map);

        assert_eq!(indexing_tasks.len(), 3);
        for indexing_task in indexing_tasks {
            assert_eq!(
                indexing_task,
                IndexingTask {
                    index_id: index_source_id.index_id.to_string(),
                    source_id: index_source_id.source_id.to_string(),
                }
            );
        }
    }

    #[test]
    fn test_build_indexing_plan_with_ingest_api_source() {
        let indexers = cluster_members_for_test(4, QuickwitService::Indexer);
        let mut source_configs_map = HashMap::new();
        let index_source_id = IndexSourceId {
            index_id: "ingest-api-index".to_string(),
            source_id: INGEST_API_SOURCE_ID.to_string(),
        };
        source_configs_map.insert(
            index_source_id.clone(),
            SourceConfig {
                source_id: index_source_id.source_id.to_string(),
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 3,
                enabled: true,
                source_params: SourceParams::IngestApi,
                transform_config: None,
            },
        );

        let indexing_tasks = build_indexing_plan(&indexers, &source_configs_map);

        assert_eq!(indexing_tasks.len(), 4);
        for indexing_task in indexing_tasks {
            assert_eq!(
                indexing_task,
                IndexingTask {
                    index_id: index_source_id.index_id.to_string(),
                    source_id: index_source_id.source_id.to_string(),
                }
            );
        }
    }

    #[test]
    fn test_build_indexing_plan_with_sources_to_ignore() {
        let indexers = cluster_members_for_test(4, QuickwitService::Indexer);
        let mut source_configs_map = HashMap::new();
        let file_index_source_id = IndexSourceId {
            index_id: "one-source-index".to_string(),
            source_id: "file-source".to_string(),
        };
        let cli_ingest_index_source_id = IndexSourceId {
            index_id: "second-source-index".to_string(),
            source_id: CLI_INGEST_SOURCE_ID.to_string(),
        };
        let kafka_index_source_id = IndexSourceId {
            index_id: "third-source-index".to_string(),
            source_id: "kafka-source".to_string(),
        };
        source_configs_map.insert(
            file_index_source_id.clone(),
            SourceConfig {
                source_id: file_index_source_id.source_id,
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 3,
                enabled: true,
                source_params: SourceParams::File(FileSourceParams { filepath: None }),
                transform_config: None,
            },
        );
        source_configs_map.insert(
            cli_ingest_index_source_id.clone(),
            SourceConfig {
                source_id: cli_ingest_index_source_id.source_id,
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 3,
                enabled: true,
                source_params: SourceParams::IngestCli,
                transform_config: None,
            },
        );
        source_configs_map.insert(
            kafka_index_source_id.clone(),
            SourceConfig {
                source_id: kafka_index_source_id.source_id,
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 3,
                enabled: false,
                source_params: kafka_source_params_for_test(),
                transform_config: None,
            },
        );
        let indexing_tasks = build_indexing_plan(&indexers, &source_configs_map);

        assert_eq!(indexing_tasks.len(), 0);
    }

    #[test]
    fn test_build_physical_indexing_plan_simple() {
        quickwit_common::setup_logging_for_tests();
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let index_2 = "test-indexing-plan-2";
        let source_2 = "source-2";
        let mut source_configs_map = HashMap::new();
        let kafka_index_source_id_1 = IndexSourceId {
            index_id: index_1.to_string(),
            source_id: source_1.to_string(),
        };
        let kafka_index_source_id_2 = IndexSourceId {
            index_id: index_2.to_string(),
            source_id: source_2.to_string(),
        };
        source_configs_map.insert(
            kafka_index_source_id_1.clone(),
            SourceConfig {
                source_id: kafka_index_source_id_1.source_id,
                max_num_pipelines_per_indexer: 2,
                desired_num_pipelines: 4,
                enabled: true,
                source_params: kafka_source_params_for_test(),
                transform_config: None,
            },
        );
        source_configs_map.insert(
            kafka_index_source_id_2.clone(),
            SourceConfig {
                source_id: kafka_index_source_id_2.source_id,
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 2,
                enabled: true,
                source_params: kafka_source_params_for_test(),
                transform_config: None,
            },
        );
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
            build_physical_indexing_plan(&indexers, &source_configs_map, indexing_tasks.clone());
        assert_eq!(physical_plan.indexing_tasks_per_node_id.len(), 2);
        let indexer_1_tasks = physical_plan
            .indexing_tasks_per_node_id
            .get(&indexers[0].node_id)
            .unwrap();
        let indexer_2_tasks = physical_plan
            .indexing_tasks_per_node_id
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
    }

    #[test]
    fn test_build_physical_indexing_plan_with_not_enough_indexers() {
        quickwit_common::setup_logging_for_tests();
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let mut source_configs_map = HashMap::new();
        let kafka_index_source_id_1 = IndexSourceId {
            index_id: index_1.to_string(),
            source_id: source_1.to_string(),
        };
        source_configs_map.insert(
            kafka_index_source_id_1.clone(),
            SourceConfig {
                source_id: kafka_index_source_id_1.source_id,
                max_num_pipelines_per_indexer: 1,
                desired_num_pipelines: 2,
                enabled: true,
                source_params: kafka_source_params_for_test(),
                transform_config: None,
            },
        );
        let indexing_tasks = vec![
            IndexingTask {
                index_id: index_1.to_string(),
                source_id: source_1.to_string(),
            },
            IndexingTask {
                index_id: index_1.to_string(),
                source_id: source_1.to_string(),
            },
        ];

        let indexers = cluster_members_for_test(1, QuickwitService::Indexer);
        // This case should never happens but we just check that the plan building is resilient
        // enough, it will ignore the tasks that cannot be allocated.
        let physical_plan =
            build_physical_indexing_plan(&indexers, &source_configs_map, indexing_tasks);
        assert_eq!(physical_plan.num_indexing_tasks(), 1);
    }

    proptest! {
        #[test]
        fn test_building_indexing_tasks_and_physical_plan(num_indexers in 1usize..50usize, index_id_sources in proptest::collection::vec(gen_kafka_source(), 1..20)) {
            let mut indexers = cluster_members_for_test(num_indexers, QuickwitService::Indexer);
            let source_configs: HashMap<IndexSourceId, SourceConfig> = index_id_sources
                .into_iter()
                .map(|(index_id, source_config)| {
                    (IndexSourceId { index_id, source_id: source_config.source_id.to_string() }, source_config)
                })
                .collect();
            let mut indexing_tasks = build_indexing_plan(&indexers, &source_configs);
            let num_indexing_tasks = indexing_tasks.len();
            assert_eq!(indexing_tasks.len(), count_indexing_tasks_count_for_test(indexers.len(), &source_configs));
            let physical_indexing_plan = build_physical_indexing_plan(&indexers, &source_configs, indexing_tasks.clone());
            indexing_tasks.shuffle(&mut rand::thread_rng());
            indexers.shuffle(&mut rand::thread_rng());
            let physical_indexing_plan_with_shuffle = build_physical_indexing_plan(&indexers, &source_configs, indexing_tasks.clone());
            assert_eq!(physical_indexing_plan, physical_indexing_plan_with_shuffle);
            // All indexing tasks must have been assigned to an indexer.
            assert_eq!(physical_indexing_plan.num_indexing_tasks(), num_indexing_tasks);
            // Indexing task must be spread over nodes: at maximum, a node can have only one more indexing task than other nodes.
            assert!(physical_indexing_plan.max_num_indexing_tasks_per_node() - physical_indexing_plan.min_num_indexing_tasks_per_node() <= 1);
            // Check basics math.
            assert_eq!(physical_indexing_plan.num_indexing_tasks_mean_per_node(), num_indexing_tasks as f32 / indexers.len() as f32 );
            assert_eq!(physical_indexing_plan.indexing_tasks_per_node().len(), indexers.len());
        }
    }

    prop_compose! {
      fn gen_kafka_source()
        (index_idx in 0usize..100usize, desired_num_pipelines in 0usize..50usize, max_num_pipelines_per_indexer in 0usize..4usize) -> (String, SourceConfig) {
          let index_id = format!("index-id-{index_idx}");
          let source_id = append_random_suffix("kafka-source");
          (index_id, SourceConfig {
              source_id,
              desired_num_pipelines,
              max_num_pipelines_per_indexer,
              enabled: true,
              source_params: kafka_source_params_for_test(),
              transform_config: None,
          })
      }
    }
}
