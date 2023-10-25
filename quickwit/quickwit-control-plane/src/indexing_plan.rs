// Copyright (C) 2023 Quickwit, Inc.
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
use std::num::NonZeroUsize;

use fnv::FnvHashMap;
use itertools::Itertools;
use quickwit_common::rendezvous_hasher::sort_by_rendez_vous_hash;
use quickwit_proto::indexing::IndexingTask;
use quickwit_proto::metastore::SourceType;
use serde::Serialize;

use crate::control_plane_model::ControlPlaneModel;
use crate::{IndexerNodeInfo, SourceUid};

/// A [`PhysicalIndexingPlan`] defines the list of indexing tasks
/// each indexer, identified by its node ID, should run.
/// TODO(fmassot): a metastore version number will be attached to the plan
/// to identify if the plan is up to date with the metastore.
#[derive(Debug, PartialEq, Clone, Serialize, Default)]
pub struct PhysicalIndexingPlan {
    indexing_tasks_per_node_id: FnvHashMap<String, Vec<IndexingTask>>,
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

    pub fn is_empty(&self) -> bool {
        self.indexing_tasks_per_node_id.is_empty()
    }

    /// Returns the number of indexing tasks for the given node ID.
    pub fn num_indexing_tasks_for_node(&self, node_id: &str) -> usize {
        self.indexing_tasks_per_node_id
            .get(node_id)
            .map(|tasks| tasks.len())
            .unwrap_or(0)
    }

    fn assign_indexing_task(
        &mut self,
        node_id: String,
        logical_indexing_task: LogicalIndexingTask,
    ) {
        let physical_indexing_task = IndexingTask {
            index_uid: logical_indexing_task.source_uid.index_uid.to_string(),
            source_id: logical_indexing_task.source_uid.source_id,
            shard_ids: logical_indexing_task.shard_ids,
        };
        self.indexing_tasks_per_node_id
            .entry(node_id)
            .or_default()
            .push(physical_indexing_task);
    }

    /// Returns the number of indexing tasks for the given node ID, index ID and source ID.
    pub fn num_indexing_tasks_for(&self, node_id: &str, index_uid: &str, source_id: &str) -> usize {
        self.indexing_tasks_per_node_id
            .get(node_id)
            .map(|tasks| {
                tasks
                    .iter()
                    .filter(|task| task.index_uid == index_uid && task.source_id == source_id)
                    .count()
            })
            .unwrap_or(0)
    }

    /// Returns the hashmap of (node ID, indexing tasks).
    pub fn indexing_tasks_per_node(&self) -> &FnvHashMap<String, Vec<IndexingTask>> {
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

/// Builds a [`PhysicalIndexingPlan`] by assigning each indexing tasks to a node ID.
/// The algorithm first sort indexing tasks by (index_id, source_id).
/// Then for each indexing tasks, it performs the following steps:
/// 1. Sort node by rendez-vous hashing to make the assignment stable (it makes it deterministic
///    too). This is not bullet proof as the node score has an impact on the assignment too.
/// 2. Select node candidates that can run the task, see [`select_node_candidates`] function.
/// 3. For each node, compute a score for this task, the higher, the better, see
///    `compute_node_score` function.
/// 4. Select the best node (highest score) and assign the task to this node.
/// Additional notes(fmassot): it's nice to have the cluster members as they contain the running
/// tasks. We can potentially use this info to assign an indexing task to a node running the same
/// task.
pub(crate) fn build_physical_indexing_plan(
    indexers: &[(String, IndexerNodeInfo)],
    mut indexing_tasks: Vec<LogicalIndexingTask>,
) -> PhysicalIndexingPlan {
    // Sort by (index_id, source_id) to make the algorithm deterministic.
    indexing_tasks.sort_by(|left, right| left.source_uid.cmp(&right.source_uid));
    let mut node_ids = indexers
        .iter()
        .map(|indexer| indexer.0.clone())
        .collect_vec();

    // Build the plan.
    let mut plan = PhysicalIndexingPlan::new(node_ids.clone());
    for indexing_task in indexing_tasks {
        sort_by_rendez_vous_hash(&mut node_ids, &indexing_task);
        let candidates = select_node_candidates(&node_ids, &plan, &indexing_task);

        // It's theoretically possible to have no candidate as all indexers can already
        // have more than `max_num_pipelines_per_indexer` assigned for a given source.
        // But, when building the list of indexing tasks to run on the cluster in
        // `build_indexing_plan`, we make sure to always respect the constraint
        // `max_num_pipelines_per_indexer` by limiting the number of indexing tasks per
        // source.
        let best_node_score_opt = candidates
            .iter()
            .rev() //< we use the reverse iterator, because in case of a tie, max picks the last element.
            // we want the first one in order to maximize affinity.
            .map(|&node_id| NodeScore {
                node_id,
                score: compute_node_score(node_id, &plan),
            })
            .max();
        if let Some(best_node_score) = best_node_score_opt {
            plan.assign_indexing_task(best_node_score.node_id.to_string(), indexing_task);
        } else {
            tracing::warn!(indexing_task=?indexing_task, "No indexer candidate available for the indexing task, cannot assign it to an indexer. This should not happen.");
        };
    }
    plan
}

struct NodeScore<'a> {
    node_id: &'a str,
    score: f32,
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
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// Returns node candidates IDs that can run the given [`IndexingTask`].
/// The selection is overly simple: a node will match unless it has
/// been already assigned the `max_num_pipelines_per_indexer` of tasks for
/// the given (index ID, source ID).
fn select_node_candidates<'a>(
    node_ids: &'a [String],
    physical_plan: &PhysicalIndexingPlan,
    indexing_task: &LogicalIndexingTask,
) -> Vec<&'a str> {
    let index_uid_str: String = indexing_task.source_uid.index_uid.to_string();
    node_ids
        .iter()
        .map(String::as_str)
        .filter(|node_id| {
            physical_plan.num_indexing_tasks_for(
                node_id,
                &index_uid_str,
                &indexing_task.source_uid.source_id,
            ) < indexing_task.max_num_pipelines_per_indexer.get()
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

impl From<IndexingTask> for SourceUid {
    fn from(indexing_task: IndexingTask) -> Self {
        Self {
            index_uid: indexing_task.index_uid.into(),
            source_id: indexing_task.source_id,
        }
    }
}

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub(crate) struct LogicalIndexingTask {
    pub source_uid: SourceUid,
    pub shard_ids: Vec<u64>,
    pub max_num_pipelines_per_indexer: NonZeroUsize,
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
pub(crate) fn list_indexing_tasks(
    num_indexers: usize,
    model: &ControlPlaneModel,
) -> Vec<LogicalIndexingTask> {
    let mut indexing_tasks: Vec<LogicalIndexingTask> = Vec::new();
    for (source_uid, source_config) in model
        .get_source_configs()
        .sorted_by(|(left, _), (right, _)| left.cmp(right))
    {
        if !source_config.enabled {
            continue;
        }
        match source_config.source_type() {
            SourceType::Cli | SourceType::File | SourceType::Vec | SourceType::Void => {
                continue;
            }
            SourceType::IngestV1 => {
                let logical_indexing_task = LogicalIndexingTask {
                    source_uid: source_uid.clone(),
                    shard_ids: Vec::new(),
                    max_num_pipelines_per_indexer: source_config.max_num_pipelines_per_indexer,
                };
                // TODO fix that ugly logic.
                indexing_tasks.extend(std::iter::repeat(logical_indexing_task).take(num_indexers));
            }
            SourceType::IngestV2 => {
                // TODO for the moment for ingest v2 we create one indexing task with all of the
                // shards. We probably want to have something more unified between
                // IngestV2 and kafka-alike... e.g. one indexing task with a bunch
                // of weighted shards, one indexing task.
                //
                // The construction of the physical plan would then have to split that into more
                // chewable physical tasks (one per pipeline, one per reasonable
                // subset of shards).

                // TODO: More precisely, we want the shards that are open or closing or such that
                // `shard.publish_position_inclusive`
                // < `shard.replication_position_inclusive`.
                // let list_shard_subrequest = ListShardsSubrequest {
                //     index_uid: source_uid.index_uid.clone().into(),
                //     source_id: source_uid.source_id.clone(),
                //     shard_state: None,
                // };
                let shard_ids = model.list_shards(&source_uid);
                let indexing_task = LogicalIndexingTask {
                    source_uid: source_uid.clone(),
                    shard_ids,
                    max_num_pipelines_per_indexer: source_config.max_num_pipelines_per_indexer,
                };
                indexing_tasks.push(indexing_task);
            }
            SourceType::Kafka
            | SourceType::Kinesis
            | SourceType::GcpPubsub
            | SourceType::Nats
            | SourceType::Pulsar => {
                let num_pipelines =
                    // The num desired pipelines is constrained by the number of indexer and the maximum
                    // of pipelines that can run on each indexer.
                    std::cmp::min(
                    source_config.desired_num_pipelines.get(),
                    source_config.max_num_pipelines_per_indexer.get() * num_indexers,
                    );
                let logical_indexing_task = LogicalIndexingTask {
                    source_uid: source_uid.clone(),
                    shard_ids: Vec::new(),
                    max_num_pipelines_per_indexer: source_config.max_num_pipelines_per_indexer,
                };
                indexing_tasks.extend(std::iter::repeat(logical_indexing_task).take(num_pipelines));
            }
        }
    }
    indexing_tasks
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use fnv::FnvHashMap;
    use itertools::Itertools;
    use proptest::prelude::*;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::service::QuickwitService;
    use quickwit_config::{
        FileSourceParams, IndexConfig, KafkaSourceParams, SourceConfig, SourceInputFormat,
        SourceParams, CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID,
    };
    use quickwit_metastore::IndexMetadata;
    use quickwit_proto::indexing::{IndexingServiceClient, IndexingTask};
    use quickwit_proto::types::IndexUid;
    use rand::seq::SliceRandom;
    use serde_json::json;
    use tonic::transport::Endpoint;

    use super::{build_physical_indexing_plan, SourceUid};
    use crate::control_plane_model::ControlPlaneModel;
    use crate::indexing_plan::{list_indexing_tasks, LogicalIndexingTask};
    use crate::IndexerNodeInfo;

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

    async fn cluster_members_for_test(
        num_members: usize,
        _quickwit_service: QuickwitService,
    ) -> Vec<(String, IndexerNodeInfo)> {
        let mut members = Vec::new();
        for idx in 0..num_members {
            let channel = Endpoint::from_static("http://127.0.0.1:10").connect_lazy();
            let client =
                IndexingServiceClient::from_channel("127.0.0.1:10".parse().unwrap(), channel);
            members.push((
                (1 + idx).to_string(),
                IndexerNodeInfo {
                    client,
                    indexing_tasks: Vec::new(),
                },
            ));
        }
        members
    }

    fn count_indexing_tasks_count_for_test(
        num_indexers: usize,
        source_configs: &FnvHashMap<SourceUid, SourceConfig>,
    ) -> usize {
        source_configs
            .iter()
            .map(|(_, source_config)| {
                std::cmp::min(
                    num_indexers * source_config.max_num_pipelines_per_indexer.get(),
                    source_config.desired_num_pipelines.get(),
                )
            })
            .sum()
    }

    #[tokio::test]
    async fn test_build_indexing_plan_one_source() {
        let source_config = SourceConfig {
            source_id: "source_id".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
            enabled: true,
            source_params: kafka_source_params_for_test(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let mut model = ControlPlaneModel::default();
        let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
        let index_uid = index_metadata.index_uid.clone();
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: source_config.source_id.clone(),
        };
        model.add_index(index_metadata);
        model.add_source(&index_uid, source_config).unwrap();
        let indexing_tasks = list_indexing_tasks(4, &model);
        assert_eq!(indexing_tasks.len(), 3);
        for indexing_task in indexing_tasks {
            assert_eq!(
                indexing_task,
                LogicalIndexingTask {
                    source_uid: source_uid.clone(),
                    shard_ids: Vec::new(),
                    max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                }
            );
        }
    }

    #[tokio::test]
    async fn test_build_indexing_plan_with_ingest_api_source() {
        let indexers = cluster_members_for_test(4, QuickwitService::Indexer).await;

        let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
        let index_uid = index_metadata.index_uid.clone();
        let source_uid = SourceUid {
            index_uid: index_uid.clone(),
            source_id: INGEST_API_SOURCE_ID.to_string(),
        };

        let source_config = SourceConfig {
            source_id: source_uid.source_id.to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
            enabled: true,
            source_params: SourceParams::IngestApi,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };

        let mut control_plane_model = ControlPlaneModel::default();
        control_plane_model.add_index(index_metadata);
        control_plane_model
            .add_source(&index_uid, source_config)
            .unwrap();
        let indexing_tasks: Vec<LogicalIndexingTask> =
            list_indexing_tasks(indexers.len(), &control_plane_model);

        assert_eq!(indexing_tasks.len(), 4);
        for indexing_task in indexing_tasks {
            assert_eq!(
                indexing_task,
                LogicalIndexingTask {
                    source_uid: source_uid.clone(),
                    shard_ids: Vec::new(),
                    max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                }
            );
        }
    }

    #[tokio::test]
    async fn test_build_indexing_plan_with_sources_to_ignore() {
        let indexers = cluster_members_for_test(4, QuickwitService::Indexer).await;
        let mut source_configs_map = FnvHashMap::default();
        let file_index_source_id = SourceUid {
            index_uid: "one-source-index:11111111111111111111111111"
                .to_string()
                .into(),
            source_id: "file-source".to_string(),
        };
        let cli_ingest_index_source_id = SourceUid {
            index_uid: "second-source-index:11111111111111111111111111"
                .to_string()
                .into(),
            source_id: CLI_INGEST_SOURCE_ID.to_string(),
        };
        let kafka_index_source_id = SourceUid {
            index_uid: "third-source-index:11111111111111111111111111"
                .to_string()
                .into(),
            source_id: "kafka-source".to_string(),
        };
        source_configs_map.insert(
            file_index_source_id.clone(),
            SourceConfig {
                source_id: file_index_source_id.source_id,
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
                enabled: true,
                source_params: SourceParams::File(FileSourceParams { filepath: None }),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            },
        );
        source_configs_map.insert(
            cli_ingest_index_source_id.clone(),
            SourceConfig {
                source_id: cli_ingest_index_source_id.source_id,
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
                enabled: true,
                source_params: SourceParams::IngestCli,
                transform_config: None,
                input_format: SourceInputFormat::Json,
            },
        );
        source_configs_map.insert(
            kafka_index_source_id.clone(),
            SourceConfig {
                source_id: kafka_index_source_id.source_id,
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
                desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
                enabled: false,
                source_params: kafka_source_params_for_test(),
                transform_config: None,
                input_format: SourceInputFormat::Json,
            },
        );

        let control_plane_model = ControlPlaneModel::default();
        let indexing_tasks = list_indexing_tasks(indexers.len(), &control_plane_model);

        assert_eq!(indexing_tasks.len(), 0);
    }

    #[tokio::test]
    async fn test_build_physical_indexing_plan_simple() {
        quickwit_common::setup_logging_for_tests();
        // Rdv hashing for (index 1, source) returns [node 2, node 1].
        let index_1 = "1";
        let source_1 = "1";
        // Rdv hashing for (index 2, source) returns [node 1, node 2].
        let index_2 = "2";
        let source_2 = "0";
        // let mut source_configs_map = FnvHashMap::default();
        let kafka_index_source_id_1 = SourceUid {
            index_uid: IndexUid::from_parts(index_1, "11111111111111111111111111"),
            source_id: source_1.to_string(),
        };
        let kafka_index_source_id_2 = SourceUid {
            index_uid: IndexUid::from_parts(index_2, "11111111111111111111111111"),
            source_id: source_2.to_string(),
        };
        let mut indexing_tasks = Vec::new();
        for _ in 0..3 {
            indexing_tasks.push(LogicalIndexingTask {
                source_uid: kafka_index_source_id_1.clone(),
                shard_ids: Vec::new(),
                max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
            });
        }
        for _ in 0..2 {
            indexing_tasks.push(LogicalIndexingTask {
                source_uid: kafka_index_source_id_2.clone(),
                shard_ids: Vec::new(),
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            });
        }

        let indexers = cluster_members_for_test(2, QuickwitService::Indexer).await;
        let physical_plan = build_physical_indexing_plan(&indexers, indexing_tasks.clone());
        assert_eq!(physical_plan.indexing_tasks_per_node_id.len(), 2);
        let indexer_1_tasks = physical_plan
            .indexing_tasks_per_node_id
            .get(&indexers[0].0)
            .unwrap();
        let indexer_2_tasks = physical_plan
            .indexing_tasks_per_node_id
            .get(&indexers[1].0)
            .unwrap();
        // (index 1, source) tasks are first placed on indexer 2 by rdv hashing.
        // Thus task 0 => indexer 2, task 1 => indexer 1, task 2 => indexer 2, task 3 => indexer 1.
        let expected_indexer_1_tasks: Vec<IndexingTask> = indexing_tasks
            .iter()
            .cloned()
            .enumerate()
            .filter(|(idx, _)| idx % 2 == 1)
            .map(|(_, task)| task.into())
            .collect_vec();
        assert_eq!(indexer_1_tasks, &expected_indexer_1_tasks);
        // (index 1, source) tasks are first placed on node 1 by rdv hashing.
        let expected_indexer_2_tasks: Vec<IndexingTask> = indexing_tasks
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| idx % 2 == 0)
            .map(|(_, task)| task.into())
            .collect_vec();
        assert_eq!(indexer_2_tasks, &expected_indexer_2_tasks);
    }

    impl From<LogicalIndexingTask> for IndexingTask {
        fn from(task: LogicalIndexingTask) -> Self {
            IndexingTask {
                index_uid: task.source_uid.index_uid.to_string(),
                source_id: task.source_uid.source_id.clone(),
                shard_ids: task.shard_ids,
            }
        }
    }

    #[tokio::test]
    async fn test_build_physical_indexing_plan_with_not_enough_indexers() {
        quickwit_common::setup_logging_for_tests();
        let index_1 = "test-indexing-plan-1";
        let source_1 = "source-1";
        let source_uid = SourceUid {
            index_uid: IndexUid::from_parts(index_1, "11111111111111111111111111"),
            source_id: source_1.to_string(),
        };
        let indexing_tasks = vec![
            LogicalIndexingTask {
                source_uid: source_uid.clone(),
                shard_ids: Vec::new(),
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            },
            LogicalIndexingTask {
                source_uid: source_uid.clone(),
                shard_ids: Vec::new(),
                max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            },
        ];

        let indexers = cluster_members_for_test(1, QuickwitService::Indexer).await;
        // This case should never happens but we just check that the plan building is resilient
        // enough, it will ignore the tasks that cannot be allocated.
        let physical_plan = build_physical_indexing_plan(&indexers, indexing_tasks);
        assert_eq!(physical_plan.num_indexing_tasks(), 1);
    }

    proptest! {
        #[test]
        fn test_building_indexing_tasks_and_physical_plan(num_indexers in 1usize..50usize, index_id_sources in proptest::collection::vec(gen_kafka_source(), 1..20)) {
            // proptest doesn't work with async
            let mut indexers = tokio::runtime::Runtime::new().unwrap().block_on(
                cluster_members_for_test(num_indexers, QuickwitService::Indexer)
            );

            let index_uids: fnv::FnvHashSet<IndexUid> =
                index_id_sources.iter()
                    .map(|(index_uid, _)| index_uid.clone())
                    .collect();

            let mut control_plane_model = ControlPlaneModel::default();
            for index_uid in index_uids {
                let index_config = IndexConfig::for_test(index_uid.index_id(), &format!("ram://test/{index_uid}"));
                control_plane_model.add_index(IndexMetadata::new_with_index_uid(index_uid, index_config));
            }
            for (index_uid, source_config) in &index_id_sources {
                control_plane_model.add_source(index_uid, source_config.clone()).unwrap();
            }

            let source_configs: FnvHashMap<SourceUid, SourceConfig> = index_id_sources
                .into_iter()
                .map(|(index_uid, source_config)| {
                    (SourceUid { index_uid: index_uid.clone(), source_id: source_config.source_id.to_string(), }, source_config)
                })
                .collect();


            let mut indexing_tasks = list_indexing_tasks(indexers.len(), &control_plane_model);
            let num_indexing_tasks = indexing_tasks.len();
            assert_eq!(indexing_tasks.len(), count_indexing_tasks_count_for_test(indexers.len(), &source_configs));
            let physical_indexing_plan = build_physical_indexing_plan(&indexers, indexing_tasks.clone());
            indexing_tasks.shuffle(&mut rand::thread_rng());
            indexers.shuffle(&mut rand::thread_rng());
            let physical_indexing_plan_with_shuffle = build_physical_indexing_plan(&indexers, indexing_tasks.clone());
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
        (index_idx in 0usize..100usize, desired_num_pipelines in 1usize..51usize, max_num_pipelines_per_indexer in 1usize..5usize) -> (IndexUid, SourceConfig) {
          let index_uid = IndexUid::from_parts(format!("index-id-{index_idx}"), "" /* this is the index uid */);
          let source_id = append_random_suffix("kafka-source");
          (index_uid, SourceConfig {
              source_id,
              desired_num_pipelines: NonZeroUsize::new(desired_num_pipelines).unwrap(),
              max_num_pipelines_per_indexer: NonZeroUsize::new(max_num_pipelines_per_indexer).unwrap(),
              enabled: true,
              source_params: kafka_source_params_for_test(),
              transform_config: None,
              input_format: SourceInputFormat::Json,
          })
      }
    }
}
