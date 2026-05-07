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

//! Generic task estimator based on `DataSourceExec` output partitioning.
//!
//! Works for any source that produces a `DataSourceExec` with accurate output
//! partitioning (parquet, tantivy, custom). For parquet sources the partition
//! count equals the file-group count; for tantivy sources it equals the total
//! segment count across splits.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    PartitionIsolatorExec, TaskEstimation, TaskEstimator, TaskRoutingContext,
};
use siphasher::sip::SipHasher;
use url::Url;

/// Estimates distributed task count from the partition count of a
/// `DataSourceExec` leaf. Returns `None` for any other plan node, letting the
/// distributed optimizer fall back to its default heuristics.
#[derive(Debug)]
pub struct DataSourceExecPartitionEstimator;

impl TaskEstimator for DataSourceExecPartitionEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let partitions = dse.properties().output_partitioning().partition_count();
        if partitions <= 1 {
            return Some(TaskEstimation::maximum(1));
        }
        Some(TaskEstimation::desired(partitions))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let _dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        if task_count <= 1 {
            return Some(Arc::clone(plan));
        }
        Some(Arc::new(PartitionIsolatorExec::new(
            Arc::clone(plan),
            task_count,
        )))
    }

    fn route_tasks(&self, routing_ctx: &TaskRoutingContext<'_>) -> DFResult<Option<Vec<Url>>> {
        if routing_ctx.available_urls.is_empty() {
            return Ok(None);
        }
        let Some(file_scan) = single_file_scan_config(routing_ctx.plan) else {
            return Ok(None);
        };
        let task_affinities = file_scan_task_affinities(file_scan, routing_ctx.task_count);
        if task_affinities.len() != routing_ctx.task_count {
            return Ok(None);
        }
        Ok(Some(route_by_rendezvous(
            &task_affinities,
            routing_ctx.available_urls,
        )))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct TaskAffinity {
    task_index: usize,
    affinity_key: String,
}

fn single_file_scan_config(plan: &Arc<dyn ExecutionPlan>) -> Option<&FileScanConfig> {
    let mut file_scans = Vec::new();
    collect_file_scan_configs(plan, &mut file_scans);
    match file_scans.as_slice() {
        [file_scan] => Some(*file_scan),
        _ => None,
    }
}

fn collect_file_scan_configs<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    file_scans: &mut Vec<&'a FileScanConfig>,
) {
    if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>()
        && let Some(file_scan) = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
    {
        file_scans.push(file_scan);
    }

    for child in plan.children() {
        collect_file_scan_configs(child, file_scans);
    }
}

fn file_scan_task_affinities(file_scan: &FileScanConfig, task_count: usize) -> Vec<TaskAffinity> {
    partition_groups(file_scan.file_groups.len(), task_count)
        .into_iter()
        .enumerate()
        .map(|(task_index, partition_group)| {
            let mut file_keys = Vec::new();
            for partition_idx in partition_group {
                if let Some(file_group) = file_scan.file_groups.get(partition_idx) {
                    for file in file_group.files() {
                        file_keys.push(format!(
                            "{}{}",
                            file_scan.object_store_url.as_str(),
                            file.object_meta.location.as_ref()
                        ));
                    }
                }
            }
            file_keys.sort_unstable();
            let affinity_key = if file_keys.is_empty() {
                format!("empty-task-{task_index}")
            } else {
                file_keys.join("\n")
            };
            TaskAffinity {
                task_index,
                affinity_key,
            }
        })
        .collect()
}

fn partition_groups(input_partitions: usize, task_count: usize) -> Vec<Vec<usize>> {
    if task_count == 0 {
        return Vec::new();
    }
    let q = input_partitions / task_count;
    let r = input_partitions % task_count;
    let mut off = 0;
    (0..task_count)
        .map(|i| q + usize::from(i < r))
        .map(|n| {
            let result = (off..(off + n)).collect();
            off += n;
            result
        })
        .collect()
}

fn route_by_rendezvous(task_affinities: &[TaskAffinity], available_urls: &[Url]) -> Vec<Url> {
    let mut routes: Vec<Option<Url>> = vec![None; task_affinities.len()];
    let mut task_order: Vec<usize> = (0..task_affinities.len()).collect();
    task_order.sort_unstable_by(|&left, &right| {
        task_affinities[left]
            .affinity_key
            .cmp(&task_affinities[right].affinity_key)
            .then_with(|| {
                task_affinities[left]
                    .task_index
                    .cmp(&task_affinities[right].task_index)
            })
    });

    for task_idx in task_order {
        let task = &task_affinities[task_idx];
        let mut candidates: Vec<usize> = (0..available_urls.len()).collect();
        candidates.sort_unstable_by(|&left, &right| {
            let left_score = rendezvous_affinity(&available_urls[left], &task.affinity_key);
            let right_score = rendezvous_affinity(&available_urls[right], &task.affinity_key);
            right_score.cmp(&left_score).then_with(|| {
                available_urls[left]
                    .as_str()
                    .cmp(available_urls[right].as_str())
            })
        });
        let chosen_idx = candidates[0];
        routes[task.task_index] = Some(available_urls[chosen_idx].clone());
    }

    routes
        .into_iter()
        .map(|route| route.expect("every task should be routed"))
        .collect()
}

fn rendezvous_affinity(url: &Url, key: &str) -> u64 {
    let mut state = SipHasher::new();
    key.hash(&mut state);
    url.as_str().hash(&mut state);
    state.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(value: &str) -> Url {
        Url::parse(value).unwrap()
    }

    fn task(task_index: usize, affinity_key: &str) -> TaskAffinity {
        TaskAffinity {
            task_index,
            affinity_key: affinity_key.to_string(),
        }
    }

    #[test]
    fn partition_groups_match_partition_isolator_layout() {
        assert_eq!(partition_groups(2, 1), vec![vec![0, 1]]);
        assert_eq!(partition_groups(6, 2), vec![vec![0, 1, 2], vec![3, 4, 5]]);
        assert_eq!(
            partition_groups(10, 3),
            vec![vec![0, 1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]
        );
    }

    #[test]
    fn rendezvous_routing_is_stable_across_url_order() {
        let tasks = vec![
            task(0, "s3://bucket/metrics-0.parquet"),
            task(1, "s3://bucket/metrics-1.parquet"),
            task(2, "s3://bucket/metrics-2.parquet"),
            task(3, "s3://bucket/metrics-3.parquet"),
            task(4, "s3://bucket/metrics-4.parquet"),
            task(5, "s3://bucket/metrics-5.parquet"),
        ];
        let urls = vec![
            url("http://node-0:7281"),
            url("http://node-1:7281"),
            url("http://node-2:7281"),
        ];
        let mut reversed_urls = urls.clone();
        reversed_urls.reverse();

        let routes = route_by_rendezvous(&tasks, &urls);
        let reversed_routes = route_by_rendezvous(&tasks, &reversed_urls);

        assert_eq!(routes, reversed_routes);
    }

    #[test]
    fn rendezvous_routing_uses_best_affinity_node_per_task() {
        let tasks = vec![task(0, "s3://bucket/metrics.parquet")];
        let urls = vec![url("http://node-0:7281"), url("http://node-1:7281")];

        let routes = route_by_rendezvous(&tasks, &urls);
        let expected = urls
            .iter()
            .max_by_key(|url| rendezvous_affinity(url, "s3://bucket/metrics.parquet"))
            .unwrap()
            .clone();

        assert_eq!(routes, vec![expected]);
    }
}
