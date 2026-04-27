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

use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{PartitionIsolatorExec, TaskEstimation, TaskEstimator};

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
}
