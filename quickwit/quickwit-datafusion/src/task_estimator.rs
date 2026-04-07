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

//! Generic task estimator for distributed execution of parquet-backed queries.
//!
//! Uses the number of file groups in a `DataSourceExec` (one per split) to
//! determine how many distributed tasks to create.  No data-source-specific code.

use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_distributed::{PartitionIsolatorExec, TaskEstimation, TaskEstimator};

/// Estimates the desired task count for distributed execution by counting
/// the number of parquet file groups (= number of splits) in the plan.
#[derive(Debug)]
pub struct QuickwitTaskEstimator;

impl TaskEstimator for QuickwitTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let (file_config, _parquet_source) = dse.downcast_to_file_source::<ParquetSource>()?;
        let num_file_groups = file_config.file_groups.len();
        if num_file_groups == 0 {
            return Some(TaskEstimation::maximum(1));
        }
        Some(TaskEstimation::desired(num_file_groups))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let (_file_config, _parquet_source) = dse.downcast_to_file_source::<ParquetSource>()?;
        if task_count <= 1 {
            return Some(Arc::clone(plan));
        }
        Some(Arc::new(PartitionIsolatorExec::new(
            Arc::clone(plan),
            task_count,
        )))
    }
}
