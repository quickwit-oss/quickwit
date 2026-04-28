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

//! Physical rewrites for sorted-series metrics rollups.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, Partitioning};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use quickwit_parquet_engine::sorted_series::SORTED_SERIES_COLUMN;

/// Replaces the inner sorted-series hash repartition in rollup plans with a
/// sort-preserving merge into a single final aggregate.
///
/// This keeps worker/file-local partial aggregation parallel, then lets the
/// coordinator stitch ordered per-series partial rows without hash-shuffling
/// those partials by `sorted_series`.
#[derive(Debug, Default)]
pub struct SortedSeriesStreamingAggregateRule;

impl PhysicalOptimizerRule for SortedSeriesStreamingAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let transformed = plan.transform_up(|plan| {
            if let Some(rewritten) = rewrite_sorted_series_final_aggregate(&plan)? {
                Ok(Transformed::yes(rewritten))
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        Ok(transformed.data)
    }

    fn name(&self) -> &str {
        "sorted_series_streaming_aggregate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite_sorted_series_final_aggregate(
    plan: &Arc<dyn ExecutionPlan>,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let Some(final_agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(None);
    };
    if final_agg.mode() != &AggregateMode::FinalPartitioned
        || !aggregate_groups_on_sorted_series(final_agg)
    {
        return Ok(None);
    }

    let Some(sort) = final_agg.input().as_any().downcast_ref::<SortExec>() else {
        return Ok(None);
    };
    if !sort.preserve_partitioning()
        || sort.fetch().is_some()
        || !ordering_starts_with_sorted_series(sort.expr())
    {
        return Ok(None);
    }

    let Some(repartition) = sort.input().as_any().downcast_ref::<RepartitionExec>() else {
        return Ok(None);
    };
    if !hash_partitioning_contains_sorted_series(repartition.partitioning()) {
        return Ok(None);
    }

    let ordering = sort.expr().clone();
    let repartition_input = Arc::clone(repartition.input());
    let partition_sort: Arc<dyn ExecutionPlan> = if repartition_input
        .equivalence_properties()
        .ordering_satisfy(ordering.clone())?
    {
        repartition_input
    } else {
        Arc::new(
            SortExec::new(ordering.clone(), repartition_input).with_preserve_partitioning(true),
        )
    };
    let merged: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(ordering, partition_sort));

    let rewritten = AggregateExec::try_new(
        AggregateMode::Final,
        final_agg.group_expr().clone(),
        final_agg.aggr_expr().to_vec(),
        final_agg.filter_expr().to_vec(),
        merged,
        final_agg.input_schema(),
    )?
    .with_limit_options(final_agg.limit_options());

    Ok(Some(Arc::new(rewritten)))
}

fn aggregate_groups_on_sorted_series(aggregate: &AggregateExec) -> bool {
    aggregate
        .group_expr()
        .expr()
        .iter()
        .any(|(expr, alias)| alias == SORTED_SERIES_COLUMN || is_sorted_series_column(expr))
}

fn hash_partitioning_contains_sorted_series(partitioning: &Partitioning) -> bool {
    let Partitioning::Hash(exprs, _) = partitioning else {
        return false;
    };
    exprs.iter().any(is_sorted_series_column)
}

fn ordering_starts_with_sorted_series(ordering: &LexOrdering) -> bool {
    is_sorted_series_column(&ordering.first().expr)
}

fn is_sorted_series_column(expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>) -> bool {
    match expr.as_any().downcast_ref::<Column>() {
        Some(column) => column.name() == SORTED_SERIES_COLUMN,
        None => false,
    }
}
