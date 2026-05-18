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

//! Pure-Rust DataFusion query execution service.
//!
//! [`DataFusionService`] is the core query execution entry point. It has no
//! tonic or gRPC dependencies: protocol adapters translate their request shape
//! into a [`DataFusionRequest`] and call [`DataFusionService::execute`].
//!
//! SQL/Substrait input, normal/explain output, and metadata collection all share
//! the same planning path. This keeps the service API small and makes metadata a
//! property of every execution instead of a second set of methods.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::{Metric, MetricType, MetricValue, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion_distributed::{
    DistributedExec, DistributedMetricsFormat, NetworkBoundaryExt, display_plan_ascii,
    rewrite_distributed_plan_with_metrics,
};
use datafusion_substrait::substrait::proto::Plan as SubstraitPlan;
use futures::StreamExt;

use crate::session::DataFusionSessionBuilder;

#[derive(Clone, Copy)]
pub enum DataFusionInput<'a> {
    Sql(&'a str),
    SubstraitBytes(&'a [u8]),
    SubstraitJson(&'a str),
}

impl DataFusionInput<'_> {
    fn summary(self) -> (&'static str, usize) {
        match self {
            Self::Sql(sql) => ("sql", sql.len()),
            Self::SubstraitBytes(plan_bytes) => ("substrait_bytes", plan_bytes.len()),
            Self::SubstraitJson(plan_json) => ("substrait_json", plan_json.len()),
        }
    }
}

impl fmt::Debug for DataFusionInput<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, len) = self.summary();
        f.debug_struct("DataFusionInput")
            .field("kind", &kind)
            .field("len", &len)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DataFusionOutput {
    #[default]
    Records,
    Explain,
    ExplainAnalyze,
}

#[derive(Clone, Copy)]
pub struct DataFusionRequest<'a> {
    pub input: DataFusionInput<'a>,
    pub output: DataFusionOutput,
    pub properties: &'a HashMap<String, String>,
}

impl<'a> DataFusionRequest<'a> {
    pub fn records(
        input: DataFusionInput<'a>,
        properties: &'a HashMap<String, String>,
    ) -> DataFusionRequest<'a> {
        Self {
            input,
            output: DataFusionOutput::Records,
            properties,
        }
    }

    pub fn explain(
        input: DataFusionInput<'a>,
        properties: &'a HashMap<String, String>,
    ) -> DataFusionRequest<'a> {
        Self {
            input,
            output: DataFusionOutput::Explain,
            properties,
        }
    }

    pub fn explain_analyze(
        input: DataFusionInput<'a>,
        properties: &'a HashMap<String, String>,
    ) -> DataFusionRequest<'a> {
        Self {
            input,
            output: DataFusionOutput::ExplainAnalyze,
            properties,
        }
    }
}

impl fmt::Debug for DataFusionRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFusionRequest")
            .field("input", &self.input)
            .field("output", &self.output)
            .field("num_properties", &self.properties.len())
            .finish()
    }
}

pub struct DataFusionExecution {
    pub stream: SendableRecordBatchStream,
    pub metadata: DataFusionExecutionMetadata,
}

pub struct SubstraitExecution {
    pub stream: SendableRecordBatchStream,
    pub metadata: SubstraitExecutionMetadata,
}

pub struct DataFusionExecutionMetadata {
    pub input: DataFusionInputMetadata,
    pub output: DataFusionOutput,
    pub logical_plan: String,
    pub physical_plan: Arc<dyn ExecutionPlan>,
    pub input_decode_duration: Duration,
    pub logical_planning_duration: Duration,
    pub physical_planning_duration: Duration,
    pub stream_creation_duration: Duration,
    pub analyze_execution_duration: Option<Duration>,
    pub analyze_output_rows: Option<usize>,
}

pub struct SubstraitExecutionMetadata {
    pub substrait_plan_json: String,
    pub logical_plan: String,
    pub physical_plan: Arc<dyn ExecutionPlan>,
    pub substrait_decode_duration: Duration,
    pub substrait_to_logical_duration: Duration,
    pub logical_to_physical_duration: Duration,
    pub stream_creation_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct DataFusionPhysicalPlanMetadata {
    pub display: String,
    pub statistics: DataFusionExecutionStatistics,
}

#[derive(Debug, Clone, Default)]
pub struct DataFusionExecutionStatistics {
    pub num_stages: usize,
    pub num_tasks: usize,
    pub elapsed_compute: Option<Duration>,
    pub repartition_time: Option<Duration>,
    pub time_elapsed_processing: Option<Duration>,
    pub output_rows: Option<usize>,
    pub output_bytes: Option<usize>,
    pub output_batches: Option<usize>,
    pub spill_count: Option<usize>,
    pub spilled_bytes: Option<usize>,
    pub spilled_rows: Option<usize>,
    pub aggregate_metrics: Vec<DataFusionMetricStatistics>,
    pub plan_metrics: Vec<DataFusionPlanMetricStatistics>,
}

#[derive(Debug, Clone)]
pub struct DataFusionPlanMetricStatistics {
    pub node_path: String,
    pub node_name: String,
    pub metric: DataFusionMetricStatistics,
}

#[derive(Debug, Clone)]
pub struct DataFusionMetricStatistics {
    pub name: String,
    pub partition: Option<usize>,
    pub metric_type: &'static str,
    pub value: Option<usize>,
    pub display_value: String,
    pub labels: Vec<(String, String)>,
    pub pruning: Option<DataFusionPruningStatistics>,
    pub ratio: Option<DataFusionRatioStatistics>,
}

#[derive(Debug, Clone)]
pub struct DataFusionPruningStatistics {
    pub pruned: usize,
    pub matched: usize,
    pub fully_matched: usize,
}

#[derive(Debug, Clone)]
pub struct DataFusionRatioStatistics {
    pub part: usize,
    pub total: usize,
}

impl DataFusionExecutionMetadata {
    pub fn physical_plan_display(&self) -> String {
        physical_plan_display(&self.physical_plan)
    }

    pub fn physical_plan_metadata(&self) -> DataFusionPhysicalPlanMetadata {
        physical_plan_metadata(&self.physical_plan)
    }
}

impl SubstraitExecutionMetadata {
    pub fn physical_plan_display(&self) -> String {
        physical_plan_display(&self.physical_plan)
    }
}

impl DataFusionExecutionStatistics {
    pub fn to_json_string(&self) -> String {
        self.to_json_value().to_string()
    }

    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "num_stages": self.num_stages,
            "num_tasks": self.num_tasks,
            "elapsed_compute_ms": self.elapsed_compute.map(duration_as_millis_f64),
            "repartition_time_ms": self.repartition_time.map(duration_as_millis_f64),
            "time_elapsed_processing_ms": self.time_elapsed_processing.map(duration_as_millis_f64),
            "output_rows": self.output_rows,
            "output_bytes": self.output_bytes,
            "output_batches": self.output_batches,
            "spill_count": self.spill_count,
            "spilled_bytes": self.spilled_bytes,
            "spilled_rows": self.spilled_rows,
            "aggregate_metrics": self
                .aggregate_metrics
                .iter()
                .map(DataFusionMetricStatistics::to_json_value)
                .collect::<Vec<_>>(),
            "plan_metrics": self
                .plan_metrics
                .iter()
                .map(DataFusionPlanMetricStatistics::to_json_value)
                .collect::<Vec<_>>(),
        })
    }
}

impl DataFusionPlanMetricStatistics {
    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "node_path": self.node_path,
            "node_name": self.node_name,
            "metric": self.metric.to_json_value(),
        })
    }
}

impl DataFusionMetricStatistics {
    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "partition": self.partition,
            "metric_type": self.metric_type,
            "value": self.value,
            "display_value": self.display_value,
            "labels": self
                .labels
                .iter()
                .map(|(name, value)| serde_json::json!({
                    "name": name,
                    "value": value,
                }))
                .collect::<Vec<_>>(),
            "pruning": self.pruning.as_ref().map(DataFusionPruningStatistics::to_json_value),
            "ratio": self.ratio.as_ref().map(DataFusionRatioStatistics::to_json_value),
        })
    }
}

impl DataFusionPruningStatistics {
    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "pruned": self.pruned,
            "matched": self.matched,
            "fully_matched": self.fully_matched,
            "total": self.pruned + self.matched,
        })
    }
}

impl DataFusionRatioStatistics {
    fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "part": self.part,
            "total": self.total,
        })
    }
}

pub enum DataFusionInputMetadata {
    Sql { sql: String },
    Substrait { plan_json: String },
}

impl DataFusionInputMetadata {
    pub fn sql(&self) -> Option<&str> {
        match self {
            Self::Sql { sql } => Some(sql.as_str()),
            Self::Substrait { .. } => None,
        }
    }

    pub fn substrait_plan_json(&self) -> Option<&str> {
        match self {
            Self::Sql { .. } => None,
            Self::Substrait { plan_json } => Some(plan_json.as_str()),
        }
    }
}

struct PreparedLogicalPlan {
    input: DataFusionInputMetadata,
    logical_plan: LogicalPlan,
    input_decode_duration: Duration,
    logical_planning_duration: Duration,
}

/// Split a SQL string into top-level statements using the configured dialect's
/// tokenizer rather than raw string splitting.
pub fn split_sql_statements(ctx: &SessionContext, sql: &str) -> DFResult<Vec<String>> {
    let state = ctx.state();
    let tokens = {
        let options = state.config().options();
        let dialect = dialect_from_str(options.sql_parser.dialect).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "unsupported SQL dialect: {}",
                options.sql_parser.dialect
            ))
        })?;

        Tokenizer::new(dialect.as_ref(), sql)
            .with_unescape(false)
            .tokenize()
            .map_err(|err| DataFusionError::Plan(err.to_string()))?
    };

    let mut statements = Vec::new();
    let mut current = String::new();
    let mut has_content = false;

    for token in tokens {
        if token == Token::SemiColon {
            if has_content {
                statements.push(std::mem::take(&mut current));
                has_content = false;
            } else {
                current.clear();
            }
            continue;
        }

        if !matches!(token, Token::Whitespace(_)) {
            has_content = true;
        }
        current.push_str(&token.to_string());
    }

    if has_content {
        statements.push(current);
    }

    if statements.is_empty() {
        return Err(DataFusionError::Plan(
            "no SQL statements provided".to_string(),
        ));
    }
    Ok(statements)
}

/// Pure-Rust query execution service backed by a `DataFusionSessionBuilder`.
///
/// Owns an `Arc<DataFusionSessionBuilder>` and dispatches queries to it.
/// No tonic or gRPC types appear in this struct's public API.
#[derive(Clone)]
pub struct DataFusionService {
    builder: Arc<DataFusionSessionBuilder>,
}

impl std::fmt::Debug for DataFusionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionService")
            .field("builder", &self.builder)
            .finish()
    }
}

impl DataFusionService {
    /// Create a new service wrapping the given session builder.
    pub fn new(builder: Arc<DataFusionSessionBuilder>) -> Self {
        Self { builder }
    }

    /// Execute a SQL or Substrait query and return both the lazy output stream
    /// and planning metadata.
    #[tracing::instrument(skip(self, request), fields(output = ?request.output))]
    pub async fn execute(&self, request: DataFusionRequest<'_>) -> DFResult<DataFusionExecution> {
        let (input_kind, input_len) = request.input.summary();
        tracing::info!(
            input_kind,
            input_len,
            output = ?request.output,
            num_properties = request.properties.len(),
            "executing DataFusion query"
        );

        let ctx = self
            .builder
            .build_session_with_properties(request.properties)?;
        let prepared = self.prepare_logical_plan(request.input, &ctx).await?;
        let logical_plan_display = prepared.logical_plan.display_indent().to_string();

        let physical_planning_start = Instant::now();
        let physical_plan = ctx
            .state()
            .create_physical_plan(&prepared.logical_plan)
            .await?;
        let physical_planning_duration = physical_planning_start.elapsed();

        let stream_creation_start = Instant::now();
        let mut analyze_execution_duration = None;
        let mut analyze_output_rows = None;
        let stream = match request.output {
            DataFusionOutput::Records => {
                execute_stream(Arc::clone(&physical_plan), ctx.task_ctx())?
            }
            DataFusionOutput::Explain => {
                explain_stream(&logical_plan_display, &physical_plan, None)?
            }
            DataFusionOutput::ExplainAnalyze => {
                let analyze_execution_start = Instant::now();
                let num_rows =
                    execute_plan_for_metrics(Arc::clone(&physical_plan), ctx.task_ctx()).await?;
                analyze_execution_duration = Some(analyze_execution_start.elapsed());
                analyze_output_rows = Some(num_rows);
                let physical_plan_metadata = physical_plan_metadata(&physical_plan);
                explain_stream(
                    &logical_plan_display,
                    &physical_plan,
                    Some(&physical_plan_metadata),
                )?
            }
        };
        let stream_creation_duration = stream_creation_start.elapsed();

        Ok(DataFusionExecution {
            stream,
            metadata: DataFusionExecutionMetadata {
                input: prepared.input,
                output: request.output,
                logical_plan: logical_plan_display,
                physical_plan,
                input_decode_duration: prepared.input_decode_duration,
                logical_planning_duration: prepared.logical_planning_duration,
                physical_planning_duration,
                stream_creation_duration,
                analyze_execution_duration,
                analyze_output_rows,
            },
        })
    }

    /// Execute a Substrait plan encoded as protobuf bytes.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn execute_substrait(
        &self,
        plan_bytes: &[u8],
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(self
            .execute(DataFusionRequest::records(
                DataFusionInput::SubstraitBytes(plan_bytes),
                properties,
            ))
            .await?
            .stream)
    }

    /// Execute a Substrait plan and return planning metadata.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn execute_substrait_with_metadata(
        &self,
        plan_bytes: &[u8],
        properties: &HashMap<String, String>,
    ) -> DFResult<SubstraitExecution> {
        substrait_execution_from_datafusion(
            self.execute(DataFusionRequest::records(
                DataFusionInput::SubstraitBytes(plan_bytes),
                properties,
            ))
            .await?,
        )
    }

    /// Execute a Substrait plan from its proto3 JSON representation.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn execute_substrait_json(
        &self,
        plan_json: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(self
            .execute(DataFusionRequest::records(
                DataFusionInput::SubstraitJson(plan_json),
                properties,
            ))
            .await?
            .stream)
    }

    /// Execute a proto3 JSON Substrait plan and return planning metadata.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn execute_substrait_json_with_metadata(
        &self,
        plan_json: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SubstraitExecution> {
        substrait_execution_from_datafusion(
            self.execute(DataFusionRequest::records(
                DataFusionInput::SubstraitJson(plan_json),
                properties,
            ))
            .await?,
        )
    }

    /// Return EXPLAIN output for a Substrait plan encoded as protobuf bytes.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn explain_substrait(
        &self,
        plan_bytes: &[u8],
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(self
            .execute(DataFusionRequest::explain(
                DataFusionInput::SubstraitBytes(plan_bytes),
                properties,
            ))
            .await?
            .stream)
    }

    /// Return EXPLAIN output and planning metadata for a Substrait plan.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn explain_substrait_with_metadata(
        &self,
        plan_bytes: &[u8],
        properties: &HashMap<String, String>,
    ) -> DFResult<SubstraitExecution> {
        substrait_execution_from_datafusion(
            self.execute(DataFusionRequest::explain(
                DataFusionInput::SubstraitBytes(plan_bytes),
                properties,
            ))
            .await?,
        )
    }

    /// Return EXPLAIN output for a proto3 JSON Substrait plan.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn explain_substrait_json(
        &self,
        plan_json: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(self
            .execute(DataFusionRequest::explain(
                DataFusionInput::SubstraitJson(plan_json),
                properties,
            ))
            .await?
            .stream)
    }

    /// Return EXPLAIN output and planning metadata for a proto3 JSON Substrait
    /// plan.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn explain_substrait_json_with_metadata(
        &self,
        plan_json: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SubstraitExecution> {
        substrait_execution_from_datafusion(
            self.execute(DataFusionRequest::explain(
                DataFusionInput::SubstraitJson(plan_json),
                properties,
            ))
            .await?,
        )
    }

    /// Execute one or more SQL statements from a single SQL string.
    ///
    /// Prefer [`DataFusionService::execute`] for new call sites.
    pub async fn execute_sql(
        &self,
        sql: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(self
            .execute(DataFusionRequest::records(
                DataFusionInput::Sql(sql),
                properties,
            ))
            .await?
            .stream)
    }

    async fn prepare_logical_plan(
        &self,
        input: DataFusionInput<'_>,
        ctx: &SessionContext,
    ) -> DFResult<PreparedLogicalPlan> {
        match input {
            DataFusionInput::Sql(sql) => self.prepare_sql(sql, ctx).await,
            DataFusionInput::SubstraitBytes(plan_bytes) => {
                self.prepare_substrait_bytes(plan_bytes, ctx).await
            }
            DataFusionInput::SubstraitJson(plan_json) => {
                self.prepare_substrait_json(plan_json, ctx).await
            }
        }
    }

    async fn prepare_sql(&self, sql: &str, ctx: &SessionContext) -> DFResult<PreparedLogicalPlan> {
        let input_decode_start = Instant::now();
        let mut statements = split_sql_statements(ctx, sql)?;
        let input_decode_duration = input_decode_start.elapsed();

        let last = statements
            .pop()
            .ok_or_else(|| DataFusionError::Plan("no SQL statements provided".to_string()))?;

        for statement in statements {
            ctx.sql(&statement).await?.collect().await?;
        }

        let logical_planning_start = Instant::now();
        let df = ctx.sql(&last).await?;
        let logical_plan = df.into_optimized_plan()?;
        let logical_planning_duration = logical_planning_start.elapsed();

        Ok(PreparedLogicalPlan {
            input: DataFusionInputMetadata::Sql {
                sql: sql.to_string(),
            },
            logical_plan,
            input_decode_duration,
            logical_planning_duration,
        })
    }

    async fn prepare_substrait_bytes(
        &self,
        plan_bytes: &[u8],
        ctx: &SessionContext,
    ) -> DFResult<PreparedLogicalPlan> {
        use prost::Message;

        let input_decode_start = Instant::now();
        let plan = SubstraitPlan::decode(plan_bytes)
            .map_err(|err| datafusion::error::DataFusionError::External(Box::new(err)))?;
        let plan_json = substrait_plan_json(&plan)?;
        let input_decode_duration = input_decode_start.elapsed();

        self.prepare_substrait_plan(plan, plan_json, input_decode_duration, ctx)
            .await
    }

    async fn prepare_substrait_json(
        &self,
        plan_json: &str,
        ctx: &SessionContext,
    ) -> DFResult<PreparedLogicalPlan> {
        let input_decode_start = Instant::now();
        let plan: SubstraitPlan = serde_json::from_str(plan_json).map_err(|err| {
            datafusion::error::DataFusionError::Plan(format!("invalid Substrait plan JSON: {err}"))
        })?;
        let input_decode_duration = input_decode_start.elapsed();

        self.prepare_substrait_plan(plan, plan_json.to_string(), input_decode_duration, ctx)
            .await
    }

    async fn prepare_substrait_plan(
        &self,
        plan: SubstraitPlan,
        plan_json: String,
        input_decode_duration: Duration,
        ctx: &SessionContext,
    ) -> DFResult<PreparedLogicalPlan> {
        let state = ctx.state();
        let logical_planning_start = Instant::now();
        let logical_plan = crate::substrait::logical_plan_from_substrait(
            &plan,
            &state,
            self.builder.substrait_extensions(),
        )
        .await?;
        let logical_planning_duration = logical_planning_start.elapsed();

        tracing::debug!(
            plan = %logical_plan.display_indent(),
            "substrait plan converted to DataFusion logical plan"
        );

        Ok(PreparedLogicalPlan {
            input: DataFusionInputMetadata::Substrait { plan_json },
            logical_plan,
            input_decode_duration,
            logical_planning_duration,
        })
    }
}

fn substrait_execution_from_datafusion(
    execution: DataFusionExecution,
) -> DFResult<SubstraitExecution> {
    let DataFusionExecution { stream, metadata } = execution;
    let DataFusionExecutionMetadata {
        input,
        output: _,
        logical_plan,
        physical_plan,
        input_decode_duration,
        logical_planning_duration,
        physical_planning_duration,
        stream_creation_duration,
        analyze_execution_duration: _,
        analyze_output_rows: _,
    } = metadata;
    let DataFusionInputMetadata::Substrait { plan_json } = input else {
        return Err(DataFusionError::Internal(
            "expected Substrait execution metadata".to_string(),
        ));
    };

    Ok(SubstraitExecution {
        stream,
        metadata: SubstraitExecutionMetadata {
            substrait_plan_json: plan_json,
            logical_plan,
            physical_plan,
            substrait_decode_duration: input_decode_duration,
            substrait_to_logical_duration: logical_planning_duration,
            logical_to_physical_duration: physical_planning_duration,
            stream_creation_duration,
        },
    })
}

fn substrait_plan_json(plan: &SubstraitPlan) -> DFResult<String> {
    serde_json::to_string(plan).map_err(|err| {
        datafusion::error::DataFusionError::Plan(format!(
            "failed to serialize Substrait plan JSON: {err}"
        ))
    })
}

async fn execute_plan_for_metrics(
    physical_plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<datafusion::execution::TaskContext>,
) -> DFResult<usize> {
    let mut stream = execute_stream(physical_plan, task_ctx)?;
    let mut num_rows = 0;
    while let Some(batch) = stream.next().await {
        num_rows += batch?.num_rows();
    }
    Ok(num_rows)
}

fn explain_stream(
    logical_plan: &str,
    physical_plan: &Arc<dyn ExecutionPlan>,
    physical_plan_metadata: Option<&DataFusionPhysicalPlanMetadata>,
) -> DFResult<SendableRecordBatchStream> {
    let schema = explain_schema();
    let physical_plan_display_text;
    let mut plan_types = vec!["logical_plan".to_string(), "physical_plan".to_string()];
    let mut plans = vec![logical_plan.to_string()];
    if let Some(physical_plan_metadata) = physical_plan_metadata {
        plans.push(physical_plan_metadata.display.clone());
        plan_types.push("execution_statistics_json".to_string());
        plans.push(physical_plan_metadata.statistics.to_json_string());
    } else {
        physical_plan_display_text = physical_plan_display(physical_plan);
        plans.push(physical_plan_display_text);
    }
    let plan_type: ArrayRef = Arc::new(StringArray::from(plan_types));
    let plan: ArrayRef = Arc::new(StringArray::from(plans));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![plan_type, plan]).map_err(|err| {
            DataFusionError::Execution(format!("failed to build explain output: {err}"))
        })?;

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok(batch)]),
    )))
}

fn explain_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("plan_type", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]))
}

fn physical_plan_metadata(
    physical_plan: &Arc<dyn ExecutionPlan>,
) -> DataFusionPhysicalPlanMetadata {
    let (physical_plan, show_metrics) = physical_plan_with_metrics(physical_plan);
    let display = if physical_plan.as_any().is::<DistributedExec>() {
        display_plan_ascii(physical_plan.as_ref(), show_metrics)
    } else {
        DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
            .indent(false)
            .to_string()
    };
    let statistics = extract_execution_statistics(physical_plan.as_ref());
    DataFusionPhysicalPlanMetadata {
        display,
        statistics,
    }
}

fn physical_plan_display(physical_plan: &Arc<dyn ExecutionPlan>) -> String {
    physical_plan_metadata(physical_plan).display
}

fn physical_plan_with_metrics(
    physical_plan: &Arc<dyn ExecutionPlan>,
) -> (Arc<dyn ExecutionPlan>, bool) {
    if physical_plan.as_any().is::<DistributedExec>() {
        return match rewrite_distributed_plan_with_metrics(
            Arc::clone(physical_plan),
            DistributedMetricsFormat::PerTask,
        ) {
            Ok(physical_plan) => (physical_plan, true),
            Err(_) => (Arc::clone(physical_plan), false),
        };
    }

    (Arc::clone(physical_plan), true)
}

fn extract_execution_statistics(
    physical_plan: &(dyn ExecutionPlan + 'static),
) -> DataFusionExecutionStatistics {
    let mut statistics = DataFusionExecutionStatistics {
        num_stages: 1,
        num_tasks: 1,
        ..Default::default()
    };
    let mut all_metrics = MetricsSet::new();
    collect_plan_metrics(
        physical_plan,
        "0".to_string(),
        &mut all_metrics,
        &mut statistics,
    );
    let aggregate_metrics = all_metrics.aggregate_by_name().sorted_for_display();
    statistics.elapsed_compute = aggregate_metrics
        .elapsed_compute()
        .map(duration_from_nanos_usize);
    statistics.repartition_time = aggregate_metrics
        .sum_by_name("repartition_time")
        .map(|metric| duration_from_nanos_usize(metric.as_usize()));
    statistics.time_elapsed_processing = aggregate_metrics
        .sum_by_name("time_elapsed_processing")
        .map(|metric| duration_from_nanos_usize(metric.as_usize()));
    statistics.output_rows = aggregate_metrics.output_rows();
    statistics.output_bytes = aggregate_metrics
        .sum(|metric| matches!(metric.value(), MetricValue::OutputBytes(_)))
        .map(|metric| metric.as_usize());
    statistics.output_batches = aggregate_metrics
        .sum(|metric| matches!(metric.value(), MetricValue::OutputBatches(_)))
        .map(|metric| metric.as_usize());
    statistics.spill_count = aggregate_metrics.spill_count();
    statistics.spilled_bytes = aggregate_metrics.spilled_bytes();
    statistics.spilled_rows = aggregate_metrics.spilled_rows();
    statistics.aggregate_metrics = aggregate_metrics
        .iter()
        .map(|metric| metric_statistics(metric))
        .collect();
    statistics
}

fn collect_plan_metrics(
    physical_plan: &(dyn ExecutionPlan + 'static),
    node_path: String,
    all_metrics: &mut MetricsSet,
    statistics: &mut DataFusionExecutionStatistics,
) {
    if let Some(network_boundary) = physical_plan.as_network_boundary() {
        statistics.num_tasks += network_boundary.input_stage().tasks.len();
        statistics.num_stages += 1;
    }

    if let Some(metrics) = physical_plan.metrics() {
        for metric in metrics.iter() {
            all_metrics.push(Arc::clone(metric));
            statistics
                .plan_metrics
                .push(DataFusionPlanMetricStatistics {
                    node_path: node_path.clone(),
                    node_name: physical_plan.name().to_string(),
                    metric: metric_statistics(metric),
                });
        }
    }

    for (idx, child) in physical_plan.children().into_iter().enumerate() {
        collect_plan_metrics(
            child.as_ref(),
            format!("{node_path}.{idx}"),
            all_metrics,
            statistics,
        );
    }
}

fn metric_statistics(metric: &Metric) -> DataFusionMetricStatistics {
    let value = metric.value();
    let pruning = match value {
        MetricValue::PruningMetrics {
            pruning_metrics, ..
        } => Some(DataFusionPruningStatistics {
            pruned: pruning_metrics.pruned(),
            matched: pruning_metrics.matched(),
            fully_matched: pruning_metrics.fully_matched(),
        }),
        _ => None,
    };
    let ratio = match value {
        MetricValue::Ratio { ratio_metrics, .. } => Some(DataFusionRatioStatistics {
            part: ratio_metrics.part(),
            total: ratio_metrics.total(),
        }),
        _ => None,
    };

    DataFusionMetricStatistics {
        name: value.name().to_string(),
        partition: metric.partition(),
        metric_type: metric_type_name(metric.metric_type()),
        value: if pruning.is_some() || ratio.is_some() {
            None
        } else {
            Some(value.as_usize())
        },
        display_value: value.to_string(),
        labels: metric
            .labels()
            .iter()
            .map(|label| (label.name().to_string(), label.value().to_string()))
            .collect(),
        pruning,
        ratio,
    }
}

fn metric_type_name(metric_type: MetricType) -> &'static str {
    match metric_type {
        MetricType::SUMMARY => "summary",
        MetricType::DEV => "dev",
    }
}

fn duration_from_nanos_usize(nanos: usize) -> Duration {
    Duration::from_nanos(nanos.min(u64::MAX as usize) as u64)
}

fn duration_as_millis_f64(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
