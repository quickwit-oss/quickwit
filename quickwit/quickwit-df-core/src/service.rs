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
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion_distributed::{
    DistributedExec, DistributedMetricsFormat, display_plan_ascii,
    rewrite_distributed_plan_with_metrics,
};
use datafusion_substrait::substrait::proto::Plan as SubstraitPlan;

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

impl DataFusionExecutionMetadata {
    pub fn physical_plan_display(&self) -> String {
        physical_plan_display(&self.physical_plan)
    }
}

impl SubstraitExecutionMetadata {
    pub fn physical_plan_display(&self) -> String {
        physical_plan_display(&self.physical_plan)
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
        let stream = match request.output {
            DataFusionOutput::Records => {
                execute_stream(Arc::clone(&physical_plan), ctx.task_ctx())?
            }
            DataFusionOutput::Explain => explain_stream(&logical_plan_display, &physical_plan)?,
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

fn explain_stream(
    logical_plan: &str,
    physical_plan: &Arc<dyn ExecutionPlan>,
) -> DFResult<SendableRecordBatchStream> {
    let schema = explain_schema();
    let physical_plan = physical_plan_display(physical_plan);
    let plan_type: ArrayRef = Arc::new(StringArray::from(vec!["logical_plan", "physical_plan"]));
    let plan: ArrayRef = Arc::new(StringArray::from(vec![
        logical_plan,
        physical_plan.as_str(),
    ]));
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

fn physical_plan_display(physical_plan: &Arc<dyn ExecutionPlan>) -> String {
    if physical_plan.as_any().is::<DistributedExec>() {
        return match rewrite_distributed_plan_with_metrics(
            Arc::clone(physical_plan),
            DistributedMetricsFormat::PerTask,
        ) {
            Ok(physical_plan) => display_plan_ascii(physical_plan.as_ref(), true),
            Err(_) => display_plan_ascii(physical_plan.as_ref(), false),
        };
    }

    DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
        .indent(false)
        .to_string()
}
