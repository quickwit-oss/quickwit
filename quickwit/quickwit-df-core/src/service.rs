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
//! [`DataFusionService`] is the core query execution entry point: it holds an
//! `Arc<DataFusionSessionBuilder>` and exposes `execute_substrait` and
//! `execute_sql` methods that return streaming `RecordBatch` iterators.
//!
//! ## No tonic / gRPC coupling
//!
//! This struct has zero gRPC dependencies. `quickwit_df_core::grpc`
//! provides the tonic adapter that encodes batches as Arrow IPC, and
//! `quickwit-serve/src/datafusion_api/setup.rs` mounts that service in OSS.
//! A downstream caller can do the same from its own handler, calling
//! `execute_substrait(&[u8])` and streaming the resulting batches in its own
//! proto response format.
//!
//! ## Usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use quickwit_datafusion::{DataFusionService, DataFusionSessionBuilder};
//!
//! let builder = Arc::new(DataFusionSessionBuilder::new().with_runtime_plugin(my_plugin));
//! let service = DataFusionService::new(Arc::clone(&builder));
//!
//! let mut stream = service.execute_substrait(&plan_bytes).await?;
//! while let Some(batch) = stream.next().await {
//!     // handle batch
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use crate::session::DataFusionSessionBuilder;

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

    /// Execute a Substrait plan encoded as protobuf bytes.
    ///
    /// Builds a fresh session via the underlying `DataFusionSessionBuilder`,
    /// decodes the plan, and returns a streaming `RecordBatch` iterator.
    /// The caller decides whether to collect, send via gRPC, or pipe to Arrow
    /// Flight — no materialization happens inside this method.
    ///
    /// `properties` flows from `ExecuteSubstraitRequest.properties` into
    /// `SessionConfig` overrides (e.g. `execution.target_partitions`). Pass
    /// an empty map when no overrides apply.
    #[tracing::instrument(skip(self, plan_bytes, properties), fields(plan_bytes_len = plan_bytes.len()))]
    pub async fn execute_substrait(
        &self,
        plan_bytes: &[u8],
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        tracing::info!(
            plan_bytes_len = plan_bytes.len(),
            num_properties = properties.len(),
            "executing substrait plan"
        );
        use datafusion_substrait::substrait::proto::Plan;
        use prost::Message;

        let plan = Plan::decode(plan_bytes)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        self.execute_substrait_plan(&plan, properties).await
    }

    /// Execute a Substrait plan from its proto3 JSON representation.
    ///
    /// Accepts the JSON format produced by DataFusion's `to_substrait_plan` +
    /// `serde_json::to_string`, or the `rollup_substrait.json` format used in
    /// integration tests and dev tooling.
    ///
    /// This is the dev/tooling path — grpcurl and Python scripts can pass the
    /// plan as a JSON string without pre-encoding to binary protobuf.
    pub async fn execute_substrait_json(
        &self,
        plan_json: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        use datafusion_substrait::substrait::proto::Plan;

        let plan: Plan = serde_json::from_str(plan_json).map_err(|e| {
            datafusion::error::DataFusionError::Plan(format!("invalid Substrait plan JSON: {e}"))
        })?;

        self.execute_substrait_plan(&plan, properties).await
    }

    async fn execute_substrait_plan(
        &self,
        plan: &datafusion_substrait::substrait::proto::Plan,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        let ctx = self.builder.build_session_with_properties(properties)?;
        crate::substrait::execute_substrait_plan_streaming(
            plan,
            &ctx,
            self.builder.substrait_extensions(),
        )
        .await
    }

    /// Like [`execute_substrait`], but returns the EXPLAIN output instead of
    /// running the plan. The returned stream emits `(plan_type, plan)` rows
    /// — same shape as a SQL `EXPLAIN VERBOSE`.
    pub async fn explain_substrait(
        &self,
        plan_bytes: &[u8],
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        use datafusion_substrait::substrait::proto::Plan;
        use prost::Message;
        let plan = Plan::decode(plan_bytes)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        self.explain_substrait_plan(&plan, properties).await
    }

    /// JSON variant of [`explain_substrait`] — same semantics, proto3-JSON input.
    pub async fn explain_substrait_json(
        &self,
        plan_json: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        use datafusion_substrait::substrait::proto::Plan;
        let plan: Plan = serde_json::from_str(plan_json).map_err(|e| {
            datafusion::error::DataFusionError::Plan(format!("invalid Substrait plan JSON: {e}"))
        })?;
        self.explain_substrait_plan(&plan, properties).await
    }

    async fn explain_substrait_plan(
        &self,
        plan: &datafusion_substrait::substrait::proto::Plan,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        let ctx = self.builder.build_session_with_properties(properties)?;
        crate::substrait::explain_substrait_plan_streaming(
            plan,
            &ctx,
            self.builder.substrait_extensions(),
        )
        .await
    }

    /// Execute one or more SQL statements from a single SQL string.
    ///
    /// DDL statements (e.g. `CREATE EXTERNAL TABLE`) are executed for side
    /// effects.  The last statement produces the result stream.
    ///
    /// Returns an error if `sql` contains no statements, or if any statement
    /// fails to parse or execute.
    #[tracing::instrument(skip(self, sql, properties), fields(sql_len = sql.len()))]
    pub async fn execute_sql(
        &self,
        sql: &str,
        properties: &HashMap<String, String>,
    ) -> DFResult<SendableRecordBatchStream> {
        tracing::info!(
            sql_len = sql.len(),
            num_properties = properties.len(),
            "executing SQL query"
        );
        let ctx = self.builder.build_session_with_properties(properties)?;
        let mut statements = split_sql_statements(&ctx, sql)?;
        let last = statements
            .pop()
            .ok_or_else(|| DataFusionError::Plan("no SQL statements provided".to_string()))?;

        for statement in statements {
            ctx.sql(&statement).await?.collect().await?;
        }

        let df = ctx.sql(&last).await?;
        df.execute_stream().await
    }
}
