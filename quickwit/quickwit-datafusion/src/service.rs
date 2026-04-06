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
//! This struct has zero gRPC dependencies.  The OSS gRPC handler in
//! `quickwit-serve/src/datafusion_api/grpc_handler.rs` wraps it and encodes
//! each batch as Arrow IPC.  Pomsky does the same from its own CloudPrem
//! handler, calling `execute_substrait(&[u8])` and streaming the resulting
//! batches in its own proto response format.
//!
//! ## Usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use quickwit_datafusion::{DataFusionService, DataFusionSessionBuilder};
//!
//! let builder = Arc::new(DataFusionSessionBuilder::new().with_source(my_source));
//! let service = DataFusionService::new(Arc::clone(&builder));
//!
//! let mut stream = service.execute_substrait(&plan_bytes).await?;
//! while let Some(batch) = stream.next().await {
//!     // handle batch
//! }
//! ```

use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::execution::SendableRecordBatchStream;

use crate::session::DataFusionSessionBuilder;

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
    pub async fn execute_substrait(
        &self,
        plan_bytes: &[u8],
    ) -> DFResult<SendableRecordBatchStream> {
        use datafusion_substrait::substrait::proto::Plan;
        use prost::Message;

        let plan = Plan::decode(plan_bytes)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        self.execute_substrait_plan(&plan).await
    }

    /// Execute a Substrait plan from its proto3 JSON representation.
    ///
    /// Accepts the JSON format produced by DataFusion's `to_substrait_plan`
    /// + `serde_json::to_string`, or the `rollup_substrait.json` format used
    /// in integration tests and dev tooling.
    ///
    /// This is the dev/tooling path — grpcurl and Python scripts can pass the
    /// plan as a JSON string without pre-encoding to binary protobuf.
    pub async fn execute_substrait_json(
        &self,
        plan_json: &str,
    ) -> DFResult<SendableRecordBatchStream> {
        use datafusion_substrait::substrait::proto::Plan;

        let plan: Plan = serde_json::from_str(plan_json).map_err(|e| {
            datafusion::error::DataFusionError::Plan(format!(
                "invalid Substrait plan JSON: {e}"
            ))
        })?;

        self.execute_substrait_plan(&plan).await
    }

    async fn execute_substrait_plan(
        &self,
        plan: &datafusion_substrait::substrait::proto::Plan,
    ) -> DFResult<SendableRecordBatchStream> {
        let ctx = self.builder.build_session()?;
        crate::substrait::execute_substrait_plan_streaming(plan, &ctx, self.builder.sources()).await
    }

    /// Execute one or more semicolon-separated SQL statements.
    ///
    /// DDL statements (e.g. `CREATE EXTERNAL TABLE`) are executed for side
    /// effects.  The last statement produces the result stream.
    ///
    /// Returns an error if `sql` is empty after splitting, or if any statement
    /// fails to parse or execute.
    pub async fn execute_sql(&self, sql: &str) -> DFResult<SendableRecordBatchStream> {
        let ctx = self.builder.build_session()?;

        // Split on `;` and discard empty fragments (trailing `;` etc.).
        let statements: Vec<&str> = sql
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();

        if statements.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "no SQL statements provided".to_string(),
            ));
        }

        // Execute all but the last statement as DDL / side-effect statements.
        let (last, prefixes) = statements
            .split_last()
            .expect("non-empty after the check above");

        for stmt in prefixes {
            ctx.sql(stmt).await?.collect().await?;
        }

        // Execute the final statement and return the stream.
        let df = ctx.sql(last).await?;
        let stream = df.execute_stream().await?;
        Ok(stream)
    }
}
