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

//! REST endpoint for DataFusion SQL queries over parquet metrics.
//!
//! `POST /api/v1/metrics/sql[?explain=true]`
//!
//! Body: UTF-8 SQL string. Multiple statements separated by `;` are supported
//! (e.g. a `CREATE OR REPLACE EXTERNAL TABLE` DDL followed by a `SELECT`).
//!
//! Returns Arrow IPC stream bytes (`content-type: application/vnd.apache.arrow.stream`).
//! With `?explain=true`, returns the physical plan as a single-row Arrow IPC batch
//! with schema `{plan: Utf8}` — useful for verifying distributed plan shape in tests.

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_sql::parser::DFParserBuilder;
use quickwit_datafusion::DataFusionSessionBuilder;
use serde::Deserialize;
use tracing::debug;
use warp::Filter;
use warp::http::Response;

use crate::with_arg;

#[derive(Debug, thiserror::Error)]
pub enum MetricsSqlError {
    #[error("no session builder configured (searcher role not active)")]
    NotConfigured,
    #[error("session creation failed: {0}")]
    Session(String),
    #[error("SQL execution failed: {0}")]
    Sql(String),
    #[error("IPC serialization failed: {0}")]
    Ipc(String),
}

impl warp::reject::Reject for MetricsSqlError {}

impl MetricsSqlError {
    fn status_code(&self) -> hyper::StatusCode {
        match self {
            Self::NotConfigured => hyper::StatusCode::SERVICE_UNAVAILABLE,
            _ => hyper::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct MetricsSqlParams {
    /// If `true`, return the physical plan text instead of executing the query.
    #[serde(default)]
    pub explain: bool,
}

/// Register the `POST /api/v1/metrics/sql[?explain=true]` route.
pub(crate) fn metrics_sql_handler(
    session_builder_opt: Option<Arc<DataFusionSessionBuilder>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("metrics" / "sql")
        .and(warp::post())
        .and(warp::query::<MetricsSqlParams>())
        .and(warp::body::bytes())
        .and(with_arg(session_builder_opt))
        .then(
            |params: MetricsSqlParams,
             body: bytes::Bytes,
             session_builder_opt: Option<Arc<DataFusionSessionBuilder>>| async move {
                build_sql_response(body, params, session_builder_opt).await
            },
        )
        .boxed()
}

async fn build_sql_response(
    body: bytes::Bytes,
    params: MetricsSqlParams,
    session_builder_opt: Option<Arc<DataFusionSessionBuilder>>,
) -> Response<bytes::Bytes> {
    let sql = match std::str::from_utf8(&body) {
        Ok(s) => s.to_string(),
        Err(_) => {
            return error_response(
                hyper::StatusCode::BAD_REQUEST,
                "request body is not valid UTF-8",
            );
        }
    };

    let session_builder = match session_builder_opt {
        Some(sb) => sb,
        None => return error_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            MetricsSqlError::NotConfigured.to_string().as_str(),
        ),
    };

    let ctx = match session_builder.build_session() {
        Ok(ctx) => ctx,
        Err(e) => return error_response(
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            &MetricsSqlError::Session(e.to_string()).to_string(),
        ),
    };

    debug!(sql_len = sql.len(), explain = params.explain, "executing metrics SQL");

    if params.explain {
        // Execute DDL statements, then return the physical plan of the last query.
        match get_physical_plan_text(&ctx, &sql).await {
            Ok(plan_text) => encode_plan_as_ipc(&plan_text),
            Err(e) => error_response(
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                &MetricsSqlError::Sql(e.to_string()).to_string(),
            ),
        }
    } else {
        match execute_sql_statements(&ctx, &sql).await {
            Ok(batches) => match encode_arrow_ipc(&batches) {
                Ok(bytes) => Response::builder()
                    .status(hyper::StatusCode::OK)
                    .header("content-type", "application/vnd.apache.arrow.stream")
                    .body(bytes::Bytes::from(bytes))
                    .unwrap(),
                Err(e) => error_response(
                    hyper::StatusCode::INTERNAL_SERVER_ERROR,
                    &MetricsSqlError::Ipc(e.to_string()).to_string(),
                ),
            },
            Err(e) => error_response(
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                &MetricsSqlError::Sql(e.to_string()).to_string(),
            ),
        }
    }
}

/// Get the indented physical plan text for the last SQL statement (for `?explain=true`).
async fn get_physical_plan_text(
    ctx: &SessionContext,
    sql: &str,
) -> datafusion::error::Result<String> {
    let fragments: Vec<&str> = sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
    let Some(last) = fragments.last() else {
        return Err(datafusion::error::DataFusionError::Plan("no SQL provided".to_string()));
    };

    // Execute all but the last statement (DDL side effects)
    for fragment in fragments.iter().rev().skip(1).rev() {
        let mut stmts = DFParserBuilder::new(fragment).build()?.parse_statements()?;
        if let Some(stmt) = stmts.pop_front() {
            let plan = ctx.state().statement_to_plan(stmt).await?;
            ctx.execute_logical_plan(plan).await?;
        }
    }

    // Create physical plan for the last statement without executing
    let mut stmts = DFParserBuilder::new(last).build()?.parse_statements()?;
    let stmt = stmts.pop_front().ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("empty final statement".to_string())
    })?;
    let logical_plan = ctx.state().statement_to_plan(stmt).await?;
    let df = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = df.create_physical_plan().await?;
    Ok(format!(
        "{}",
        datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true)
    ))
}

fn encode_plan_as_ipc(plan_text: &str) -> Response<bytes::Bytes> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("plan", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![plan_text]))],
    );
    match batch {
        Ok(batch) => {
            let mut buf = Vec::new();
            if let Ok(mut writer) = StreamWriter::try_new(&mut buf, &schema) {
                let _ = writer.write(&batch);
                let _ = writer.finish();
            }
            Response::builder()
                .status(hyper::StatusCode::OK)
                .header("content-type", "application/vnd.apache.arrow.stream")
                .body(bytes::Bytes::from(buf))
                .unwrap()
        }
        Err(e) => error_response(hyper::StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// Execute one or more SQL statements sequentially.
///
/// DDL statements (like CREATE EXTERNAL TABLE) are executed for their side effects.
/// The last statement's results are returned.
///
/// Note: `DFParser::parse_statement()` consumes the trailing `;`, which breaks
/// the multi-statement loop in `parse_statements()`.  We split on `;` manually.
async fn execute_sql_statements(
    ctx: &SessionContext,
    sql: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let fragments: Vec<&str> = sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

    if fragments.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "no SQL statements provided".to_string(),
        ));
    }

    let mut last_batches = Vec::new();
    for fragment in fragments {
        let mut stmts = DFParserBuilder::new(fragment).build()?.parse_statements()?;
        let stmt = stmts.pop_front().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan("failed to parse SQL fragment".to_string())
        })?;
        let plan = ctx.state().statement_to_plan(stmt).await?;
        let df = ctx.execute_logical_plan(plan).await?;
        last_batches = df.collect().await?;
    }

    Ok(last_batches)
}

fn encode_arrow_ipc(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let mut ipc_buf = Vec::new();
    if let Some(batch) = batches.first() {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, &batch.schema())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(ipc_buf)
}

fn error_response(status: hyper::StatusCode, message: &str) -> Response<bytes::Bytes> {
    let body = serde_json::json!({"error": message}).to_string();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(bytes::Bytes::from(body))
        .unwrap()
}
