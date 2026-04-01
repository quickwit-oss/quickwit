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
//! `POST /api/v1/metrics/sql` accepts a SQL string (optionally multi-statement,
//! e.g. a DDL + SELECT pair) and returns the results as Arrow IPC stream bytes.
//!
//! Example:
//! ```
//! POST /api/v1/metrics/sql
//! Content-Type: text/plain
//!
//! CREATE EXTERNAL TABLE "my-index" (
//!   metric_name VARCHAR NOT NULL,
//!   timestamp_secs BIGINT NOT NULL,
//!   value DOUBLE NOT NULL,
//!   service VARCHAR
//! ) STORED AS metrics LOCATION 'my-index';
//! SELECT metric_name, AVG(value) FROM "my-index" GROUP BY metric_name;
//! ```

use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_sql::parser::DFParserBuilder;
use quickwit_datafusion::session::MetricsSessionBuilder;
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

/// Register the `POST /api/v1/metrics/sql` route.
pub(crate) fn metrics_sql_handler(
    session_builder_opt: Option<Arc<MetricsSessionBuilder>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("metrics" / "sql")
        .and(warp::post())
        .and(warp::body::bytes())
        .and(with_arg(session_builder_opt))
        .then(
            |body: bytes::Bytes,
             session_builder_opt: Option<Arc<MetricsSessionBuilder>>| async move {
                build_sql_response(body, session_builder_opt).await
            },
        )
        .boxed()
}

async fn build_sql_response(
    body: bytes::Bytes,
    session_builder_opt: Option<Arc<MetricsSessionBuilder>>,
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

    match execute_sql_query(&sql, session_builder_opt).await {
        Ok(ipc_bytes) => Response::builder()
            .status(hyper::StatusCode::OK)
            .header("content-type", "application/vnd.apache.arrow.stream")
            .body(bytes::Bytes::from(ipc_bytes))
            .unwrap(),
        Err(err) => error_response(err.status_code(), &err.to_string()),
    }
}

fn error_response(status: hyper::StatusCode, message: &str) -> Response<bytes::Bytes> {
    let body = serde_json::json!({"error": message}).to_string();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(bytes::Bytes::from(body))
        .unwrap()
}

async fn execute_sql_query(
    sql: &str,
    session_builder_opt: Option<Arc<MetricsSessionBuilder>>,
) -> Result<Vec<u8>, MetricsSqlError> {
    let session_builder = session_builder_opt
        .ok_or(MetricsSqlError::NotConfigured)?;

    let ctx = session_builder
        .build_session()
        .map_err(|e| MetricsSqlError::Session(e.to_string()))?;

    debug!(sql_len = sql.len(), "executing metrics SQL query");

    let df = execute_sql_statements(&ctx, sql)
        .await
        .map_err(|e| MetricsSqlError::Sql(e.to_string()))?;

    let batches = df
        .collect()
        .await
        .map_err(|e| MetricsSqlError::Sql(e.to_string()))?;

    encode_arrow_ipc(&batches).map_err(|e| MetricsSqlError::Ipc(e.to_string()))
}

/// Execute one or more SQL statements sequentially.
///
/// DDL statements (like CREATE EXTERNAL TABLE) are executed for their side effects.
/// The last statement's `DataFrame` is returned.
///
/// Note: We parse each semicolon-delimited fragment individually because
/// `DFParser::parse_statement()` consumes the trailing semicolon, which
/// breaks the multi-statement loop in `parse_statements()`.
async fn execute_sql_statements(
    ctx: &SessionContext,
    sql: &str,
) -> datafusion::error::Result<datafusion::dataframe::DataFrame> {
    // Split on semicolons and execute each non-empty fragment
    let fragments: Vec<&str> = sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

    if fragments.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "no SQL statements provided".to_string(),
        ));
    }

    let mut last_df = None;
    for fragment in fragments {
        let mut stmts = DFParserBuilder::new(fragment).build()?.parse_statements()?;
        let stmt = stmts.pop_front().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "failed to parse SQL fragment".to_string(),
            )
        })?;
        let plan = ctx.state().statement_to_plan(stmt).await?;
        let df = ctx.execute_logical_plan(plan).await?;
        last_df = Some(df);
    }

    last_df.ok_or_else(|| {
        datafusion::error::DataFusionError::Plan("no query statement found".to_string())
    })
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
