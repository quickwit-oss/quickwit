//! REST handler for the experimental DataFusion SQL endpoint.
//!
//! `POST /api/v1/{index_id}/datafusion` accepts a SQL query and
//! returns results as JSON.
//!
//! Gated behind `SearcherConfig::enable_datafusion_endpoint`.

use std::sync::Arc;

use serde::Deserialize;
use warp::Filter;
use warp::reject::Rejection;

use crate::with_arg;
use crate::QuickwitServices;

#[derive(Debug, Deserialize)]
pub struct DataFusionRequest {
    /// SQL query to execute.
    pub sql: String,
}

async fn datafusion_handler(
    _index_id: String,
    request: DataFusionRequest,
    services: Arc<QuickwitServices>,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let ctx = services.datafusion_session_builder.build_session();

    // TODO: use index_id to register the right tables from the metastore.
    // For now, execute against whatever tables are registered on the session.

    let result: Result<String, String> = async {
        let df = ctx
            .sql(&request.sql)
            .await
            .map_err(|e| e.to_string())?;
        let results = df.collect().await.map_err(|e| e.to_string())?;
        let formatted =
            datafusion::common::arrow::util::pretty::pretty_format_batches(&results)
                .map_err(|e| e.to_string())?;
        Ok(formatted.to_string())
    }
    .await;

    match result {
        Ok(table) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({
                "results": table,
            })),
            warp::http::StatusCode::OK,
        )),
        Err(err) => Ok(warp::reply::with_status(
            warp::reply::json(&serde_json::json!({
                "error": err,
            })),
            warp::http::StatusCode::BAD_REQUEST,
        )),
    }
}

pub fn datafusion_handler_filter(
    services: Arc<QuickwitServices>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!(String / "datafusion")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_arg(services))
        .then(datafusion_handler)
}
