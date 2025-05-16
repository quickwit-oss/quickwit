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

use quickwit_common::rate_limited_error;
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_doc_transforms::DatadogLogMsg;
use quickwit_ingest::DocBatchV2Builder;
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestRouterService, IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::types::{DocUidGenerator, IndexId};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{self, Serialize};
use tracing::{debug, error};
use warp::{Filter, Rejection};

use crate::decompression::get_body_bytes;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

const DATADOG_INDEX_ID: &str = "datadog";

#[derive(utoipa::OpenApi)]
#[openapi(paths(datadog_logs,))]
pub struct DatadogApi;

pub(crate) fn datadog_api_handlers(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_healthcheck()
        .or(datadog_logs(ingest_router.clone()))
        .boxed()
}

#[utoipa::path(get, tag = "Datadog Healthcheck Endpoint", path = "/api/v1/validate")]
pub(crate) fn datadog_healthcheck()
-> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_healthcheck_filter()
        .then(|| async move { warp::reply::with_status("ok", warp::http::StatusCode::OK) })
        .boxed()
}

pub(crate) fn datadog_healthcheck_filter() -> impl Filter<Extract = (), Error = Rejection> + Clone {
    let path_filter = warp::path!("api" / "v1" / "validate");
    path_filter.and(warp::get())
}

/// Based on vector agent logs endpoint:
/// https://github.com/vectordotdev/vector/blob/450de36904f3d1524057e8cdb736941194da8d22/src/sources/datadog_agent/mod.rs#L499
pub(crate) fn datadog_logs_filter() -> impl Filter<Extract = (Body,), Error = Rejection> + Clone {
    let path_filter = warp::path!("api" / "v1" / "input")
        .or(warp::path!("api" / "v2" / "logs"))
        .unify();
    path_filter
        .and(warp::post())
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/json",
        ))
        .and(get_body_bytes())
}

#[utoipa::path(
    post,
    tag = "Datadog Logs",
    path = "/api/v2/logs",
    request_body(content = String, description = "Datadog Log JSON message", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully exported logs.", body = bool),
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add docs to."),
    )
)]
pub(crate) fn datadog_logs(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_logs_filter()
        .and(with_arg(ingest_router))
        .and(warp::post())
        .then(|body, ingest_router| async move {
            datadog_ingest_logs(ingest_router, DATADOG_INDEX_ID.to_string(), body).await
        })
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}

#[derive(Debug, Clone, thiserror::Error, Serialize)]
pub enum DatadogApiError {
    #[error("invalid datadog log request: {0}")]
    InvalidPayload(String),
    #[error("error when ingesting payload: {0}")]
    Ingest(String),
    #[error("Datadog Log Preprocessing Panicked: {0}")]
    Panicked(String),
}

impl ServiceError for DatadogApiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            DatadogApiError::Panicked(_) => ServiceErrorCode::Internal,
            DatadogApiError::InvalidPayload(_) => ServiceErrorCode::BadRequest,
            DatadogApiError::Ingest(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "datadog internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
        }
    }
}

fn deserialize_datadog_log(data: &[u8]) -> Result<Vec<DatadogLogMsg>, DatadogApiError> {
    serde_json::from_slice(data).map_err(|error| {
        error!(
            message = "Failed to parse datadog logs.",
            internal_log_rate_limit = true,
            error = ?error
        );
        DatadogApiError::InvalidPayload(format!("Error parsing JSON: {:?}", error))
    })
}

async fn datadog_ingest_logs(
    ingest_router: IngestRouterServiceClient,
    index_id: IndexId,
    body: Body,
) -> Result<(), DatadogApiError> {
    if body.content.is_empty() || body.content.as_ref() == b"{}" {
        // The datadog agent may send an empty payload as a keep alive
        // https://github.com/DataDog/datadog-agent/blob/5a6c5dd75a2233fbf954e38ddcc1484df4c21a35/pkg/logs/client/http/destination.go#L52
        debug!(
            message = "Empty payload ignored.",
            internal_log_rate_limit = true
        );
        return Ok(());
    }

    let handle = quickwit_common::thread_pool::run_cpu_intensive(move || {
        // TODO: We could just validate + get the byte bounds of each object instead of the more
        // expensive serde_json rountrip.
        // e.g. Vec<RawValue> + validation
        let messages: Vec<DatadogLogMsg> = deserialize_datadog_log(&body.content)?;

        let mut doc_batch_builder = DocBatchV2Builder::default();
        let mut doc_uid_generator = DocUidGenerator::default();

        for doc in messages {
            doc_batch_builder.add_doc(
                doc_uid_generator.next_doc_uid(),
                serde_json::to_string(&doc).unwrap().as_bytes(),
            );
        }
        Ok(doc_batch_builder.build())
    });
    let doc_batch = handle
        .await
        .map_err(|err| DatadogApiError::Panicked(err.to_string()))??;

    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id,
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch,
    };
    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests: vec![subrequest],
    };
    let response = ingest_router
        .ingest(request)
        .await
        .map_err(|err| DatadogApiError::Ingest(err.to_string()))?;
    for failure in response.failures.iter() {
        error!(
            message = "Failed to ingest logs.",
            internal_log_rate_limit = true,
            error = ?failure
        );
    }
    if !response.failures.is_empty() {
        return Err(DatadogApiError::Ingest(format!(
            "Failed to ingest logs {:?}.",
            response.failures
        )));
    }
    Ok(())
}
