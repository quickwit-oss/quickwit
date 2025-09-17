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

use std::sync::LazyLock;

use quickwit_common::dd_metrics::{DD_STATUS_CODES, DDCounters};
use quickwit_common::rate_limited_error;
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_doc_transforms::DatadogLogMsg;
use quickwit_ingest::DocBatchV2Builder;
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::ingest::router::{
    IngestFailureReason, IngestRequestV2, IngestRouterService, IngestRouterServiceClient,
    IngestSubrequest,
};
use quickwit_proto::types::{DocUidGenerator, IndexId};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use tracing::{debug, error};
use warp::{Filter, Rejection};

use crate::decompression::get_body_bytes;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

const DATADOG_INDEX_ID: &str = "datadog";

pub static DD_INGEST_METRICS: LazyLock<DDCounters> =
    LazyLock::new(|| DDCounters::new("ingest_requests.count", "status_code", DD_STATUS_CODES));

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

#[derive(Debug, thiserror::Error)]
pub enum DatadogApiError {
    #[error("failed to ingest payload: {1}")]
    Ingest(ServiceErrorCode, String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("failed to parse payload: {0}")]
    InvalidPayload(serde_json::Error),
}

impl ServiceError for DatadogApiError {
    fn error_code(&self) -> ServiceErrorCode {
        rate_limited_error!(limit_per_min = 6, error = %self);

        match self {
            Self::InvalidPayload(_) => ServiceErrorCode::BadRequest,
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::Ingest(error_code, _) => *error_code,
        }
    }
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
            message = "received empty payload, ignoring",
            internal_log_rate_limit = true
        );
        return Ok(());
    }
    let doc_batch_fut = quickwit_common::thread_pool::run_cpu_intensive(move || {
        // TODO: We could just validate + get the byte bounds of each object instead of the more
        // expensive serde_json rountrip.
        // e.g. Vec<RawValue> + validation
        let messages: Vec<DatadogLogMsg> =
            serde_json::from_slice(&body.content).map_err(DatadogApiError::InvalidPayload)?;

        let mut doc_batch_builder = DocBatchV2Builder::default();
        let mut doc_uid_generator = DocUidGenerator::default();

        for message in messages {
            let doc_json =
                serde_json::to_vec(&message).expect("JSON serialization should not fail");

            doc_batch_builder.add_doc(doc_uid_generator.next_doc_uid(), &doc_json);
        }
        Ok(doc_batch_builder.build())
    });
    let doc_batch = doc_batch_fut.await.map_err(|_panicked| {
        DatadogApiError::Internal("task panicked while processing log events payload".to_string())
    })??;

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
        .map_err(|error| DatadogApiError::Ingest(error.error_code(), error.to_string()))?;

    // Since we issued only one subrequest, there should be only one success or failure in the
    // response.
    let num_successes = response.successes.len();
    let num_failures = response.failures.len();
    assert!(
        num_successes + num_failures == 1,
        "expected only one success or failure, got {num_successes} successes and {num_failures} \
         failures",
    );

    if num_failures == 0 {
        DD_INGEST_METRICS.get("200").increment(1);
        return Ok(());
    }
    let failure_reason = response.failures[0].reason();

    // Same mapping as Elastic bulk v2:
    let (error_code, error_message) = match failure_reason {
        IngestFailureReason::Unspecified => (ServiceErrorCode::Internal, "unknown error"),
        IngestFailureReason::IndexNotFound => (ServiceErrorCode::NotFound, "index not found"),
        IngestFailureReason::SourceNotFound => (ServiceErrorCode::NotFound, "source not found"),
        IngestFailureReason::Internal => (ServiceErrorCode::Internal, "internal error"),
        IngestFailureReason::NoShardsAvailable => (
            ServiceErrorCode::TooManyRequests,
            "too many requests (no shards available)",
        ),
        IngestFailureReason::ShardRateLimited => (
            ServiceErrorCode::TooManyRequests,
            "too many requests (rate limiting)",
        ),
        IngestFailureReason::WalFull => (ServiceErrorCode::Internal, "WAL full"),
        IngestFailureReason::Timeout => (ServiceErrorCode::Timeout, "request timed out"),
        IngestFailureReason::RouterLoadShedding => {
            (ServiceErrorCode::Internal, "router load shedding")
        }
        IngestFailureReason::LoadShedding => (ServiceErrorCode::Internal, "load shedding)"),
        IngestFailureReason::CircuitBreaker => (ServiceErrorCode::Internal, "circuit breaker)"),
    };
    let status_code = error_code.http_status_code();
    DD_INGEST_METRICS.get(status_code.as_str()).increment(1);

    Err(DatadogApiError::Ingest(
        error_code,
        error_message.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::IngestV2Error;
    use quickwit_proto::ingest::router::{
        IngestFailure, IngestResponseV2, IngestRouterServiceClient, IngestSuccess,
        MockIngestRouterService,
    };
    use quickwit_proto::types::{IndexUid, Position, ShardId};

    use super::*;

    #[tokio::test]
    async fn test_datadog_ingest_logs() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 1);
                assert_eq!(ingest_request.subrequests[0].index_id, DATADOG_INDEX_ID);
                assert_eq!(ingest_request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(
                    ingest_request.subrequests[0]
                        .doc_batch
                        .as_ref()
                        .unwrap()
                        .num_docs(),
                    1
                );

                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        subrequest_id: 0,
                        index_uid: Some(IndexUid::for_test(DATADOG_INDEX_ID, 0)),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        shard_id: Some(ShardId::from(1)),
                        replication_position_inclusive: Some(Position::offset(0u64)),
                        num_ingested_docs: 1,
                        parse_failures: Vec::new(),
                    }],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = datadog_logs(ingest_router);
        let payload = r#"
            [
              {
                "message": "Hello, world!"
              }
            ]
        "#;
        let response = warp::test::request()
            .path("/api/v2/logs")
            .method("POST")
            .header("content-type", "application/json")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_datadog_ingest_logs_empty_payload() {
        let ingest_router = IngestRouterServiceClient::mocked();
        let handler = datadog_logs(ingest_router);

        let response = warp::test::request()
            .path("/api/v2/logs")
            .method("POST")
            .header("content-type", "application/json")
            .body("")
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200);

        let response = warp::test::request()
            .path("/api/v2/logs")
            .method("POST")
            .header("content-type", "application/json")
            .body("{}")
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_datadog_ingest_logs_invalid_payload() {
        let ingest_router = IngestRouterServiceClient::mocked();
        let handler = datadog_logs(ingest_router);

        let response = warp::test::request()
            .path("/api/v2/logs")
            .method("POST")
            .header("content-type", "application/json")
            .body("invalid payload")
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 400);
    }

    #[tokio::test]
    async fn test_datadog_ingest_logs_ingest_error() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 1);
                assert_eq!(ingest_request.subrequests[0].index_id, DATADOG_INDEX_ID);
                assert_eq!(ingest_request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(
                    ingest_request.subrequests[0]
                        .doc_batch
                        .as_ref()
                        .unwrap()
                        .num_docs(),
                    1
                );

                Err(IngestV2Error::Timeout(
                    "request timed out after 10 seconds".to_string(),
                ))
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = datadog_logs(ingest_router);
        let payload = r#"
            [
              {
                "message": "Hello, world!"
              }
            ]
        "#;
        let response = warp::test::request()
            .path("/api/v2/logs")
            .method("POST")
            .header("content-type", "application/json")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 408);
    }

    #[tokio::test]
    async fn test_datadog_ingest_logs_ingest_failure() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 1);
                assert_eq!(ingest_request.subrequests[0].index_id, DATADOG_INDEX_ID);
                assert_eq!(ingest_request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert_eq!(
                    ingest_request.subrequests[0]
                        .doc_batch
                        .as_ref()
                        .unwrap()
                        .num_docs(),
                    1
                );

                Ok(IngestResponseV2 {
                    successes: Vec::new(),
                    failures: vec![IngestFailure {
                        subrequest_id: 0,
                        index_id: DATADOG_INDEX_ID.to_string(),
                        source_id: INGEST_V2_SOURCE_ID.to_string(),
                        reason: IngestFailureReason::ShardRateLimited as i32,
                    }],
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = datadog_logs(ingest_router);
        let payload = r#"
            [
              {
                "message": "Hello, world!"
              }
            ]
        "#;
        let response = warp::test::request()
            .path("/api/v2/logs")
            .method("POST")
            .header("content-type", "application/json")
            .body(payload)
            .reply(&handler)
            .await;
        assert_eq!(response.status(), 429);
    }
}
