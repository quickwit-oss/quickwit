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

use std::collections::HashMap;
use std::time::Instant;

use quickwit_processing::{DatadogLogMsg, MessageValue};
use quickwit_common::dd_metrics::DD_INGEST_METRICS;
use quickwit_common::{rate_limited_error, rate_limited_warn};
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_ingest::DocBatchV2Builder;
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestRouterService, IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::types::DocUidGenerator;
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::Deserialize;
use serde_with::formats::CommaSeparator;
use serde_with::{StringWithSeparator, serde_as};
use tracing::debug;
use warp::{Filter, Rejection};

use super::index_router::IndexRouter;
use super::log_msg_accessors::{custom_field_accessor, tag_accessor};
use crate::datadog_api::get_error_code_and_message;
use crate::decompression::get_body_bytes;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

#[derive(utoipa::OpenApi)]
#[openapi(paths(datadog_logs,))]
pub struct DatadogApi;

pub(crate) fn datadog_api_handlers(
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_healthcheck()
        .or(datadog_logs(ingest_router, index_router))
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

#[serde_as]
#[derive(Debug, Clone, Default, Deserialize)]
/// Option to override fields in Datadog log messages via URL parameters.
pub struct DatadogLogsQueryParams {
    service: Option<String>,
    #[serde(alias = "host")]
    hostname: Option<String>,
    ddsource: Option<String>,
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    ddtags: Option<Vec<String>>,
}

/// Based on vector agent logs endpoint:
/// https://github.com/vectordotdev/vector/blob/450de36904f3d1524057e8cdb736941194da8d22/src/sources/datadog_agent/mod.rs#L499
pub(crate) fn datadog_logs_filter()
-> impl Filter<Extract = (Body, DatadogLogsQueryParams), Error = Rejection> + Clone {
    let path_filter = warp::path!("api" / "v1" / "input")
        .or(warp::path!("api" / "v2" / "logs"))
        .unify();
    path_filter
        .and(warp::post())
        .and(get_body_bytes())
        .and(warp::query::<DatadogLogsQueryParams>())
}

#[utoipa::path(
    post,
    tag = "Datadog Logs",
    path = "/api/v2/logs",
    request_body(content = String, description = "Datadog Log JSON message or a String"),
    responses(
        (status = 200, description = "Successfully exported logs.", body = bool),
    ),
    params(
        ("service" = String, Query, description = "Override service for all messages"),
        ("hostname" = String, Query, description = "Override hostname for all messages"),
        ("ddsource" = String, Query, description = "Override ddsource for all messages"),
        ("ddtags" = String, Query, description = "Override ddtags as comma-separated list"),
    )
)]
pub(crate) fn datadog_logs(
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_logs_filter()
        .and(with_arg(ingest_router))
        .and(with_arg(index_router))
        .and(warp::post())
        .then(
            |body: Body, query: DatadogLogsQueryParams, ingest_router, index_router| async move {
                datadog_ingest_logs(ingest_router, index_router, body, query).await
            },
        )
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

fn try_parse_datadog_log_messages(body: &Body) -> Result<Vec<DatadogLogMsg>, DatadogApiError> {
    // Try to parse it as vec of DatadogLogMsg
    if let Ok(messages) = serde_json::from_slice::<Vec<DatadogLogMsg>>(&body.content) {
        return Ok(messages);
    }

    // Try to parse it as a Vec of JSON objects
    if let Ok(messages_json) =
        serde_json::from_slice::<Vec<serde_json::Map<String, serde_json::Value>>>(&body.content)
    {
        let mut messages: Vec<DatadogLogMsg> = Vec::with_capacity(messages_json.len());
        for message_json in messages_json {
            let message: DatadogLogMsg = DatadogLogMsg {
                message: MessageValue::Obj(message_json),
                status: None,
                timestamp: None,
                hostname: None,
                service: None,
                ddsource: None,
                ddtags: Vec::new(),
            };
            messages.push(message);
        }
        return Ok(messages);
    }

    // try to parse it as a single DatadogLogMsg
    if let Ok(message) = serde_json::from_slice::<DatadogLogMsg>(&body.content) {
        return Ok(vec![message]);
    }

    // try to parse it as a single JSON object (map)
    if let Ok(message_json) =
        serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&body.content)
    {
        let message: DatadogLogMsg = DatadogLogMsg {
            message: MessageValue::Obj(message_json),
            status: None,
            timestamp: None,
            hostname: None,
            service: None,
            ddsource: None,
            ddtags: Vec::new(),
        };
        return Ok(vec![message]);
    }

    // Fallback: If JSON parsing fails, treat as plain text
    let text = String::from_utf8(body.content.to_vec()).map_err(|utf8_err| {
        DatadogApiError::InvalidPayload(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("payload is not valid UTF-8: {}", utf8_err),
        )))
    })?;
    Ok(vec![DatadogLogMsg {
        message: text.into(),
        status: None,
        timestamp: None,
        hostname: None,
        service: None,
        ddsource: None,
        ddtags: Vec::new(),
    }])
}

async fn datadog_ingest_logs(
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
    body: Body,
    query: DatadogLogsQueryParams,
) -> Result<(), DatadogApiError> {
    let start = Instant::now();
    if body.content.is_empty() || body.content.as_ref() == b"{}" {
        // The datadog agent may send an empty payload as a keep alive
        // https://github.com/DataDog/datadog-agent/blob/5a6c5dd75a2233fbf954e38ddcc1484df4c21a35/pkg/logs/client/http/destination.go#L52
        debug!(
            message = "received empty payload, ignoring",
            internal_log_rate_limit = true
        );
        return Ok(());
    }
    // Acquire the router guard once for the entire batch to ensure consistency
    // and avoid cloning index_ids for each document.
    let router = index_router.get_router();

    let subrequests_fut = quickwit_common::thread_pool::run_cpu_intensive(move || {
        let mut messages = try_parse_datadog_log_messages(&body)?;
        // Apply URL parameter overrides to each message, if present.
        if query.service.is_some()
            || query.hostname.is_some()
            || query.ddsource.is_some()
            || query.ddtags.is_some()
        {
            for message in &mut messages {
                if let Some(service) = &query.service {
                    message.service = Some(service.clone());
                }
                if let Some(hostname) = &query.hostname {
                    message.hostname = Some(hostname.clone());
                }
                if let Some(ddsource) = &query.ddsource {
                    message.ddsource = Some(ddsource.clone());
                }
                if let Some(ddtags) = &query.ddtags {
                    message.ddtags = ddtags.clone();
                }
            }
        }

        // Group documents by target index using per-document routing.
        let mut batches_by_index: HashMap<&str, DocBatchV2Builder> = HashMap::new();
        let mut doc_uid_generator = DocUidGenerator::default();
        let mut num_unrouted_docs = 0u64;

        for message in &messages {
            let Some(index_id) =
                router.resolve_index(&tag_accessor(message), &custom_field_accessor(message))
            else {
                num_unrouted_docs += 1;
                continue;
            };

            let doc_json =
                serde_json::to_vec(&message).expect("JSON serialization should not fail");

            batches_by_index
                .entry(index_id)
                .or_default()
                .add_doc(doc_uid_generator.next_doc_uid(), &doc_json);
        }

        if num_unrouted_docs > 0 {
            DD_INGEST_METRICS
                .ingest_unrouted_docs_total
                .increment(num_unrouted_docs);
            rate_limited_warn!(
                limit_per_min = 10,
                num_unrouted_docs = num_unrouted_docs,
                "dropped logs with no matching routing rule"
            );
        }

        // Build subrequests for each index.
        let subrequests: Vec<IngestSubrequest> = batches_by_index
            .into_iter()
            .enumerate()
            .map(|(i, (index_id, builder))| IngestSubrequest {
                subrequest_id: i as u32,
                index_id: index_id.to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                doc_batch: builder.build(),
            })
            .collect();

        Ok(subrequests)
    });
    let subrequests: Vec<IngestSubrequest> = subrequests_fut.await.map_err(|_panicked| {
        DatadogApiError::Internal("task panicked while processing log events payload".to_string())
    })??;

    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests,
    };
    let num_subrequests = request.subrequests.len();
    let response = ingest_router
        .ingest(request)
        .await
        .map_err(|error| DatadogApiError::Ingest(error.error_code(), error.to_string()))?;

    // Each subrequest should have exactly one success or failure in the response.
    let num_successes = response.successes.len();
    let num_failures = response.failures.len();
    assert!(
        num_successes + num_failures == num_subrequests,
        "expected {num_subrequests} responses, got {num_successes} successes and {num_failures} \
         failures",
    );

    if num_failures == 0 {
        DD_INGEST_METRICS
            .ingest_requests_total
            .get("200")
            .increment(1);
        DD_INGEST_METRICS
            .ingest_request_duration_seconds
            .get("200")
            .record(start.elapsed().as_secs_f64());
        return Ok(());
    }
    // Return the first failure reason (could be improved to aggregate errors).
    let failure_reason = response.failures[0].reason();
    let (error_code, error_message) = get_error_code_and_message(failure_reason);

    let status_code = error_code.http_status_code();
    DD_INGEST_METRICS
        .ingest_requests_total
        .get(status_code.as_str())
        .increment(1);
    DD_INGEST_METRICS
        .ingest_request_duration_seconds
        .get(status_code.as_str())
        .record(start.elapsed().as_secs_f64());

    Err(DatadogApiError::Ingest(
        error_code,
        error_message.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::IngestV2Error;
    use quickwit_proto::ingest::router::{
        IngestFailure, IngestFailureReason, IngestResponseV2, IngestRouterServiceClient,
        IngestSuccess, MockIngestRouterService,
    };
    use quickwit_proto::types::{IndexUid, Position, ShardId};

    use super::*;

    const DATADOG_INDEX_ID: &str = "datadog";

    fn test_index_router() -> IndexRouter {
        IndexRouter::for_test(&[("*", DATADOG_INDEX_ID)])
    }

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
        let handler = datadog_logs(ingest_router, test_index_router());
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
        let handler = datadog_logs(ingest_router, test_index_router());

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
        let handler = datadog_logs(ingest_router, test_index_router());
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
        let handler = datadog_logs(ingest_router, test_index_router());
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

    #[tokio::test]
    async fn test_datadog_ingest_logs_routes_to_multiple_indexes() {
        let index_router = IndexRouter::for_test(&[
            ("service:frontend", "frontend-index"),
            ("service:backend", "backend-index"),
            ("*", "catch-all-index"),
        ]);

        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .once()
            .returning(|ingest_request| {
                // Should have 3 subrequests, one per index
                assert_eq!(ingest_request.subrequests.len(), 3);

                // Collect index_ids and doc counts
                let index_doc_counts: HashMap<&str, usize> = ingest_request
                    .subrequests
                    .iter()
                    .map(|sr| {
                        (
                            sr.index_id.as_str(),
                            sr.doc_batch.as_ref().unwrap().num_docs(),
                        )
                    })
                    .collect();

                // Verify routing: 2 frontend, 1 backend, 1 catch-all
                assert_eq!(index_doc_counts.get("frontend-index"), Some(&2));
                assert_eq!(index_doc_counts.get("backend-index"), Some(&1));
                assert_eq!(index_doc_counts.get("catch-all-index"), Some(&1));

                // Return success for all subrequests
                let successes = ingest_request
                    .subrequests
                    .iter()
                    .map(|sr| IngestSuccess {
                        subrequest_id: sr.subrequest_id,
                        index_uid: Some(IndexUid::for_test(&sr.index_id, 0)),
                        source_id: sr.source_id.clone(),
                        shard_id: Some(ShardId::from(1)),
                        replication_position_inclusive: Some(Position::offset(0u64)),
                        num_ingested_docs: sr.doc_batch.as_ref().unwrap().num_docs() as u32,
                        parse_failures: Vec::new(),
                    })
                    .collect();

                Ok(IngestResponseV2 {
                    successes,
                    failures: Vec::new(),
                })
            });

        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let handler = datadog_logs(ingest_router, index_router);

        // 4 logs: 2 frontend, 1 backend, 1 with no service (catch-all)
        let payload = r#"
            [
              {"message": "frontend log 1", "service": "frontend"},
              {"message": "backend log", "service": "backend"},
              {"message": "frontend log 2", "service": "frontend"},
              {"message": "no service log"}
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
}
