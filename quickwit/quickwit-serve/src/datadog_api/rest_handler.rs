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

use pomchi::{DatadogLogMsg, MessageValue};
use quickwit_common::dd_metrics::DD_INGEST_METRICS;
use quickwit_common::{rate_limited_error, rate_limited_warn};
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_ingest::DocBatchV2Builder;
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::ingest::router::{
    IngestFailureReason, IngestRequestV2, IngestRouterService, IngestRouterServiceClient,
    IngestSubrequest,
};
use quickwit_proto::types::DocUidGenerator;
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::Deserialize;
use serde_with::formats::CommaSeparator;
use serde_with::{StringWithSeparator, serde_as};
use tracing::debug;
use warp::{Filter, Rejection};

use quickwit_opentelemetry::otlp::{
    ArrowDocBatchV2Builder, ArrowMetricsBatchBuilder, MetricDataPoint, MetricType,
};
use time::OffsetDateTime;
use time::format_description::well_known::Iso8601;

use super::index_router::IndexRouter;
use super::log_msg_accessors::{custom_field_accessor, tag_accessor};
use crate::decompression::get_body_bytes;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

const DATADOG_METRICS_INDEX_ID: &str = "datadog-metrics";

#[derive(utoipa::OpenApi)]
#[openapi(paths(datadog_logs,))]
pub struct DatadogApi;

pub(crate) fn datadog_api_handlers(
    ingest_router: IngestRouterServiceClient,
    index_router: IndexRouter,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_healthcheck()
        .or(datadog_logs(ingest_router.clone(), index_router))
        .or(byoc_metrics(ingest_router))
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

fn map_ingest_failure(reason: IngestFailureReason) -> (ServiceErrorCode, &'static str) {
    match reason {
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
        IngestFailureReason::LoadShedding => (ServiceErrorCode::Internal, "load shedding"),
        IngestFailureReason::CircuitBreaker => (ServiceErrorCode::Internal, "circuit breaker"),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatadogApiError {
    #[error("bad request: {0}")]
    BadRequest(String),
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
            Self::BadRequest(_) => ServiceErrorCode::BadRequest,
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

    let (error_code, error_message) = map_ingest_failure(failure_reason);
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

// BYOC metrics endpoint: POST /api/v1/datadog-metrics/ingest
fn byoc_metrics_filter() -> impl Filter<Extract = (Body,), Error = Rejection> + Clone {
    warp::post()
        .and(warp::path("api"))
        .and(warp::path("v1"))
        .and(warp::path(DATADOG_METRICS_INDEX_ID))
        .and(warp::path("ingest"))
        .and(warp::path::end())
        .and(get_body_bytes())
}

pub(crate) fn byoc_metrics(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    byoc_metrics_filter()
        .and(with_arg(ingest_router))
        .then(
            |body: Body, ingest_router: IngestRouterServiceClient| async move {
                byoc_ingest_metrics(ingest_router, body).await
            },
        )
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}

async fn byoc_ingest_metrics(
    ingest_router: IngestRouterServiceClient,
    body: Body,
) -> Result<(), DatadogApiError> {
    let start = Instant::now();

    if body.content.is_empty() {
        return Ok(());
    }

    // The managed metrics index in BYOC will receive payloads from vector. There will be only one
    // ingest subrequest, since we're only routing to the datadog-metrics index.
    let subrequest = quickwit_common::thread_pool::run_cpu_intensive(move || {
        let data_points = try_parse_vector_metrics(&body)?;
        if data_points.is_empty() {
            return Ok(None);
        }

        let mut arrow_builder = ArrowMetricsBatchBuilder::with_capacity(data_points.len());
        let mut doc_uid_generator = DocUidGenerator::default();
        let mut doc_uids = Vec::with_capacity(data_points.len());

        for dp in &data_points {
            arrow_builder.append(dp);
            doc_uids.push(doc_uid_generator.next_doc_uid());
        }

        let record_batch = arrow_builder.finish();
        let doc_batch = ArrowDocBatchV2Builder::from_record_batch(&record_batch, doc_uids)
            .map_err(|e| DatadogApiError::Internal(format!("failed to serialize Arrow IPC: {e}")))?
            .build();

        Ok(Some(IngestSubrequest {
            subrequest_id: 0,
            index_id: DATADOG_METRICS_INDEX_ID.to_string(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            doc_batch: Some(doc_batch),
        }))
    })
    .await
    .map_err(|_| {
        DatadogApiError::Internal("task panicked while parsing metrics payload".to_string())
    })??;

    let Some(subrequest) = subrequest else {
        return Ok(());
    };

    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests: vec![subrequest],
    };
    let response = ingest_router
        .ingest(request)
        .await
        .map_err(|e| DatadogApiError::Ingest(e.error_code(), e.to_string()))?;

    // There's only one subrequest, so if failures is 0, this means the ingest request succeeded.
    if response.failures.is_empty() {
        DD_INGEST_METRICS
            .metrics_ingest_requests_total
            .get("200")
            .increment(1);
        DD_INGEST_METRICS
            .metrics_ingest_request_duration_seconds
            .get("200")
            .record(start.elapsed().as_secs_f64());
        return Ok(());
    }

    let (error_code, error_message) = map_ingest_failure(response.failures[0].reason());
    let status_code = error_code.http_status_code();
    DD_INGEST_METRICS
        .metrics_ingest_requests_total
        .get(status_code.as_str())
        .increment(1);
    DD_INGEST_METRICS
        .metrics_ingest_request_duration_seconds
        .get(status_code.as_str())
        .record(start.elapsed().as_secs_f64());
    Err(DatadogApiError::Ingest(
        error_code,
        error_message.to_string(),
    ))
}

#[derive(Debug, Deserialize)]
struct VectorNumericValue {
    value: f64,
}

/// A single metric as emitted by Vector's native metric format.
#[derive(Debug, Deserialize)]
struct VectorMetricMsg {
    name: String,
    #[serde(default)]
    tags: HashMap<String, String>,
    timestamp: Option<String>,
    #[serde(default)]
    counter: Option<VectorNumericValue>,
    #[serde(default)]
    gauge: Option<VectorNumericValue>,
}

fn parse_iso8601_to_secs(ts: &str) -> Option<u64> {
    let dt = OffsetDateTime::parse(ts, &Iso8601::DEFAULT).ok()?;
    u64::try_from(dt.unix_timestamp()).ok()
}

fn vector_msg_to_data_point(msg: VectorMetricMsg) -> Result<MetricDataPoint, DatadogApiError> {
    if msg.name.is_empty() {
        return Err(DatadogApiError::BadRequest(
            "metric has empty name".to_string(),
        ));
    }

    let (metric_type, value) = if let Some(counter) = msg.counter {
        (MetricType::Sum, counter.value)
    } else if let Some(gauge) = msg.gauge {
        (MetricType::Gauge, gauge.value)
    } else {
        return Err(DatadogApiError::BadRequest(format!(
            "metric '{}' has no counter or gauge value",
            msg.name
        )));
    };

    let timestamp_secs = match &msg.timestamp {
        Some(ts) => parse_iso8601_to_secs(ts).ok_or_else(|| {
            DatadogApiError::BadRequest(format!("failed to parse timestamp '{ts}'"))
        })?,
        None => {
            return Err(DatadogApiError::BadRequest(format!(
                "metric '{}' is missing timestamp",
                msg.name
            )));
        }
    };

    Ok(MetricDataPoint {
        metric_name: msg.name,
        metric_type,
        timestamp_secs,
        value,
        tags: msg.tags,
    })
}

/// Parse a Vector JSON array payload into metric data points.
///
/// Vector sends a JSON array of flat metric objects: `[{"name": ..., "counter": {...}}, ...]`.
fn try_parse_vector_metrics(body: &Body) -> Result<Vec<MetricDataPoint>, DatadogApiError> {
    let messages: Vec<VectorMetricMsg> =
        serde_json::from_slice(&body.content).map_err(DatadogApiError::InvalidPayload)?;

    let mut data_points = Vec::with_capacity(messages.len());
    for msg in messages {
        data_points.push(vector_msg_to_data_point(msg)?);
    }

    Ok(data_points)
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::IngestV2Error;
    use quickwit_proto::ingest::router::{
        IngestFailure, IngestResponseV2, IngestRouterServiceClient, IngestSuccess,
        MockIngestRouterService,
    };
    use quickwit_proto::metastore::IndexRoutingRule;
    use quickwit_proto::types::{IndexUid, Position, ShardId};
    use serde_json::json;

    use super::*;

    const DATADOG_INDEX_ID: &str = "datadog";

    fn test_index_router() -> IndexRouter {
        IndexRouter::for_test(vec![IndexRoutingRule {
            index_id: DATADOG_INDEX_ID.to_string(),
            filter: "*".to_string(),
        }])
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
        let index_router = IndexRouter::for_test(vec![
            IndexRoutingRule {
                filter: "service:frontend".to_string(),
                index_id: "frontend-index".to_string(),
            },
            IndexRoutingRule {
                filter: "service:backend".to_string(),
                index_id: "backend-index".to_string(),
            },
            IndexRoutingRule {
                filter: "*".to_string(),
                index_id: "catch-all-index".to_string(),
            },
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

    #[tokio::test]
    async fn test_byoc_ingest_metrics() {
        let mut mock = MockIngestRouterService::new();
        mock.expect_ingest()
            .once()
            .returning(|ingest_request| {
                assert_eq!(ingest_request.subrequests.len(), 1);
                assert_eq!(ingest_request.subrequests[0].index_id, "datadog-metrics");
                assert_eq!(ingest_request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
                assert!(ingest_request.subrequests[0].doc_batch.is_some());
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        num_ingested_docs: 2,
                        ..Default::default()
                    }],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock);
        let handler = byoc_metrics(ingest_router);

        let body = json!([
            {"name": "cpu.usage", "tags": {"env": "prod"}, "timestamp": "2026-03-11T14:19:55Z", "gauge": {"value": 85.5}},
            {"name": "http.requests", "tags": {"service": "api"}, "timestamp": "2026-03-11T14:19:55Z", "counter": {"value": 42}}
        ]);

        let resp = warp::test::request()
            .path("/api/v1/datadog-metrics/ingest")
            .method("POST")
            .json(&body)
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_vector_ingest_metrics_timeout() {
        let mut mock = MockIngestRouterService::new();
        mock.expect_ingest().once().returning(|ingest_request| {
            assert_eq!(ingest_request.subrequests.len(), 1);
            assert_eq!(ingest_request.subrequests[0].index_id, "datadog-metrics");
            assert_eq!(ingest_request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
            assert!(ingest_request.subrequests[0].doc_batch.is_some());
            Err(IngestV2Error::Timeout("timed out".to_string()))
        });
        let ingest_router = IngestRouterServiceClient::from_mock(mock);
        let handler = byoc_metrics(ingest_router);

        let body = json!([
            {"name": "cpu.usage", "tags": {"env": "prod"}, "timestamp": "2026-03-11T14:19:55Z", "gauge": {"value": 1.0}}
        ]);

        let resp = warp::test::request()
            .path("/api/v1/datadog-metrics/ingest")
            .method("POST")
            .json(&body)
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), 408);
    }

    #[tokio::test]
    async fn test_vector_ingest_metrics_ingest_failure() {
        let mut mock = MockIngestRouterService::new();
        mock.expect_ingest().once().returning(|ingest_request| {
            assert_eq!(ingest_request.subrequests.len(), 1);
            assert_eq!(ingest_request.subrequests[0].index_id, "datadog-metrics");
            assert_eq!(ingest_request.subrequests[0].source_id, INGEST_V2_SOURCE_ID);
            assert!(ingest_request.subrequests[0].doc_batch.is_some());
            Ok(IngestResponseV2 {
                successes: Vec::new(),
                failures: vec![IngestFailure {
                    subrequest_id: 0,
                    index_id: "datadog-metrics".to_string(),
                    source_id: INGEST_V2_SOURCE_ID.to_string(),
                    reason: IngestFailureReason::ShardRateLimited as i32,
                }],
            })
        });
        let ingest_router = IngestRouterServiceClient::from_mock(mock);
        let handler = byoc_metrics(ingest_router);

        let body = json!([
            {"name": "cpu.usage", "tags": {"env": "prod"}, "timestamp": "2026-03-11T14:19:55Z", "gauge": {"value": 1.0}}
        ]);

        let resp = warp::test::request()
            .path("/api/v1/datadog-metrics/ingest")
            .method("POST")
            .json(&body)
            .reply(&handler)
            .await;

        assert_eq!(resp.status(), 429);
    }

    #[test]
    fn test_counter_maps_to_sum() {
        let msg = VectorMetricMsg {
            name: "http.requests".to_string(),
            tags: HashMap::new(),
            timestamp: Some("2026-03-11T14:19:55Z".to_string()),
            counter: Some(VectorNumericValue { value: 42.0 }),
            gauge: None,
        };
        let dp = vector_msg_to_data_point(msg).unwrap();
        assert_eq!(dp.metric_type, MetricType::Sum);
        assert_eq!(dp.value, 42.0);
    }

    #[test]
    fn test_gauge_maps_to_gauge() {
        let msg = VectorMetricMsg {
            name: "cpu.usage".to_string(),
            tags: HashMap::new(),
            timestamp: Some("2026-03-11T14:19:55Z".to_string()),
            counter: None,
            gauge: Some(VectorNumericValue { value: 85.5 }),
        };
        let dp = vector_msg_to_data_point(msg).unwrap();
        assert_eq!(dp.metric_type, MetricType::Gauge);
        assert_eq!(dp.value, 85.5);
    }

    #[test]
    fn test_empty_name_returns_error() {
        let msg = VectorMetricMsg {
            name: "".to_string(),
            tags: HashMap::new(),
            timestamp: None,
            counter: Some(VectorNumericValue { value: 1.0 }),
            gauge: None,
        };
        assert!(vector_msg_to_data_point(msg).is_err());
    }

    #[test]
    fn test_no_counter_or_gauge_returns_error() {
        let msg = VectorMetricMsg {
            name: "m".to_string(),
            tags: HashMap::new(),
            timestamp: None,
            counter: None,
            gauge: None,
        };
        assert!(vector_msg_to_data_point(msg).is_err());
    }

    #[test]
    fn test_tags_pass_through() {
        let tags = HashMap::from([
            ("service".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
            ("host".to_string(), "srv-1".to_string()),
            ("custom".to_string(), "value".to_string()),
        ]);
        let msg = VectorMetricMsg {
            name: "m".to_string(),
            tags: tags.clone(),
            timestamp: Some("2026-03-11T14:19:55Z".to_string()),
            counter: Some(VectorNumericValue { value: 1.0 }),
            gauge: None,
        };
        let dp = vector_msg_to_data_point(msg).unwrap();
        assert_eq!(dp.tags, tags);
    }

    #[test]
    fn test_iso8601_timestamp_parsing() {
        let msg = VectorMetricMsg {
            name: "m".to_string(),
            tags: HashMap::new(),
            timestamp: Some("2026-03-11T14:19:55Z".to_string()),
            counter: Some(VectorNumericValue { value: 1.0 }),
            gauge: None,
        };
        let dp = vector_msg_to_data_point(msg).unwrap();
        assert_eq!(dp.timestamp_secs, 1773505195);
    }

    #[test]
    fn test_missing_timestamp_returns_error() {
        let msg = VectorMetricMsg {
            name: "m".to_string(),
            tags: HashMap::new(),
            timestamp: None,
            counter: Some(VectorNumericValue { value: 1.0 }),
            gauge: None,
        };
        assert!(vector_msg_to_data_point(msg).is_err());
    }
}
