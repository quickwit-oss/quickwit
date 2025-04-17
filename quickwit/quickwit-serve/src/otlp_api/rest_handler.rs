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
use quickwit_opentelemetry::otlp::{OtelSignal, OtlpGrpcLogsService, OtlpGrpcTracesService};
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsService;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceService;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use quickwit_proto::types::IndexId;
use quickwit_proto::{ServiceError, ServiceErrorCode, tonic};
use serde::{self, Serialize};
use tracing::error;
use warp::{Filter, Rejection};

use crate::decompression::get_body_bytes;
use crate::rest::recover_fn;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, require, with_arg};

#[derive(utoipa::OpenApi)]
#[openapi(paths(
    otlp_default_logs_handler,
    otlp_logs_handler,
    otlp_default_traces_handler,
    otlp_ingest_traces_handler
))]
pub struct OtlpApi;

/// Setup OpenTelemetry API handlers.
pub(crate) fn otlp_ingest_api_handlers(
    otlp_logs_service: Option<OtlpGrpcLogsService>,
    otlp_traces_service: Option<OtlpGrpcTracesService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    otlp_default_logs_handler(otlp_logs_service.clone())
        .or(otlp_default_traces_handler(otlp_traces_service.clone()).recover(recover_fn))
        .or(otlp_logs_handler(otlp_logs_service).recover(recover_fn))
        .or(otlp_ingest_traces_handler(otlp_traces_service).recover(recover_fn))
        .boxed()
}

/// Open Telemetry REST/Protobuf logs ingest endpoint.
#[utoipa::path(
    post,
    tag = "Open Telemetry",
    path = "/otlp/v1/logs",
    request_body(content = String, description = "`ExportLogsServiceRequest` protobuf message", content_type = "application/x-protobuf"),
    responses(
        (status = 200, description = "Successfully exported logs.", body = ExportLogsServiceResponse)
    ),
)]
pub(crate) fn otlp_default_logs_handler(
    otlp_logs_service: Option<OtlpGrpcLogsService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_logs_service)
        .and(warp::path!("otlp" / "v1" / "logs"))
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/x-protobuf",
        ))
        .and(warp::header::optional::<String>(
            OtelSignal::Logs.header_name(),
        ))
        .and(warp::post())
        .and(get_body_bytes())
        .then(
            |otlp_logs_service, index_id: Option<String>, body| async move {
                let index_id =
                    index_id.unwrap_or_else(|| OtelSignal::Logs.default_index_id().to_string());
                otlp_ingest_logs(otlp_logs_service, index_id, body).await
            },
        )
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}
/// Open Telemetry REST/Protobuf logs ingest endpoint.
#[utoipa::path(
    post,
    tag = "Open Telemetry",
    path = "/{index}/otlp/v1/logs",
    request_body(content = String, description = "`ExportLogsServiceRequest` protobuf message", content_type = "application/x-protobuf"),
    responses(
        (status = 200, description = "Successfully exported logs.", body = ExportLogsServiceResponse)
    ),
)]
pub(crate) fn otlp_logs_handler(
    otlp_log_service: Option<OtlpGrpcLogsService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_log_service)
        .and(warp::path!(String / "otlp" / "v1" / "logs"))
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/x-protobuf",
        ))
        .and(warp::post())
        .and(get_body_bytes())
        .then(otlp_ingest_logs)
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}

/// Open Telemetry REST/Protobuf traces ingest endpoint.
#[utoipa::path(
    post,
    tag = "Open Telemetry",
    path = "/otlp/v1/traces",
    request_body(content = String, description = "`ExportTraceServiceRequest` protobuf message", content_type = "application/x-protobuf"),
    responses(
        (status = 200, description = "Successfully exported traces.", body = ExportTracesServiceResponse)
    ),
)]
pub(crate) fn otlp_default_traces_handler(
    otlp_traces_service: Option<OtlpGrpcTracesService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_traces_service)
        .and(warp::path!("otlp" / "v1" / "traces"))
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/x-protobuf",
        ))
        .and(warp::header::optional::<String>(
            OtelSignal::Traces.header_name(),
        ))
        .and(warp::post())
        .and(get_body_bytes())
        .then(
            |otlp_traces_service, index_id: Option<String>, body| async move {
                let index_id =
                    index_id.unwrap_or_else(|| OtelSignal::Traces.default_index_id().to_string());
                otlp_ingest_traces(otlp_traces_service, index_id, body).await
            },
        )
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}
/// Open Telemetry REST/Protobuf traces ingest endpoint.
#[utoipa::path(
    post,
    tag = "Open Telemetry",
    path = "/{index}/otlp/v1/traces",
    request_body(content = String, description = "`ExportTraceServiceRequest` protobuf message", content_type = "application/x-protobuf"),
    responses(
        (status = 200, description = "Successfully exported traces.", body = ExportTracesServiceResponse)
    ),
)]
pub(crate) fn otlp_ingest_traces_handler(
    otlp_traces_service: Option<OtlpGrpcTracesService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_traces_service)
        .and(warp::path!(String / "otlp" / "v1" / "traces"))
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/x-protobuf",
        ))
        .and(warp::post())
        .and(get_body_bytes())
        .then(otlp_ingest_traces)
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}

#[derive(Debug, Clone, thiserror::Error, Serialize)]
pub enum OtlpApiError {
    #[error("invalid OTLP request: {0}")]
    InvalidPayload(String),
    #[error("error when ingesting payload: {0}")]
    Ingest(String),
}

impl ServiceError for OtlpApiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            OtlpApiError::InvalidPayload(_) => ServiceErrorCode::BadRequest,
            OtlpApiError::Ingest(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "otlp internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
        }
    }
}

async fn otlp_ingest_logs(
    otlp_logs_service: OtlpGrpcLogsService,
    index_id: IndexId,
    body: Body,
) -> Result<ExportLogsServiceResponse, OtlpApiError> {
    let export_logs_request: ExportLogsServiceRequest =
        prost::Message::decode(&body.content[..])
            .map_err(|err| OtlpApiError::InvalidPayload(err.to_string()))?;
    let mut request = tonic::Request::new(export_logs_request);
    let index = index_id
        .try_into()
        .map_err(|_| OtlpApiError::InvalidPayload("invalid index id".to_string()))?;
    request
        .metadata_mut()
        .insert(OtelSignal::Logs.header_name(), index);
    let result = otlp_logs_service
        .export(request)
        .await
        .map_err(|err| OtlpApiError::Ingest(err.to_string()))?;
    Ok(result.into_inner())
}

async fn otlp_ingest_traces(
    otlp_traces_service: OtlpGrpcTracesService,
    index_id: IndexId,
    body: Body,
) -> Result<ExportTraceServiceResponse, OtlpApiError> {
    let export_traces_request: ExportTraceServiceRequest =
        prost::Message::decode(&body.content[..])
            .map_err(|err| OtlpApiError::InvalidPayload(err.to_string()))?;
    let mut request = tonic::Request::new(export_traces_request);
    let index = index_id
        .try_into()
        .map_err(|_| OtlpApiError::InvalidPayload("invalid index id".to_string()))?;
    request
        .metadata_mut()
        .insert(OtelSignal::Traces.header_name(), index);
    let response = otlp_traces_service
        .export(request)
        .await
        .map_err(|err| OtlpApiError::Ingest(err.to_string()))?;
    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use prost::Message;
    use quickwit_ingest::CommitType;
    use quickwit_opentelemetry::otlp::{
        OtlpGrpcLogsService, OtlpGrpcTracesService, make_resource_spans_for_test,
    };
    use quickwit_proto::ingest::router::{
        IngestResponseV2, IngestRouterServiceClient, IngestSuccess, MockIngestRouterService,
    };
    use quickwit_proto::opentelemetry::proto::collector::logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    };
    use quickwit_proto::opentelemetry::proto::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    };
    use quickwit_proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use quickwit_proto::opentelemetry::proto::resource::v1::Resource;
    use warp::Filter;

    use super::otlp_ingest_api_handlers;
    use crate::rest::recover_fn;

    fn compress(body: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(body).expect("Failed to write to encoder");
        encoder.finish().expect("Failed to finish compression")
    }

    #[tokio::test]
    async fn test_otlp_ingest_logs_handler() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .times(2)
            .withf(|request| {
                if request.subrequests.len() == 1 {
                    let subrequest = &request.subrequests[0];
                    subrequest.doc_batch.is_some()
                    // && request.commit == CommitType::Auto as i32
                    && subrequest.doc_batch.as_ref().unwrap().doc_lengths.len() == 1
                    && subrequest.index_id == quickwit_opentelemetry::otlp::OTEL_LOGS_INDEX_ID
                } else {
                    false
                }
            })
            .returning(|_| {
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        num_ingested_docs: 1,
                        ..Default::default()
                    }],
                    failures: Vec::new(),
                })
            });
        mock_ingest_router
            .expect_ingest()
            .times(2)
            .withf(|request| {
                if request.subrequests.len() == 1 {
                    let subrequest = &request.subrequests[0];
                    subrequest.doc_batch.is_some()
                    // && request.commit == CommitType::Auto as i32
                    && subrequest.doc_batch.as_ref().unwrap().doc_lengths.len() == 1
                    && subrequest.index_id == "otel-logs-v0_6"
                } else {
                    false
                }
            })
            .returning(|_| {
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        num_ingested_docs: 1,
                        ..Default::default()
                    }],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let logs_service = OtlpGrpcLogsService::new(ingest_router.clone());
        let traces_service = OtlpGrpcTracesService::new(ingest_router, Some(CommitType::Force));
        let export_logs_request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: None,
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                        time_unix_nano: 1704036033047000000,
                        severity_number: 0,
                        severity_text: "ERROR".to_string(),
                        span_id: Vec::new(),
                        trace_id: Vec::new(),
                        flags: 0,
                        observed_time_unix_nano: 0,
                    }],
                    scope: None,
                    schema_url: "".to_string(),
                }],
                schema_url: "".to_string(),
            }],
        };
        let body = export_logs_request.encode_to_vec();
        let otlp_traces_api_handler =
            otlp_ingest_api_handlers(Some(logs_service), Some(traces_service)).recover(recover_fn);
        {
            // Test default otlp endpoint
            let resp = warp::test::request()
                .path("/otlp/v1/logs")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .body(body.clone())
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportLogsServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(
                actual_response
                    .partial_success
                    .unwrap()
                    .rejected_log_records,
                0
            );
        }
        {
            // Test default otlp endpoint with compression
            let resp = warp::test::request()
                .path("/otlp/v1/logs")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .header("content-encoding", "gzip")
                .body(compress(&body))
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportLogsServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(
                actual_response
                    .partial_success
                    .unwrap()
                    .rejected_log_records,
                0
            );
        }
        {
            // Test endpoint with index ID through header
            let resp = warp::test::request()
                .path("/otlp/v1/logs")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .header("qw-otel-logs-index", "otel-logs-v0_6")
                .body(body.clone())
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportLogsServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(
                actual_response
                    .partial_success
                    .unwrap()
                    .rejected_log_records,
                0
            );
        }
        {
            // Test endpoint with given index ID through path.
            let resp = warp::test::request()
                .path("/otel-logs-v0_6/otlp/v1/logs")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .body(body.clone())
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportLogsServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(
                actual_response
                    .partial_success
                    .unwrap()
                    .rejected_log_records,
                0
            );
        }
    }

    #[tokio::test]
    async fn test_otlp_ingest_traces_handler() {
        let mut mock_ingest_router = MockIngestRouterService::new();
        mock_ingest_router
            .expect_ingest()
            .times(2)
            .withf(|request| {
                if request.subrequests.len() == 1 {
                    let subrequest = &request.subrequests[0];
                    subrequest.doc_batch.is_some()
                    // && request.commit == CommitType::Auto as i32
                    && subrequest.doc_batch.as_ref().unwrap().doc_lengths.len() == 5
                    && subrequest.index_id == quickwit_opentelemetry::otlp::OTEL_TRACES_INDEX_ID
                } else {
                    false
                }
            })
            .returning(|_| {
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        num_ingested_docs: 1,
                        ..Default::default()
                    }],
                    failures: Vec::new(),
                })
            });
        mock_ingest_router
            .expect_ingest()
            .times(2)
            .withf(|request| {
                if request.subrequests.len() == 1 {
                    let subrequest = &request.subrequests[0];
                    subrequest.doc_batch.is_some()
                    // && request.commit == CommitType::Auto as i32
                    && subrequest.doc_batch.as_ref().unwrap().doc_lengths.len() == 5
                    && subrequest.index_id == "otel-traces-v0_6"
                } else {
                    false
                }
            })
            .returning(|_| {
                Ok(IngestResponseV2 {
                    successes: vec![IngestSuccess {
                        num_ingested_docs: 1,
                        ..Default::default()
                    }],
                    failures: Vec::new(),
                })
            });
        let ingest_router = IngestRouterServiceClient::from_mock(mock_ingest_router);
        let logs_service = OtlpGrpcLogsService::new(ingest_router.clone());
        let traces_service = OtlpGrpcTracesService::new(ingest_router, Some(CommitType::Force));
        let export_trace_request = ExportTraceServiceRequest {
            resource_spans: make_resource_spans_for_test(),
        };
        let body = export_trace_request.encode_to_vec();
        let otlp_traces_api_handler =
            otlp_ingest_api_handlers(Some(logs_service), Some(traces_service)).recover(recover_fn);
        {
            // Test default otlp endpoint
            let resp = warp::test::request()
                .path("/otlp/v1/traces")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .body(body.clone())
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportTraceServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(actual_response.partial_success.unwrap().rejected_spans, 0);
        }
        {
            // Test default otlp endpoint with compression
            let resp = warp::test::request()
                .path("/otlp/v1/traces")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .header("content-encoding", "gzip")
                .body(compress(&body))
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportTraceServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(actual_response.partial_success.unwrap().rejected_spans, 0);
        }
        {
            // Test endpoint with given index ID through header.
            let resp = warp::test::request()
                .path("/otlp/v1/traces")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .header("qw-otel-traces-index", "otel-traces-v0_6")
                .body(body.clone())
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportTraceServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(actual_response.partial_success.unwrap().rejected_spans, 0);
        }
        {
            // Test endpoint with given index ID through path.
            let resp = warp::test::request()
                .path("/otel-traces-v0_6/otlp/v1/traces")
                .method("POST")
                .header("content-type", "application/x-protobuf")
                .body(body)
                .reply(&otlp_traces_api_handler)
                .await;
            assert_eq!(resp.status(), 200);
            let actual_response: ExportTraceServiceResponse =
                serde_json::from_slice(resp.body()).unwrap();
            assert!(actual_response.partial_success.is_some());
            assert_eq!(actual_response.partial_success.unwrap().rejected_spans, 0);
        }
    }
}
