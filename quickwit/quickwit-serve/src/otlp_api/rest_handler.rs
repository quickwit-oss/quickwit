// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use bytes::Bytes;
use quickwit_opentelemetry::otlp::{
    OtlpGrpcLogsService, OtlpGrpcTracesService, OTEL_LOGS_INDEX_ID, OTEL_TRACES_INDEX_ID,
};
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsService;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceService;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use quickwit_proto::{tonic, ServiceError, ServiceErrorCode};
use serde::{self, Serialize};
use tracing::error;
use warp::{Filter, Rejection};

use crate::json_api_response::make_json_api_response;
use crate::{require, with_arg, BodyFormat};

#[derive(utoipa::OpenApi)]
#[openapi(paths())]
pub(crate) struct OtlpApi;

/// Setup OpenTelemetry API handlers.
pub(crate) fn otlp_ingest_api_handlers(
    otlp_logs_service: Option<OtlpGrpcLogsService>,
    otlp_traces_service: Option<OtlpGrpcTracesService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    otlp_default_logs_handler(otlp_logs_service.clone())
        .or(otlp_default_traces_handler(otlp_traces_service.clone()))
        .or(otlp_logs_handler(otlp_logs_service))
        .or(otlp_ingest_traces_handler(otlp_traces_service))
}

pub(crate) fn otlp_default_logs_handler(
    otlp_logs_service: Option<OtlpGrpcLogsService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_logs_service)
        .and(warp::path!("otlp" / "v1" / "logs"))
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/x-protobuf",
        ))
        .and(warp::post())
        .and(warp::body::bytes())
        .then(|otlp_logs_service, body| async move {
            otlp_ingest_logs(otlp_logs_service, OTEL_LOGS_INDEX_ID.to_string(), body).await
        })
        .and(with_arg(BodyFormat::default()))
        .map(make_json_api_response)
}

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
        .and(warp::body::bytes())
        .then(otlp_ingest_logs)
        .and(with_arg(BodyFormat::default()))
        .map(make_json_api_response)
}

pub(crate) fn otlp_default_traces_handler(
    otlp_traces_service: Option<OtlpGrpcTracesService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_traces_service)
        .and(warp::path!("otlp" / "v1" / "traces"))
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/x-protobuf",
        ))
        .and(warp::post())
        .and(warp::body::bytes())
        .then(|otlp_traces_service, body| async move {
            otlp_ingest_traces(otlp_traces_service, OTEL_TRACES_INDEX_ID.to_string(), body).await
        })
        .and(with_arg(BodyFormat::default()))
        .map(make_json_api_response)
}

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
        .and(warp::body::bytes())
        .then(otlp_ingest_traces)
        .and(with_arg(BodyFormat::default()))
        .map(make_json_api_response)
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
            OtlpApiError::Ingest(_) => ServiceErrorCode::Internal,
        }
    }
}

async fn otlp_ingest_logs(
    otlp_logs_service: OtlpGrpcLogsService,
    _index_id: String, // <- TODO: use index ID when gRPC service supports it.
    body: Bytes,
) -> Result<ExportLogsServiceResponse, OtlpApiError> {
    // TODO: use index ID.
    let export_logs_request: ExportLogsServiceRequest = prost::Message::decode(&body[..])
        .map_err(|err| OtlpApiError::InvalidPayload(err.to_string()))?;
    let result = otlp_logs_service
        .export(tonic::Request::new(export_logs_request))
        .await
        .map_err(|err| OtlpApiError::Ingest(err.to_string()))?;
    Ok(result.into_inner())
}

async fn otlp_ingest_traces(
    otlp_traces_service: OtlpGrpcTracesService,
    _index_id: String, // <- TODO: use index ID when gRPC service supports it.
    body: Bytes,
) -> Result<ExportTraceServiceResponse, OtlpApiError> {
    let export_traces_request: ExportTraceServiceRequest = prost::Message::decode(&body[..])
        .map_err(|err| OtlpApiError::InvalidPayload(err.to_string()))?;
    let response = otlp_traces_service
        .export(tonic::Request::new(export_traces_request))
        .await
        .map_err(|err| OtlpApiError::Ingest(err.to_string()))?;
    Ok(response.into_inner())
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use quickwit_ingest::{CommitType, IngestResponse, IngestServiceClient};
    use quickwit_opentelemetry::otlp::{
        make_resource_spans_for_test, OtlpGrpcLogsService, OtlpGrpcTracesService,
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

    #[tokio::test]
    async fn test_otlp_ingest_logs_handler() {
        let mut ingest_service_mock = IngestServiceClient::mock();
        ingest_service_mock
            .expect_ingest()
            .withf(|request| {
                request.doc_batches.len() == 1
                    // && request.commit == CommitType::Auto as i32
                    && request.doc_batches[0].doc_lengths.len() == 1
            })
            .returning(|_| {
                Ok(IngestResponse {
                    num_docs_for_processing: 1,
                })
            });
        let ingest_service_client = IngestServiceClient::from(ingest_service_mock);
        let logs_service = OtlpGrpcLogsService::new(ingest_service_client.clone());
        let traces_service =
            OtlpGrpcTracesService::new(ingest_service_client, Some(CommitType::Force));
        let export_logs_request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: None,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        time_unix_nano: 1704036033047000000,
                        severity_number: 0,
                        severity_text: "ERROR".to_string(),
                        span_id: vec![],
                        trace_id: vec![],
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
            // Test endpoint with given index ID.
            let resp = warp::test::request()
                .path("/otel-traces-v0_6/otlp/v1/logs")
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
        let mut ingest_service_mock = IngestServiceClient::mock();
        ingest_service_mock
            .expect_ingest()
            .withf(|request| {
                request.doc_batches.len() == 1
                    && request.commit == CommitType::Force as i32
                    && request.doc_batches[0].doc_lengths.len() == 5
            })
            .returning(|_| {
                Ok(IngestResponse {
                    num_docs_for_processing: 1,
                })
            });
        let ingest_service_client = IngestServiceClient::from(ingest_service_mock);
        let logs_service = OtlpGrpcLogsService::new(ingest_service_client.clone());
        let traces_service =
            OtlpGrpcTracesService::new(ingest_service_client, Some(CommitType::Force));
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
            // Test endpoint with given index ID.
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
