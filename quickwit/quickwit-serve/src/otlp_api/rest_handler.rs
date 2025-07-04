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

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Extension, Router};
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

use crate::BodyFormat;
use crate::decompression::Body;
use crate::rest_api_response::into_rest_api_response;

#[derive(utoipa::OpenApi)]
#[openapi(paths(
    otlp_default_logs_handler,
    otlp_logs_handler,
    otlp_default_traces_handler,
    otlp_ingest_traces_handler
))]
pub struct OtlpApi;

/// Creates routes for OTLP API endpoints
pub(crate) fn otlp_routes(
    otlp_logs_service: Option<OtlpGrpcLogsService>,
    otlp_traces_service: Option<OtlpGrpcTracesService>,
) -> Router {
    Router::new()
        .route("/otlp/v1/logs", post(otlp_default_logs_handler))
        .route("/:index/otlp/v1/logs", post(otlp_logs_handler))
        .route("/otlp/v1/traces", post(otlp_default_traces_handler))
        .route("/:index/otlp/v1/traces", post(otlp_ingest_traces_handler))
        .layer(Extension(otlp_logs_service))
        .layer(Extension(otlp_traces_service))
}

/// Helper function to process axum body into Body struct for OTLP handlers
async fn process_body(
    headers: &HeaderMap,
    body_bytes: axum::body::Bytes,
) -> Result<Body, OtlpApiError> {
    use std::io::Read;
    use std::sync::OnceLock;

    use bytes::Bytes;
    use flate2::read::{MultiGzDecoder, ZlibDecoder};
    use quickwit_common::thread_pool::run_cpu_intensive;

    use crate::load_shield::LoadShield;

    fn get_ingest_load_shield() -> &'static LoadShield {
        static LOAD_SHIELD: OnceLock<LoadShield> = OnceLock::new();
        LOAD_SHIELD.get_or_init(|| LoadShield::new("ingest"))
    }

    // Acquire load shield permit
    let permit = get_ingest_load_shield()
        .acquire_permit()
        .await
        .map_err(|_| OtlpApiError::InvalidPayload("Load shield permit denied".to_string()))?;

    // Get content encoding
    let encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Decompress body if needed
    let content = match encoding.as_deref() {
        Some("identity") => body_bytes,
        Some("gzip" | "x-gzip") => {
            let body_vec = body_bytes.to_vec();
            let decompressed = run_cpu_intensive(move || {
                let mut decompressed = Vec::new();
                let mut decoder = MultiGzDecoder::new(body_vec.as_slice());
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|_| OtlpApiError::InvalidPayload("Corrupted gzip data".to_string()))?;
                Ok::<Vec<u8>, OtlpApiError>(decompressed)
            })
            .await
            .map_err(|_| OtlpApiError::InvalidPayload("Decompression failed".to_string()))??;
            Bytes::from(decompressed)
        }
        Some("zstd") => {
            let body_vec = body_bytes.to_vec();
            let decompressed = run_cpu_intensive(move || {
                zstd::decode_all(body_vec.as_slice())
                    .map(Bytes::from)
                    .map_err(|_| OtlpApiError::InvalidPayload("Corrupted zstd data".to_string()))
            })
            .await
            .map_err(|_| OtlpApiError::InvalidPayload("Decompression failed".to_string()))??;
            decompressed
        }
        Some("deflate" | "x-deflate") => {
            let body_vec = body_bytes.to_vec();
            let decompressed = run_cpu_intensive(move || {
                let mut decompressed = Vec::new();
                ZlibDecoder::new(body_vec.as_slice())
                    .read_to_end(&mut decompressed)
                    .map_err(|_| {
                        OtlpApiError::InvalidPayload("Corrupted deflate data".to_string())
                    })?;
                Ok::<Vec<u8>, OtlpApiError>(decompressed)
            })
            .await
            .map_err(|_| OtlpApiError::InvalidPayload("Decompression failed".to_string()))??;
            Bytes::from(decompressed)
        }
        Some(encoding) => {
            return Err(OtlpApiError::InvalidPayload(format!(
                "Unsupported Content-Encoding {}. Supported encodings are 'gzip', 'zstd', and \
                 'deflate'",
                encoding
            )));
        }
        None => body_bytes,
    };

    Ok(Body::new(content, permit))
}

/// Axum handler for POST /otlp/v1/logs
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
async fn otlp_default_logs_handler(
    Extension(otlp_logs_service): Extension<Option<OtlpGrpcLogsService>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let Some(otlp_logs_service) = otlp_logs_service else {
        return into_rest_api_response::<ExportLogsServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "OTLP logs service not available".to_string(),
            )),
            BodyFormat::default(),
        );
    };

    // Check content-type header
    if !headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("application/x-protobuf"))
        .unwrap_or(false)
    {
        return into_rest_api_response::<ExportLogsServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "Invalid content-type".to_string(),
            )),
            BodyFormat::default(),
        );
    }

    // Get index ID from header or use default
    let index_id = headers
        .get(OtelSignal::Logs.header_name())
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| OtelSignal::Logs.default_index_id().to_string());

    let body = match process_body(&headers, body).await {
        Ok(body) => body,
        Err(err) => {
            return into_rest_api_response::<ExportLogsServiceResponse, _>(
                Err(err),
                BodyFormat::default(),
            );
        }
    };
    let result = otlp_ingest_logs(otlp_logs_service, index_id, body).await;
    into_rest_api_response(result, BodyFormat::default())
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
async fn otlp_logs_handler(
    Extension(otlp_logs_service): Extension<Option<OtlpGrpcLogsService>>,
    Path(index_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let Some(otlp_logs_service) = otlp_logs_service else {
        return into_rest_api_response::<ExportLogsServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "OTLP logs service not available".to_string(),
            )),
            BodyFormat::default(),
        );
    };

    // Check content-type header
    if !headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("application/x-protobuf"))
        .unwrap_or(false)
    {
        return into_rest_api_response::<ExportLogsServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "Invalid content-type".to_string(),
            )),
            BodyFormat::default(),
        );
    }

    let body = match process_body(&headers, body).await {
        Ok(body) => body,
        Err(err) => {
            return into_rest_api_response::<ExportLogsServiceResponse, _>(
                Err(err),
                BodyFormat::default(),
            );
        }
    };
    let result = otlp_ingest_logs(otlp_logs_service, index_id, body).await;
    into_rest_api_response(result, BodyFormat::default())
}

/// Open Telemetry REST/Protobuf traces ingest endpoint.
#[utoipa::path(
    post,
    tag = "Open Telemetry",
    path = "/otlp/v1/traces",
    request_body(content = String, description = "`ExportTraceServiceRequest` protobuf message", content_type = "application/x-protobuf"),
    responses(
        (status = 200, description = "Successfully exported traces.", body = ExportTraceServiceResponse)
    ),
)]
async fn otlp_default_traces_handler(
    Extension(otlp_traces_service): Extension<Option<OtlpGrpcTracesService>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let Some(otlp_traces_service) = otlp_traces_service else {
        return into_rest_api_response::<ExportTraceServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "OTLP traces service not available".to_string(),
            )),
            BodyFormat::default(),
        );
    };

    // Check content-type header
    if !headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("application/x-protobuf"))
        .unwrap_or(false)
    {
        return into_rest_api_response::<ExportTraceServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "Invalid content-type".to_string(),
            )),
            BodyFormat::default(),
        );
    }

    // Get index ID from header or use default
    let index_id = headers
        .get(OtelSignal::Traces.header_name())
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| OtelSignal::Traces.default_index_id().to_string());

    let body = match process_body(&headers, body).await {
        Ok(body) => body,
        Err(err) => {
            return into_rest_api_response::<ExportTraceServiceResponse, _>(
                Err(err),
                BodyFormat::default(),
            );
        }
    };
    let result = otlp_ingest_traces(otlp_traces_service, index_id, body).await;
    into_rest_api_response(result, BodyFormat::default())
}

/// Open Telemetry REST/Protobuf traces ingest endpoint.
#[utoipa::path(
    post,
    tag = "Open Telemetry",
    path = "/{index}/otlp/v1/traces",
    request_body(content = String, description = "`ExportTraceServiceRequest` protobuf message", content_type = "application/x-protobuf"),
    responses(
        (status = 200, description = "Successfully exported traces.", body = ExportTraceServiceResponse)
    ),
)]
async fn otlp_ingest_traces_handler(
    Extension(otlp_traces_service): Extension<Option<OtlpGrpcTracesService>>,
    Path(index_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let Some(otlp_traces_service) = otlp_traces_service else {
        return into_rest_api_response::<ExportTraceServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "OTLP traces service not available".to_string(),
            )),
            BodyFormat::default(),
        );
    };

    // Check content-type header
    if !headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("application/x-protobuf"))
        .unwrap_or(false)
    {
        return into_rest_api_response::<ExportTraceServiceResponse, _>(
            Err(OtlpApiError::InvalidPayload(
                "Invalid content-type".to_string(),
            )),
            BodyFormat::default(),
        );
    }

    let body = match process_body(&headers, body).await {
        Ok(body) => body,
        Err(err) => {
            return into_rest_api_response::<ExportTraceServiceResponse, _>(
                Err(err),
                BodyFormat::default(),
            );
        }
    };
    let result = otlp_ingest_traces(otlp_traces_service, index_id, body).await;
    into_rest_api_response(result, BodyFormat::default())
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
