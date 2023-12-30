// Copyright (C) 2023 Quickwit, Inc.
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
use quickwit_opentelemetry::otlp::{OtlpGrpcLogsService, OtlpGrpcTracesService};
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
    oltp_ingest_logs_handler(otlp_logs_service).or(oltp_ingest_traces_handler(otlp_traces_service))
}

pub(crate) fn oltp_ingest_logs_handler(
    otlp_log_service: Option<OtlpGrpcLogsService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_log_service)
        .and(warp::path!(String / "oltp" / "logs"))
        .and(warp::post())
        .and(warp::body::bytes())
        .then(otlp_ingest_logs)
        .and(with_arg(BodyFormat::default()))
        .map(make_json_api_response)
}

pub(crate) fn oltp_ingest_traces_handler(
    otlp_traces_service: Option<OtlpGrpcTracesService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    require(otlp_traces_service)
        .and(warp::path!(String / "oltp" / "traces"))
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
mod tests {}
