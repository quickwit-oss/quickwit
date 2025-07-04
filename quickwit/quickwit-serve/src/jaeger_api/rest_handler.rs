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

use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Router};
use itertools::Itertools;
use quickwit_jaeger::JaegerService;
use quickwit_proto::jaeger::storage::v1::{
    FindTracesRequest, GetOperationsRequest, GetServicesRequest, GetTraceRequest,
    SpansResponseChunk, TraceQueryParameters,
};
use quickwit_proto::tonic;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

use super::model::build_jaeger_traces;
use super::parse_duration::{parse_duration_with_units, to_well_known_timestamp};
use crate::BodyFormat;
use crate::jaeger_api::model::{
    DEFAULT_NUMBER_OF_TRACES, JaegerError, JaegerResponseBody, JaegerSpan, JaegerTrace,
    TracesSearchQueryParams,
};
use crate::rest_api_response::RestApiResponse;
use crate::search_api::extract_index_id_patterns;

#[derive(utoipa::OpenApi)]
#[openapi(paths(
    jaeger_services_handler,
    jaeger_service_operations_handler,
    jaeger_traces_search_handler,
    jaeger_traces_handler
))]
pub(crate) struct JaegerApi;

/// Creates routes for Jaeger API endpoints
pub(crate) fn jaeger_routes(jaeger_service_opt: Option<JaegerService>) -> Router {
    Router::new()
        .route("/:index/jaeger/api/services", get(jaeger_services_handler))
        .route(
            "/:index/jaeger/api/services/:service/operations",
            get(jaeger_service_operations_handler),
        )
        .route(
            "/:index/jaeger/api/traces",
            get(jaeger_traces_search_handler),
        )
        .route("/:index/jaeger/api/traces/:id", get(jaeger_traces_handler))
        .layer(Extension(jaeger_service_opt))
}

/// Axum handler for GET /{index}/jaeger/api/services
#[utoipa::path(
    get,
    tag = "Jaeger",
    path = "/{otel-traces-index-id}/jaeger/api/services",
    responses(
        (status = 200, description = "Successfully fetched services names.", body = JaegerResponseBody )
    ),
    params(
        ("otel-traces-index-id" = String, Path, description = "The name of the index to get services for.")
    )
)]
async fn jaeger_services_handler(
    Extension(jaeger_service_opt): Extension<Option<JaegerService>>,
    Path(index): Path<String>,
) -> impl IntoResponse {
    let Some(jaeger_service) = jaeger_service_opt else {
        return make_jaeger_api_response::<()>(
            Err(JaegerError {
                status: StatusCode::NOT_FOUND,
                message: "Jaeger service is not available".to_string(),
            }),
            BodyFormat::default(),
        );
    };

    let index_id_patterns = match extract_index_id_patterns(index).await {
        Ok(patterns) => patterns,
        Err(_) => {
            return make_jaeger_api_response::<()>(
                Err(JaegerError {
                    status: StatusCode::BAD_REQUEST,
                    message: "Invalid index pattern".to_string(),
                }),
                BodyFormat::default(),
            );
        }
    };

    let result = jaeger_services(index_id_patterns, jaeger_service).await;
    make_jaeger_api_response(result, BodyFormat::default())
}

/// Axum handler for GET /{index}/jaeger/api/services/{service}/operations
#[utoipa::path(
    get,
    tag = "Jaeger",
    path = "/{otel-traces-index-id}/jaeger/api/services/{service}/operations",
    responses(
        (status = 200, description = "Successfully fetched operations names the given service.", body = JaegerResponseBody )
    ),
    params(
        ("otel-traces-index-id" = String, Path, description = "The name of the index to get operations for."),
        ("service" = String, Path, description = "The name of the service to get operations for."),
    )
)]
async fn jaeger_service_operations_handler(
    Extension(jaeger_service_opt): Extension<Option<JaegerService>>,
    Path((index, service)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(jaeger_service) = jaeger_service_opt else {
        return make_jaeger_api_response::<()>(
            Err(JaegerError {
                status: StatusCode::NOT_FOUND,
                message: "Jaeger service is not available".to_string(),
            }),
            BodyFormat::default(),
        );
    };

    let index_id_patterns = match extract_index_id_patterns(index).await {
        Ok(patterns) => patterns,
        Err(_) => {
            return make_jaeger_api_response::<()>(
                Err(JaegerError {
                    status: StatusCode::BAD_REQUEST,
                    message: "Invalid index pattern".to_string(),
                }),
                BodyFormat::default(),
            );
        }
    };

    let result = jaeger_service_operations(index_id_patterns, service, jaeger_service).await;
    make_jaeger_api_response(result, BodyFormat::default())
}

/// Axum handler for GET /{index}/jaeger/api/traces
#[utoipa::path(
    get,
    tag = "Jaeger",
    path = "/{otel-traces-index-id}/jaeger/api/traces",
    responses(
        (status = 200, description = "Successfully fetched traces information.", body = JaegerResponseBody )
    ),
    params(
        ("otel-traces-index-id" = String, Path, description = "The name of the index to get traces for."),
        ("service" = Option<String>, Query, description = "The service name."),
        ("operation" = Option<String>, Query, description = "The operation name."),
        ("start" = Option<i64>, Query, description = "The start time in nanoseconds."),
        ("end" = Option<i64>, Query, description = "The end time in nanoseconds."),
        ("tags" = Option<String>, Query, description = "Sets tags with values in the logfmt format, such as error=true status=200."),
        ("min_duration" = Option<String>, Query, description = "Filters all traces with a duration higher than the set value. Possible values are 1.2s, 100ms, 500us."),
        ("max_duration" = Option<String>, Query, description = "Filters all traces with a duration lower than the set value. Possible values are 1.2s, 100ms, 500us."),
        ("limit" = Option<i32>, Query, description = "Limits the number of traces returned."),
    )
)]
async fn jaeger_traces_search_handler(
    Extension(jaeger_service_opt): Extension<Option<JaegerService>>,
    Path(index): Path<String>,
    Query(search_params): Query<TracesSearchQueryParams>,
) -> impl IntoResponse {
    let Some(jaeger_service) = jaeger_service_opt else {
        return make_jaeger_api_response::<()>(
            Err(JaegerError {
                status: StatusCode::NOT_FOUND,
                message: "Jaeger service is not available".to_string(),
            }),
            BodyFormat::default(),
        );
    };

    let index_id_patterns = match extract_index_id_patterns(index).await {
        Ok(patterns) => patterns,
        Err(_) => {
            return make_jaeger_api_response::<()>(
                Err(JaegerError {
                    status: StatusCode::BAD_REQUEST,
                    message: "Invalid index pattern".to_string(),
                }),
                BodyFormat::default(),
            );
        }
    };

    let result = jaeger_traces_search(index_id_patterns, search_params, jaeger_service).await;
    make_jaeger_api_response(result, BodyFormat::default())
}

/// Axum handler for GET /{index}/jaeger/api/traces/{id}
#[utoipa::path(
    get,
    tag = "Jaeger",
    path = "/{otel-traces-index-id}/jaeger/api/traces/{id}",
    responses(
        (status = 200, description = "Successfully fetched traces spans for the provided trace ID.", body = JaegerResponseBody )
    ),
    params(
        ("otel-traces-index-id" = String, Path, description = "The name of the index to get traces for."),
        ("id" = String, Path, description = "The ID of the trace to get spans for."),
    )
)]
async fn jaeger_traces_handler(
    Extension(jaeger_service_opt): Extension<Option<JaegerService>>,
    Path((index, trace_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let Some(jaeger_service) = jaeger_service_opt else {
        return make_jaeger_api_response::<()>(
            Err(JaegerError {
                status: StatusCode::NOT_FOUND,
                message: "Jaeger service is not available".to_string(),
            }),
            BodyFormat::default(),
        );
    };

    let index_id_patterns = match extract_index_id_patterns(index).await {
        Ok(patterns) => patterns,
        Err(_) => {
            return make_jaeger_api_response::<()>(
                Err(JaegerError {
                    status: StatusCode::BAD_REQUEST,
                    message: "Invalid index pattern".to_string(),
                }),
                BodyFormat::default(),
            );
        }
    };

    let result = jaeger_get_trace_by_id(index_id_patterns, trace_id, jaeger_service).await;
    make_jaeger_api_response(result, BodyFormat::default())
}

async fn jaeger_services(
    index_id_patterns: Vec<String>,
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<String>>, JaegerError> {
    let get_services_response = jaeger_service
        .get_services_for_indexes(GetServicesRequest {}, index_id_patterns)
        .await
        .map_err(|error| JaegerError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("failed to fetch services: {}", error),
        })?;
    Ok(JaegerResponseBody::<Vec<String>> {
        data: get_services_response.services,
    })
}

async fn jaeger_service_operations(
    index_id_patterns: Vec<String>,
    service_name: String,
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<String>>, JaegerError> {
    let get_operations_request = GetOperationsRequest {
        service: service_name,
        span_kind: "".to_string(),
    };
    let get_operations_response = jaeger_service
        .get_operations_for_indexes(get_operations_request, index_id_patterns)
        .await
        .map_err(|error| JaegerError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("failed to fetch services: {error}"),
        })?;

    let operations = get_operations_response
        .operations
        .into_iter()
        .map(|op| op.name)
        .collect_vec();
    Ok(JaegerResponseBody::<Vec<String>> { data: operations })
}

async fn jaeger_traces_search(
    index_id_patterns: Vec<String>,
    search_params: TracesSearchQueryParams,
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<JaegerTrace>>, JaegerError> {
    let duration_min = search_params
        .min_duration
        .map(parse_duration_with_units)
        .transpose()?;
    let duration_max = search_params
        .max_duration
        .map(parse_duration_with_units)
        .transpose()?;
    let tags = search_params
        .tags
        .clone()
        .map(|s| {
            serde_json::from_str::<HashMap<String, String>>(&s).map_err(|error| {
                let error_msg = format!(
                    "failed to deserialize tags `{:?}`: {:?}",
                    search_params.tags, error
                );
                error!(error_msg);
                JaegerError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: error_msg,
                }
            })
        })
        .transpose()?
        .unwrap_or(Default::default());
    let query = TraceQueryParameters {
        service_name: search_params.service.unwrap_or_default(),
        operation_name: search_params.operation.unwrap_or_default(),
        tags,
        start_time_min: search_params
            .start
            .map(|ts| to_well_known_timestamp(ts * 1000)),
        start_time_max: search_params
            .end
            .map(|ts| to_well_known_timestamp(ts * 1000)),
        duration_min,
        duration_max,
        num_traces: search_params.limit.unwrap_or(DEFAULT_NUMBER_OF_TRACES),
    };
    let find_traces_request = FindTracesRequest { query: Some(query) };
    let spans_chunk_stream = jaeger_service
        .find_traces_for_indexes(
            find_traces_request,
            "find_traces",
            Instant::now(),
            index_id_patterns,
            true,
        )
        .await
        .map_err(|error| {
            error!(error = ?error, "failed to fetch traces");
            JaegerError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: "failed to fetch traces".to_string(),
            }
        })?;
    let jaeger_spans = collect_and_build_jaeger_spans(spans_chunk_stream).await?;
    let jaeger_traces: Vec<JaegerTrace> = build_jaeger_traces(jaeger_spans)?;
    Ok(JaegerResponseBody {
        data: jaeger_traces,
    })
}

async fn collect_and_build_jaeger_spans(
    mut spans_chunk_stream: ReceiverStream<Result<SpansResponseChunk, tonic::Status>>,
) -> anyhow::Result<Vec<JaegerSpan>> {
    let mut all_spans = Vec::<JaegerSpan>::new();
    while let Some(Ok(SpansResponseChunk { spans })) = spans_chunk_stream.next().await {
        let jaeger_spans: Vec<JaegerSpan> =
            spans.into_iter().map(JaegerSpan::try_from).try_collect()?;
        all_spans.extend(jaeger_spans);
    }
    Ok(all_spans)
}

async fn jaeger_get_trace_by_id(
    index_id_patterns: Vec<String>,
    trace_id_string: String,
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<JaegerTrace>>, JaegerError> {
    let trace_id = hex::decode(trace_id_string.clone()).map_err(|error| {
        error!(error = ?error, "failed to decode trace `{}`", trace_id_string.clone());
        JaegerError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "failed to decode trace id".to_string(),
        }
    })?;
    let get_trace_request = GetTraceRequest { trace_id };
    let spans_chunk_stream: ReceiverStream<Result<SpansResponseChunk, tonic::Status>> =
        jaeger_service
            .get_trace_for_indexes(
                get_trace_request,
                "get_trace",
                Instant::now(),
                index_id_patterns,
            )
            .await
            .map_err(|error| {
                error!(error = ?error, "failed to fetch trace `{trace_id_string}`");
                JaegerError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: "failed to fetch trace".to_string(),
                }
            })?;
    let jaeger_spans = collect_and_build_jaeger_spans(spans_chunk_stream).await?;
    let jaeger_traces: Vec<JaegerTrace> = build_jaeger_traces(jaeger_spans)?;
    Ok(JaegerResponseBody {
        data: jaeger_traces,
    })
}

fn make_jaeger_api_response<T: serde::Serialize>(
    jaeger_result: Result<T, JaegerError>,
    body_format: BodyFormat,
) -> RestApiResponse {
    let status_code = match &jaeger_result {
        Ok(_) => StatusCode::OK,
        Err(err) => err.status,
    };
    RestApiResponse::new(&jaeger_result, status_code, body_format)
}
