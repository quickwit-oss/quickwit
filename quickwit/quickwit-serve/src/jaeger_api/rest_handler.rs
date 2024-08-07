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

use std::collections::HashMap;
use std::time::Instant;

use http_serde::http::StatusCode;
use itertools::Itertools;
use quickwit_jaeger::JaegerService;
use quickwit_proto::jaeger::storage::v1::{
    FindTracesRequest, GetOperationsRequest, GetServicesRequest, GetTraceRequest,
    SpansResponseChunk, TraceQueryParameters,
};
use quickwit_proto::tonic;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::error;
use warp::{Filter, Rejection};

use super::model::build_jaeger_traces;
use super::parse_duration::{parse_duration_with_units, to_well_known_timestamp};
use crate::jaeger_api::model::{
    JaegerError, JaegerResponseBody, JaegerSpan, JaegerTrace, TracesSearchQueryParams,
    DEFAULT_NUMBER_OF_TRACES,
};
use crate::rest::recover_fn;
use crate::rest_api_response::RestApiResponse;
use crate::search_api::extract_index_id_patterns;
use crate::{require, BodyFormat};

#[derive(utoipa::OpenApi)]
#[openapi(paths(
    jaeger_services_handler,
    jaeger_service_operations_handler,
    jaeger_traces_search_handler,
    jaeger_traces_handler
))]
pub(crate) struct JaegerApi;

/// Setup Jaeger API handlers
///
/// This is where all Jaeger handlers
/// should be registered.
/// Request are executed on the `otel traces v0_7` index.
pub(crate) fn jaeger_api_handlers(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_services_handler(jaeger_service_opt.clone())
        .or(jaeger_service_operations_handler(
            jaeger_service_opt.clone(),
        ))
        .or(jaeger_traces_search_handler(jaeger_service_opt.clone()))
        .or(jaeger_traces_handler(jaeger_service_opt.clone()))
        .recover(recover_fn)
}

fn jaeger_api_path_filter() -> impl Filter<Extract = (Vec<String>,), Error = Rejection> + Clone {
    warp::path!(String / "jaeger" / "api" / ..)
        .and(warp::get())
        .and_then(extract_index_id_patterns)
}

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
pub fn jaeger_services_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_api_path_filter()
        .and(warp::path!("services"))
        .and(require(jaeger_service_opt))
        .then(jaeger_services)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

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
pub fn jaeger_service_operations_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_api_path_filter()
        .and(warp::path!("services" / String / "operations"))
        .and(require(jaeger_service_opt))
        .then(jaeger_service_operations)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

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
pub fn jaeger_traces_search_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_api_path_filter()
        .and(warp::path!("traces"))
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(require(jaeger_service_opt))
        .then(jaeger_traces_search)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

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
pub fn jaeger_traces_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    jaeger_api_path_filter()
        .and(warp::path!("traces" / String))
        .and(warp::get())
        .and(require(jaeger_service_opt))
        .then(jaeger_get_trace_by_id)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
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
        start_time_min: search_params.start.map(to_well_known_timestamp),
        start_time_max: search_params.end.map(to_well_known_timestamp),
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use quickwit_config::JaegerConfig;
    use quickwit_opentelemetry::otlp::OTEL_TRACES_INDEX_ID;
    use quickwit_search::MockSearchService;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::recover_fn;

    #[tokio::test]
    async fn test_when_jaeger_not_found() {
        let jaeger_api_handler = jaeger_api_handlers(None).recover(crate::rest::recover_fn_final);
        let resp = warp::test::request()
            .path("/otel-traces-v0_7/jaeger/api/services")
            .reply(&jaeger_api_handler)
            .await;
        assert_eq!(resp.status(), 404);
        let error_body = serde_json::from_slice::<HashMap<String, String>>(resp.body()).unwrap();
        assert!(error_body.contains_key("message"));
        assert_eq!(error_body.get("message").unwrap(), "Route not found");
    }

    #[tokio::test]
    async fn test_jaeger_services() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_list_terms()
            .withf(|req| {
                req.index_id_patterns == vec![OTEL_TRACES_INDEX_ID]
                    && req.field == "service_name"
                    && req.start_timestamp.is_some()
            })
            .return_once(|_| {
                Ok(quickwit_proto::search::ListTermsResponse {
                    num_hits: 0,
                    terms: Vec::new(),
                    elapsed_time_micros: 0,
                    errors: Vec::new(),
                })
            });
        let mock_search_service = Arc::new(mock_search_service);
        let jaeger = JaegerService::new(JaegerConfig::default(), mock_search_service);

        let jaeger_api_handler = jaeger_api_handlers(Some(jaeger)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/otel-traces-v0_7/jaeger/api/services")
            .reply(&jaeger_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        assert!(actual_response_json
            .get("data")
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_jaeger_service_operations() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_list_terms()
            .withf(|req| {
                req.index_id_patterns == vec![OTEL_TRACES_INDEX_ID]
                    && req.field == "span_fingerprint"
                    && req.start_timestamp.is_some()
            })
            .return_once(|_| {
                Ok(quickwit_proto::search::ListTermsResponse {
                    num_hits: 1,
                    terms: Vec::new(),
                    elapsed_time_micros: 0,
                    errors: Vec::new(),
                })
            });
        let mock_search_service = Arc::new(mock_search_service);
        let jaeger = JaegerService::new(JaegerConfig::default(), mock_search_service);
        let jaeger_api_handler = jaeger_api_handlers(Some(jaeger)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/otel-traces-v0_7/jaeger/api/services/service1/operations")
            .reply(&jaeger_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        assert!(actual_response_json
            .get("data")
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_jaeger_traces_search() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .withf(|req| {
                assert!(req.query_ast.contains(
                    "{\"type\":\"term\",\"field\":\"resource_attributes.tag.first\",\"value\":\"\
                     common\"}"
                ));
                assert!(req.query_ast.contains(
                    "{\"type\":\"term\",\"field\":\"resource_attributes.tag.second\",\"value\":\"\
                     true\"}"
                ));
                assert_eq!(
                    req.index_id_patterns,
                    vec![OTEL_TRACES_INDEX_ID.to_string()]
                );
                true
            })
            .return_once(|_| {
                Ok(quickwit_proto::search::SearchResponse {
                    num_hits: 0,
                    hits: Vec::new(),
                    elapsed_time_micros: 0,
                    errors: Vec::new(),
                    aggregation: None,
                    scroll_id: None,
                })
            });
        let mock_search_service = Arc::new(mock_search_service);
        let jaeger = JaegerService::new(JaegerConfig::default(), mock_search_service);
        let jaeger_api_handler = jaeger_api_handlers(Some(jaeger)).recover(recover_fn);
        let resp = warp::test::request()
            .path(
                "/otel-traces-v0_7/jaeger/api/traces?service=quickwit&\
                 operation=delete_splits_marked_for_deletion&minDuration=500us&maxDuration=1.2s&\
                 tags=%7B%22tag.first%22%3A%22common%22%2C%22tag.second%22%3A%22true%22%7D&\
                 limit=1&start=1702352106016000&end=1702373706016000&lookback=custom",
            )
            .reply(&jaeger_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_jaeger_trace_by_id() {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_search()
            .withf(|req| req.index_id_patterns == vec![OTEL_TRACES_INDEX_ID.to_string()])
            .return_once(|_| {
                Ok(quickwit_proto::search::SearchResponse {
                    num_hits: 0,
                    hits: Vec::new(),
                    elapsed_time_micros: 0,
                    errors: Vec::new(),
                    aggregation: None,
                    scroll_id: None,
                })
            });
        let mock_search_service = Arc::new(mock_search_service);
        let jaeger = JaegerService::new(JaegerConfig::default(), mock_search_service);

        let jaeger_api_handler = jaeger_api_handlers(Some(jaeger)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/otel-traces-v0_7/jaeger/api/traces/1506026ddd216249555653218dc88a6c")
            .reply(&jaeger_api_handler)
            .await;

        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body()).unwrap();
        assert!(actual_response_json
            .get("data")
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty());
    }
}
