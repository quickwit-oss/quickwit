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

use hyper::StatusCode;
use itertools::Itertools;
use quickwit_jaeger::JaegerService;
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTracesRequest, GetOperationsRequest, GetServicesRequest, GetTraceRequest,
    SpansResponseChunk, TraceQueryParameters,
};
use quickwit_proto::tonic;
use quickwit_proto::tonic::Request;
use tokio_stream::StreamExt;
use tracing::info;
use warp::{Filter, Rejection};

use crate::jaeger_api::model::{
    JaegerError, JaegerResponseBody, JaegerSpan, JaegerTrace, TracesSearchQueryParams,
    ALL_OPERATIONS, DEFAULT_NUMBER_OF_TRACES,
};
use crate::jaeger_api::util::{
    hex_string_to_bytes, parse_duration_with_units, to_well_known_timestamp,
};
use crate::json_api_response::JsonApiResponse;
use crate::{require, BodyFormat};

/// Setup Jaeger API handlers
///
/// This is where all Jaeger handlers
/// should be registered.
/// Request are executed on the `otel traces v0_6` index.
pub(crate) fn jaeger_api_handlers(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    let jaeger_api_root_url = warp::path!("otel-traces-v0_6" / "jaeger" / "api" / ..);
    jaeger_api_root_url.and(
        jaeger_services_handler(jaeger_service_opt.clone())
            .or(jaeger_service_operations_handler(
                jaeger_service_opt.clone(),
            ))
            .or(jaeger_traces_search_handler(jaeger_service_opt.clone()))
            .or(jaeger_traces_handler(jaeger_service_opt.clone())),
    )
}

#[utoipa::path(
    get,
    tag = "Jaeger Services",
    path = "/otel-traces-v0_6/jaeger/api/services",
    responses(
        (status = 200, description = "Successfully fetched services names.", body = JaegerResponseBody )
    )
)]
pub fn jaeger_services_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("services")
        .and(warp::get())
        .and(require(jaeger_service_opt))
        .then(jaeger_services)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

#[utoipa::path(
    get,
    tag = "Jaeger Operations",
    path = "/otel-traces-v0_6/jaeger/api/services/{service}/operations",
    responses(
        (status = 200, description = "Successfully fetched operations names  the given service.", body = JaegerResponseBody )
    )
)]
pub fn jaeger_service_operations_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("services" / String / "operations")
        .and(warp::get())
        .and(require(jaeger_service_opt))
        .then(jaeger_service_operations)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

#[utoipa::path(
    get,
    tag = "Jaeger Traces",
    path = "/otel-traces-v0_6/jaeger/api/traces?service={service}&start={start_in_ns}&end={end_in_ns}&lookback=custom",
    responses(
        (status = 200, description = "Successfully fetched traces information.", body = JaegerResponseBody )
    ),
    params(
        TracesSearchQueryParams,
        ("service" = Option<String>, Query, description = "The service name."),
        ("start" = Option<i64>, Query, description = "The start time in nanoseconds."),
        ("end" = Option<i64>, Query, description = "The end time in nanoseconds."),
    )
)]
pub fn jaeger_traces_search_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("traces")
        .and(warp::get())
        .and(serde_qs::warp::query(serde_qs::Config::default()))
        .and(require(jaeger_service_opt))
        .then(jaeger_traces_search)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

#[utoipa::path(
    get,
    tag = "Jaeger Traces",
    path = "/otel-traces-v0_6/jaeger/api/traces/{id}",
    responses(
        (status = 200, description = "Successfully fetched traces spans for the provided trace ID.", body = JaegerResponseBody )
    )
)]
pub fn jaeger_traces_handler(
    jaeger_service_opt: Option<JaegerService>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("traces" / String)
        .and(warp::get())
        .and(require(jaeger_service_opt))
        .then(jaeger_get_trace_by_id)
        .map(|result| make_jaeger_api_response(result, BodyFormat::default()))
}

async fn jaeger_services(
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<String>>, JaegerError> {
    let get_services_response = jaeger_service
        .get_services(with_tonic(GetServicesRequest {}))
        .await
        .unwrap()
        .into_inner();
    Ok(JaegerResponseBody::<Vec<String>> {
        data: get_services_response.services,
    })
}

async fn jaeger_service_operations(
    service_name: String,
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<String>>, JaegerError> {
    let get_operations_request = GetOperationsRequest {
        service: service_name,
        span_kind: "".to_string(),
    };
    let get_operations_response = jaeger_service
        .get_operations(with_tonic(get_operations_request))
        .await
        .unwrap()
        .into_inner();

    let operations = get_operations_response
        .operations
        .into_iter()
        .map(|op| op.name)
        .collect_vec();
    Ok(JaegerResponseBody::<Vec<String>> { data: operations })
}

async fn jaeger_traces_search(
    search_params: TracesSearchQueryParams,
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<JaegerTrace>>, JaegerError> {
    let query = TraceQueryParameters {
        service_name: search_params.service.unwrap_or_default(),
        operation_name: search_params
            .operation
            .unwrap_or(ALL_OPERATIONS.to_string()),
        tags: Default::default(),
        start_time_min: search_params.start.map(to_well_known_timestamp),
        start_time_max: search_params.end.map(to_well_known_timestamp),
        duration_min: parse_duration_with_units(search_params.min_duration),
        duration_max: parse_duration_with_units(search_params.max_duration),
        num_traces: search_params.limit.unwrap_or(DEFAULT_NUMBER_OF_TRACES),
    };
    let find_traces_request = FindTracesRequest { query: Some(query) };

    let mut span_stream = jaeger_service
        .find_traces(with_tonic(find_traces_request))
        .await
        .unwrap()
        .into_inner();
    let SpansResponseChunk { spans } = span_stream.next().await.unwrap().unwrap();

    let result: Vec<JaegerTrace> = spans
        .iter()
        .map(|span| JaegerSpan::find_better_name_for_pb_convert(&span))
        .group_by(|span| span.trace_id.clone())
        .into_iter()
        .map(|(span_id, group)| JaegerTrace::new(span_id, group.collect_vec()))
        .collect_vec();

    info!("traces {:?}", result);

    Ok(JaegerResponseBody { data: result })
}

async fn jaeger_get_trace_by_id(
    trace_id_json: String,
    jaeger_service: JaegerService,
) -> Result<JaegerResponseBody<Vec<JaegerTrace>>, JaegerError> {
    let trace_id = hex_string_to_bytes(trace_id_json.as_str());
    let get_trace_request = GetTraceRequest { trace_id };
    let mut span_stream = jaeger_service
        .get_trace(with_tonic(get_trace_request))
        .await
        .unwrap()
        .into_inner();
    let SpansResponseChunk { spans } = span_stream.next().await.unwrap().unwrap();

    let result: Vec<JaegerTrace> = spans
        .iter()
        .map(|span| JaegerSpan::find_better_name_for_pb_convert(&span))
        .group_by(|span| span.trace_id.clone())
        .into_iter()
        .map(|(span_id, group)| JaegerTrace::new(span_id, group.collect_vec()))
        .collect_vec();

    Ok(JaegerResponseBody { data: result })
}

fn make_jaeger_api_response<T: serde::Serialize>(
    jaeger_result: Result<T, JaegerError>,
    format: BodyFormat,
) -> JsonApiResponse {
    let status_code = match &jaeger_result {
        Ok(_) => StatusCode::OK,
        Err(err) => err.status,
    };
    JsonApiResponse::new(&jaeger_result, status_code, &format)
}

fn with_tonic<T>(message: T) -> Request<T> {
    tonic::Request::new(message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use assert_json_diff::assert_json_include;
    use quickwit_config::JaegerConfig;
    use quickwit_opentelemetry::otlp::OTEL_TRACES_INDEX_ID;
    use quickwit_search::{encode_term_for_test, MockSearchService};
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::recover_fn;

    #[tokio::test]
    async fn test_when_jaeger_not_found() {
        let jaeger_api_handler = jaeger_api_handlers(None).recover(recover_fn);
        let resp = warp::test::request()
            .path("/otel-traces-v0_6/jaeger/api/services")
            .reply(&jaeger_api_handler)
            .await;
        let error_body = serde_json::from_slice::<HashMap<String, String>>(resp.body()).unwrap();
        assert_eq!(resp.status(), 404);
        assert!(error_body.contains_key("message"));
        assert_eq!(error_body.get("message").unwrap(), "Route not found");
    }

    #[tokio::test]
    async fn test_jaeger_services() -> anyhow::Result<()> {
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_root_list_terms()
            .withf(|req| {
                req.index_id == OTEL_TRACES_INDEX_ID
                    && req.field == "service_name"
                    && req.start_timestamp.is_some()
            })
            .return_once(|_| {
                Ok(quickwit_proto::search::ListTermsResponse {
                    num_hits: 3,
                    terms: vec![
                        encode_term_for_test!("service1"),
                        encode_term_for_test!("service2"),
                        encode_term_for_test!("service3"),
                    ],
                    elapsed_time_micros: 0,
                    errors: Vec::new(),
                })
            });
        let mock_search_service = Arc::new(mock_search_service);
        let jaeger = JaegerService::new(JaegerConfig::default(), mock_search_service);

        let jaeger_api_handler = jaeger_api_handlers(Some(jaeger)).recover(recover_fn);
        let resp = warp::test::request()
            .path("/otel-traces-v0_6/jaeger/api/services")
            .reply(&jaeger_api_handler)
            .await;
        assert_eq!(resp.status(), 200);
        let actual_response_json: JsonValue = serde_json::from_slice(resp.body())?;
        let expected_response_json = serde_json::json!(["service1", "service2", "service3"]);
        assert_json_include!(
            actual: actual_response_json.get("data").unwrap(),
            expected: expected_response_json
        );
        Ok(())
    }
}
