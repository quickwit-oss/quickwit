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
use std::mem;
use std::ops::{Bound, RangeInclusive};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use itertools::{Either, Itertools};
use prost::Message;
use prost_types::{Duration as WellKnownDuration, Timestamp as WellKnownTimestamp};
use quickwit_config::JaegerConfig;
use quickwit_opentelemetry::otlp::{
    Event as QwEvent, Link as QwLink, OTEL_TRACES_INDEX_ID, Span as QwSpan, SpanFingerprint,
    SpanId, SpanKind as QwSpanKind, SpanStatus as QwSpanStatus, TraceId,
    extract_otel_traces_index_id_patterns_from_metadata,
};
use quickwit_proto::jaeger::api_v2::{
    KeyValue as JaegerKeyValue, Log as JaegerLog, Process as JaegerProcess, Span as JaegerSpan,
    SpanRef as JaegerSpanRef, SpanRefType as JaegerSpanRefType, ValueType,
};
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, FindTraceIDsResponse, FindTracesRequest, GetOperationsRequest,
    GetOperationsResponse, GetServicesRequest, GetServicesResponse, GetTraceRequest, Operation,
    SpansResponseChunk, TraceQueryParameters,
};
use quickwit_proto::opentelemetry::proto::trace::v1::status::StatusCode as OtlpStatusCode;
use quickwit_proto::search::{CountHits, ListTermsRequest, SearchRequest};
use quickwit_query::BooleanOperand;
use quickwit_query::query_ast::{BoolQuery, QueryAst, RangeQuery, TermQuery, UserInputQuery};
use quickwit_search::{FindTraceIdsCollector, SearchService};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tantivy::collector::Collector;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{Span as RuntimeSpan, debug, error, instrument, warn};

use crate::metrics::JAEGER_SERVICE_METRICS;

mod metrics;

// OpenTelemetry to Jaeger Transformation
// <https://opentelemetry.io/docs/reference/specification/trace/sdk_exporters/jaeger/>

type TimeIntervalSecs = RangeInclusive<i64>;

type JaegerResult<T> = Result<T, Status>;

type SpanStream = ReceiverStream<Result<SpansResponseChunk, Status>>;

#[derive(Clone)]
pub struct JaegerService {
    search_service: Arc<dyn SearchService>,
    lookback_period_secs: i64,
    max_trace_duration_secs: i64,
    max_fetch_spans: u64,
}

impl JaegerService {
    pub fn new(config: JaegerConfig, search_service: Arc<dyn SearchService>) -> Self {
        Self {
            search_service,
            lookback_period_secs: config.lookback_period().as_secs() as i64,
            max_trace_duration_secs: config.max_trace_duration().as_secs() as i64,
            max_fetch_spans: config.max_fetch_spans.get(),
        }
    }

    #[instrument("get_services", skip_all)]
    pub async fn get_services_for_indexes(
        &self,
        request: GetServicesRequest,
        index_id_patterns: Vec<String>,
    ) -> JaegerResult<GetServicesResponse> {
        debug!(request=?request, index_ids=?index_id_patterns, "`get_services` request");

        let max_hits = Some(1_000);
        let start_timestamp =
            Some(OffsetDateTime::now_utc().unix_timestamp() - self.lookback_period_secs);

        let search_request = ListTermsRequest {
            index_id_patterns,
            field: "service_name".to_string(),
            max_hits,
            start_timestamp,
            end_timestamp: None,
            start_key: None,
            end_key: None,
        };
        let search_response = self.search_service.root_list_terms(search_request).await?;
        let services: Vec<String> = search_response
            .terms
            .into_iter()
            .map(|term_bytes| extract_term(&term_bytes))
            .sorted()
            .collect();
        let response = GetServicesResponse { services };
        debug!(response=?response, "`get_services` response");
        Ok(response)
    }

    #[instrument("get_operations", skip_all, fields(service=%request.service, span_kind=%request.span_kind))]
    pub async fn get_operations_for_indexes(
        &self,
        request: GetOperationsRequest,
        index_id_patterns: Vec<String>,
    ) -> JaegerResult<GetOperationsResponse> {
        debug!(request=?request, request=?request, index_ids=?index_id_patterns, "`get_operations` request");

        let max_hits = Some(1_000);
        let start_timestamp =
            Some(OffsetDateTime::now_utc().unix_timestamp() - self.lookback_period_secs);

        let span_kind_opt = request.span_kind.parse().ok();
        let start_key = SpanFingerprint::start_key(&request.service, span_kind_opt.clone());
        let end_key = SpanFingerprint::end_key(&request.service, span_kind_opt);

        let search_request = ListTermsRequest {
            index_id_patterns,
            field: "span_fingerprint".to_string(),
            max_hits,
            start_timestamp,
            end_timestamp: None,
            start_key,
            end_key,
        };
        let search_response = self.search_service.root_list_terms(search_request).await?;
        let operations: Vec<Operation> = search_response
            .terms
            .into_iter()
            .map(|term_json| extract_operation(&term_json))
            .sorted()
            .collect();
        debug!(operations=?operations, "`get_operations` response");
        let response = GetOperationsResponse {
            operations,
            operation_names: Vec::new(), // `operation_names` is deprecated.
        };
        Ok(response)
    }

    // Instrumentation happens in `find_trace_ids`.
    pub async fn find_trace_ids_for_indexes(
        &self,
        request: FindTraceIDsRequest,
        index_id_patterns: Vec<String>,
    ) -> JaegerResult<FindTraceIDsResponse> {
        debug!(request=?request, index_ids=?index_id_patterns, "`find_trace_ids` request");

        let trace_query = request
            .query
            .ok_or_else(|| Status::invalid_argument("Query is empty."))?;

        let (trace_ids, _) = self.find_trace_ids(trace_query, index_id_patterns).await?;
        let trace_ids = trace_ids
            .into_iter()
            .map(|trace_id| trace_id.to_vec())
            .collect();
        debug!(trace_ids=?trace_ids, "`find_trace_ids` response");
        let response = FindTraceIDsResponse { trace_ids };
        Ok(response)
    }

    #[instrument("find_traces", skip_all)]
    pub async fn find_traces_for_indexes(
        &self,
        request: FindTracesRequest,
        operation_name: &'static str,
        request_start: Instant,
        index_id_patterns: Vec<String>,
        root_only: bool,
    ) -> JaegerResult<SpanStream> {
        debug!(request=?request, "`find_traces` request");

        let trace_query = request
            .query
            .ok_or_else(|| Status::invalid_argument("Trace query is empty."))?;
        let (trace_ids, span_timestamps_range) = self
            .find_trace_ids(trace_query, index_id_patterns.clone())
            .await?;
        let start = span_timestamps_range.start() - self.max_trace_duration_secs;
        let end = span_timestamps_range.end() + self.max_trace_duration_secs;
        let search_window = start..=end;
        let response = self
            .stream_spans(
                &trace_ids,
                search_window,
                operation_name,
                request_start,
                index_id_patterns,
                root_only,
            )
            .await?;
        Ok(response)
    }

    #[instrument("get_trace", skip_all)]
    pub async fn get_trace_for_indexes(
        &self,
        request: GetTraceRequest,
        operation_name: &'static str,
        request_start: Instant,
        index_id_patterns: Vec<String>,
    ) -> JaegerResult<SpanStream> {
        debug!(request=?request, "`get_trace` request");
        debug_assert_eq!(request.trace_id.len(), 16);
        let trace_id = TraceId::try_from(request.trace_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let end = OffsetDateTime::now_utc().unix_timestamp();
        let start = end - self.lookback_period_secs;
        let search_window = start..=end;
        let response = self
            .stream_spans(
                &[trace_id],
                search_window,
                operation_name,
                request_start,
                index_id_patterns,
                false,
            )
            .await?;
        Ok(response)
    }

    #[instrument("find_trace_ids", skip_all fields(service_name=%trace_query.service_name, operation_name=%trace_query.operation_name))]
    async fn find_trace_ids(
        &self,
        trace_query: TraceQueryParameters,
        index_id_patterns: Vec<String>,
    ) -> Result<(Vec<TraceId>, TimeIntervalSecs), Status> {
        let span_kind_opt = None;
        let min_span_start_timestamp_secs_opt = trace_query.start_time_min.map(|ts| ts.seconds);
        let max_span_start_timestamp_secs_opt = trace_query.start_time_max.map(|ts| ts.seconds);
        let min_span_duration_millis_opt = trace_query
            .duration_min
            .and_then(|d| to_duration_millis(&d));
        let max_span_duration_millis_opt = trace_query
            .duration_max
            .and_then(|d| to_duration_millis(&d));
        let query = build_search_query(
            &trace_query.service_name,
            span_kind_opt,
            &trace_query.operation_name,
            trace_query.tags,
            min_span_start_timestamp_secs_opt,
            max_span_start_timestamp_secs_opt,
            min_span_duration_millis_opt,
            max_span_duration_millis_opt,
        );
        let query_ast =
            serde_json::to_string(&query).map_err(|err| Status::internal(err.to_string()))?;
        let aggregation_query = build_aggregations_query(trace_query.num_traces as usize);
        let max_hits = 0;
        let search_request = SearchRequest {
            index_id_patterns,
            query_ast,
            aggregation_request: Some(aggregation_query),
            max_hits,
            start_timestamp: min_span_start_timestamp_secs_opt,
            end_timestamp: max_span_start_timestamp_secs_opt,
            count_hits: CountHits::Underestimate.into(),
            ..Default::default()
        };
        let search_response = self.search_service.root_search(search_request).await?;

        let Some(agg_result_postcard) = search_response.aggregation_postcard else {
            debug!("the query matched no traces");
            return Ok((Vec::new(), 0..=0));
        };
        let trace_ids = collect_trace_ids(&agg_result_postcard)?;
        debug!("the query matched {} traces.", trace_ids.0.len());
        Ok(trace_ids)
    }

    #[instrument("stream_spans", skip_all, fields(num_traces=%trace_ids.len(), num_spans=Empty, num_bytes=Empty))]
    async fn stream_spans(
        &self,
        trace_ids: &[TraceId],
        search_window: TimeIntervalSecs,
        operation_name: &'static str,
        request_start: Instant,
        index_id_patterns: Vec<String>,
        root_only: bool,
    ) -> Result<SpanStream, Status> {
        if trace_ids.is_empty() {
            let (_tx, rx) = mpsc::channel(1);
            return Ok(ReceiverStream::new(rx));
        }
        let num_traces = trace_ids.len() as u64;
        let mut query = BoolQuery::default();

        for trace_id in trace_ids {
            let value = trace_id.hex_display();
            let term_query = TermQuery {
                field: "trace_id".to_string(),
                value,
            };
            query.should.push(term_query.into());
        }
        if root_only {
            // we do this so we don't error on old indexes, and instead return both root and non
            // root spans
            let is_root = UserInputQuery {
                user_text: "NOT is_root:false".to_string(),
                default_fields: None,
                default_operator: BooleanOperand::And,
                lenient: true,
            };
            let mut new_query = BoolQuery::default();
            new_query.must.push(query.into());
            new_query.must.push(is_root.into());
            query = new_query;
        }

        let query_ast: QueryAst = query.into();
        let query_ast =
            serde_json::to_string(&query_ast).map_err(|err| Status::internal(err.to_string()))?;

        let search_request = SearchRequest {
            index_id_patterns,
            query_ast,
            start_timestamp: Some(*search_window.start()),
            end_timestamp: Some(*search_window.end()),
            max_hits: self.max_fetch_spans,
            count_hits: CountHits::Underestimate.into(),
            ..Default::default()
        };
        let search_response = match self.search_service.root_search(search_request).await {
            Ok(search_response) => search_response,
            Err(search_error) => {
                error!(search_error=?search_error, "failed to fetch spans");
                record_error(operation_name, request_start);
                return Err(Status::internal("Failed to fetch spans."));
            }
        };
        let mut spans: Vec<JaegerSpan> = Vec::with_capacity(search_response.hits.len());

        for hit in search_response.hits {
            match qw_span_to_jaeger_span(&hit.json) {
                Ok(span) => {
                    spans.push(span);
                }
                Err(status) => {
                    record_error(operation_name, request_start);
                    return Err(status);
                }
            };
        }
        if trace_ids.len() > 1 {
            spans.sort_unstable_by(|left, right| left.trace_id.cmp(&right.trace_id));
        }
        let (tx, rx) = mpsc::channel(2);
        let current_span = RuntimeSpan::current();

        tokio::task::spawn(async move {
            const MAX_CHUNK_LEN: usize = 1_000;
            const MAX_CHUNK_NUM_BYTES: usize = 4 * 1024 * 1024 - 10 * 1024; // 4 MiB, the default max size of gRPC messages, minus some headroom.

            let chunk_len = MAX_CHUNK_LEN.min(spans.len());
            let mut chunk = Vec::with_capacity(chunk_len);
            let mut chunk_num_bytes = 0;
            let mut num_spans_total = 0;
            let mut num_bytes_total = 0;

            while let Some(span) = spans.pop() {
                let span_num_bytes = span.encoded_len();

                if chunk.len() == MAX_CHUNK_LEN
                    || chunk_num_bytes + span_num_bytes > MAX_CHUNK_NUM_BYTES
                {
                    let num_spans = chunk.len();
                    num_spans_total += num_spans;
                    num_bytes_total += chunk_num_bytes;

                    // + 1 to account for the span we just popped from `spans` but haven't yet
                    // appended to `chunk`.
                    let chunk_len = MAX_CHUNK_LEN.min(spans.len() + 1);
                    let chunk = mem::replace(&mut chunk, Vec::with_capacity(chunk_len));
                    if let Err(send_error) = tx.send(Ok(SpansResponseChunk { spans: chunk })).await
                    {
                        debug!(send_error=?send_error, "client disconnected");
                        return;
                    }
                    record_send(operation_name, num_spans, chunk_num_bytes);
                    chunk_num_bytes = 0;
                }
                chunk_num_bytes += span_num_bytes;
                chunk.push(span);
            }
            if !chunk.is_empty() {
                let num_spans = chunk.len();
                num_spans_total += num_spans;
                num_bytes_total += chunk_num_bytes;

                if let Err(send_error) = tx.send(Ok(SpansResponseChunk { spans: chunk })).await {
                    debug!(error=?send_error, "client disconnected");
                    return;
                }
                record_send(operation_name, num_spans, chunk_num_bytes);
            }
            current_span.record("num_spans", num_spans_total);
            current_span.record("num_bytes", num_bytes_total);

            JAEGER_SERVICE_METRICS
                .fetched_traces_total
                .with_label_values([operation_name, OTEL_TRACES_INDEX_ID])
                .inc_by(num_traces);

            let elapsed = request_start.elapsed().as_secs_f64();
            JAEGER_SERVICE_METRICS
                .request_duration_seconds
                .with_label_values([operation_name, OTEL_TRACES_INDEX_ID, "false"])
                .observe(elapsed);
        });
        Ok(ReceiverStream::new(rx))
    }
}

macro_rules! metrics {
    ($expr:expr, [$operation:ident, $($label:expr),*]) => {
        let start = std::time::Instant::now();
        let labels = [stringify!($operation), $($label,)*];
        JAEGER_SERVICE_METRICS.requests_total.with_label_values(labels).inc();
        let (res, is_error) = match $expr {
            ok @ Ok(_) => {
                (ok, "false")
            },
            err @ Err(_) => {
                JAEGER_SERVICE_METRICS.request_errors_total.with_label_values(labels).inc();
                (err, "true")
            },
        };
        let elapsed = start.elapsed().as_secs_f64();
        let labels = [stringify!($operation), $($label,)* is_error];
        JAEGER_SERVICE_METRICS.request_duration_seconds.with_label_values(labels).observe(elapsed);

        return res.map(Response::new);
    };
}

fn record_error(operation_name: &'static str, request_start: Instant) {
    JAEGER_SERVICE_METRICS
        .request_errors_total
        .with_label_values([operation_name, OTEL_TRACES_INDEX_ID])
        .inc();

    let elapsed = request_start.elapsed().as_secs_f64();
    JAEGER_SERVICE_METRICS
        .request_duration_seconds
        .with_label_values([operation_name, OTEL_TRACES_INDEX_ID, "true"])
        .observe(elapsed);
}

fn record_send(operation_name: &'static str, num_spans: usize, num_bytes: usize) {
    JAEGER_SERVICE_METRICS
        .fetched_spans_total
        .with_label_values([operation_name, OTEL_TRACES_INDEX_ID])
        .inc_by(num_spans as u64);
    JAEGER_SERVICE_METRICS
        .transferred_bytes_total
        .with_label_values([operation_name, OTEL_TRACES_INDEX_ID])
        .inc_by(num_bytes as u64);
}

#[async_trait]
impl SpanReaderPlugin for JaegerService {
    type GetTraceStream = SpanStream;

    type FindTracesStream = SpanStream;

    async fn get_services(
        &self,
        request: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        metrics!(
            self.get_services_for_indexes(request.into_inner(), index_id_patterns)
                .await,
            [get_services, OTEL_TRACES_INDEX_ID]
        );
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        metrics!(
            self.get_operations_for_indexes(request.into_inner(), index_id_patterns)
                .await,
            [get_operations, OTEL_TRACES_INDEX_ID]
        );
    }

    async fn find_trace_i_ds(
        &self,
        request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        metrics!(
            self.find_trace_ids_for_indexes(request.into_inner(), index_id_patterns)
                .await,
            [find_trace_ids, OTEL_TRACES_INDEX_ID]
        );
    }

    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        self.find_traces_for_indexes(
            request.into_inner(),
            "find_traces",
            Instant::now(),
            index_id_patterns,
            false, /* if we use true, Jaeger will display "1 Span", and display an empty trace
                    * when clicking on the ui (but display the full trace after reloading the
                    * page) */
        )
        .await
        .map(Response::new)
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<Self::GetTraceStream>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;
        self.get_trace_for_indexes(
            request.into_inner(),
            "get_trace",
            Instant::now(),
            index_id_patterns,
        )
        .await
        .map(Response::new)
    }
}

#[allow(deprecated)]
fn extract_term(term_bytes: &[u8]) -> String {
    tantivy::Term::wrap(term_bytes)
        .value()
        .as_str()
        .expect("Term should be a valid UTF-8 string.")
        .to_string()
}

fn extract_operation(term_bytes: &[u8]) -> Operation {
    let term = extract_term(term_bytes);
    let fingerprint = SpanFingerprint::from_string(term);
    let span_name = fingerprint
        .span_name()
        .expect("The span fingerprint should be properly formed.")
        .to_string();
    let span_kind = fingerprint
        .span_kind()
        .map(|span_kind| span_kind.as_jaeger())
        .expect("The span fingerprint should be properly formed.")
        .to_string();
    Operation {
        name: span_name,
        span_kind,
    }
}

// TODO: builder pattern
#[allow(clippy::too_many_arguments)]
fn build_search_query(
    service_name: &str,
    span_kind_opt: Option<QwSpanKind>,
    span_name: &str,
    mut tags: HashMap<String, String>,
    min_span_start_timestamp_secs_opt: Option<i64>,
    max_span_start_timestamp_secs_opt: Option<i64>,
    min_span_duration_millis_opt: Option<i64>,
    max_span_duration_millis_opt: Option<i64>,
) -> QueryAst {
    // TODO disable based on some feature?
    if let Some(qw_query) = tags.remove("_qw_query") {
        return quickwit_query::query_ast::query_ast_from_user_text(&qw_query, None);
    }
    // TODO should we use filter instead of must? Does it changes anything? Less scoring?
    let mut query_ast = BoolQuery::default();

    if !service_name.is_empty() {
        query_ast.must.push(
            TermQuery {
                field: "service_name".to_string(),
                value: service_name.to_string(),
            }
            .into(),
        );
    }
    if let Some(span_kind) = span_kind_opt {
        query_ast.must.push(
            TermQuery {
                field: "span_kind".to_string(),
                value: span_kind.as_char().to_string(),
            }
            .into(),
        )
    }
    if !span_name.is_empty() {
        query_ast.must.push(
            TermQuery {
                field: "span_name".to_string(),
                value: span_name.to_string(),
            }
            .into(),
        )
    }
    if !tags.is_empty() {
        // Sort the tags for deterministic tests.
        for (key, value) in tags.into_iter().sorted() {
            // In Jaeger land, `event` is a regular event attribute whereas in OpenTelemetry land,
            // it is an event top-level field named `name`. In Quickwit, it is stored as
            // `event_name` to distinguish it from the span top-level field `name`.
            if key == "event" {
                query_ast.must.push(
                    TermQuery {
                        field: "events.event_name".to_string(),
                        value,
                    }
                    .into(),
                )
            } else if key == "error" && value == "true" {
                query_ast.must.push(
                    TermQuery {
                        field: "span_status.code".to_string(),
                        value: "error".to_string(),
                    }
                    .into(),
                )
            } else if key == "error" && value == "false" {
                query_ast.must_not.push(
                    TermQuery {
                        field: "span_status.code".to_string(),
                        value: "error".to_string(),
                    }
                    .into(),
                )
            } else {
                let mut sub_query = BoolQuery::default();

                sub_query.should.push(
                    TermQuery {
                        field: format!("resource_attributes.{key}"),
                        value: value.clone(),
                    }
                    .into(),
                );
                sub_query.should.push(
                    TermQuery {
                        field: format!("span_attributes.{key}"),
                        value: value.clone(),
                    }
                    .into(),
                );
                sub_query.should.push(
                    TermQuery {
                        field: format!("events.event_attributes.{key}"),
                        value,
                    }
                    .into(),
                );
                query_ast.must.push(sub_query.into())
            }
        }
    }
    if min_span_start_timestamp_secs_opt.is_some() || max_span_start_timestamp_secs_opt.is_some() {
        let mut start_range = RangeQuery {
            field: "span_start_timestamp_nanos".to_string(),
            lower_bound: Bound::Unbounded,
            upper_bound: Bound::Unbounded,
        };

        if let Some(min_span_start_timestamp_secs) = min_span_start_timestamp_secs_opt {
            let min_span_start_datetime =
                OffsetDateTime::from_unix_timestamp(min_span_start_timestamp_secs)
                    .expect("Timestamp should fall within the [Date::MIN, Date::MAX] interval.");
            let min_span_start_datetime_rfc3339 = min_span_start_datetime
                .format(&Rfc3339)
                .expect("Datetime should be formattable to RFC 3339.");
            start_range.lower_bound = Bound::Included(min_span_start_datetime_rfc3339.into());
        }

        if let Some(max_span_start_timestamp_secs) = max_span_start_timestamp_secs_opt {
            let max_span_start_datetime =
                OffsetDateTime::from_unix_timestamp(max_span_start_timestamp_secs)
                    .expect("Timestamp should fall within the [Date::MIN, Date::MAX] interval.");
            let max_span_start_datetime_rfc3339 = max_span_start_datetime
                .format(&Rfc3339)
                .expect("Datetime should be formattable to RFC 3339.");
            start_range.upper_bound = Bound::Included(max_span_start_datetime_rfc3339.into());
        }

        query_ast.must.push(start_range.into());
    }
    if min_span_duration_millis_opt.is_some() || max_span_duration_millis_opt.is_some() {
        let mut duration_range = RangeQuery {
            field: "span_duration_millis".to_string(),
            lower_bound: Bound::Unbounded,
            upper_bound: Bound::Unbounded,
        };

        if let Some(min_span_duration_millis) = min_span_duration_millis_opt {
            duration_range.lower_bound = Bound::Included(min_span_duration_millis.into());
        }

        if let Some(max_span_duration_millis) = max_span_duration_millis_opt {
            duration_range.upper_bound = Bound::Included(max_span_duration_millis.into());
        }

        query_ast.must.push(duration_range.into());
    }
    if !query_ast.must.is_empty() || !query_ast.must_not.is_empty() {
        query_ast.into()
    } else {
        QueryAst::MatchAll
    }
}

fn build_aggregations_query(num_traces: usize) -> String {
    let query = serde_json::to_string(&FindTraceIdsCollector {
        num_traces,
        trace_id_field_name: "trace_id".to_string(),
        span_timestamp_field_name: "span_start_timestamp_nanos".to_string(),
    })
    .expect("The collector should be JSON serializable.");
    debug!(query=%query, "Aggregations query");
    query
}

#[allow(clippy::result_large_err)]
fn qw_span_to_jaeger_span(qw_span_json: &str) -> Result<JaegerSpan, Status> {
    let mut qw_span: QwSpan = json_deserialize(qw_span_json, "span")?;

    let start_time = Some(to_well_known_timestamp(qw_span.span_start_timestamp_nanos));
    let duration = Some(to_well_known_duration(
        qw_span.span_start_timestamp_nanos,
        qw_span.span_end_timestamp_nanos,
    ));
    qw_span.resource_attributes.remove("service.name");
    let process = Some(JaegerProcess {
        service_name: qw_span.service_name,
        tags: otlp_attributes_to_jaeger_tags(qw_span.resource_attributes),
    });
    let logs: Vec<JaegerLog> = qw_span
        .events
        .into_iter()
        .map(qw_event_to_jaeger_log)
        .collect::<Result<_, _>>()?;

    let mut tags = otlp_attributes_to_jaeger_tags(qw_span.span_attributes);
    inject_dropped_count_tags(
        &mut tags,
        qw_span.span_dropped_attributes_count,
        qw_span.span_dropped_events_count,
        qw_span.span_dropped_links_count,
    );
    inject_span_kind_tag(&mut tags, qw_span.span_kind);
    inject_span_status_tags(&mut tags, qw_span.span_status);

    let references =
        otlp_links_to_jaeger_references(&qw_span.trace_id, qw_span.parent_span_id, qw_span.links)?;

    let span = JaegerSpan {
        trace_id: qw_span.trace_id.to_vec(),
        span_id: qw_span.span_id.to_vec(),
        operation_name: qw_span.span_name,
        references,
        flags: 0, // TODO
        start_time,
        duration,
        tags,
        logs,
        process,
        process_id: "".to_string(), // TODO
        warnings: Vec::new(),       // TODO
    };
    Ok(span)
}

fn to_duration_millis(duration: &WellKnownDuration) -> Option<i64> {
    let duration_millis = duration.seconds * 1_000 + (duration.nanos as i64) / 1_000_000;
    if duration_millis == 0 {
        None
    } else {
        Some(duration_millis)
    }
}

fn to_well_known_timestamp(timestamp_nanos: u64) -> WellKnownTimestamp {
    let seconds = (timestamp_nanos / 1_000_000_000) as i64;
    let nanos = (timestamp_nanos % 1_000_000_000) as i32;
    WellKnownTimestamp { seconds, nanos }
}

fn to_well_known_duration(
    start_timestamp_nanos: u64,
    end_timestamp_nanos: u64,
) -> WellKnownDuration {
    let duration_nanos = end_timestamp_nanos - start_timestamp_nanos;
    let seconds = (duration_nanos / 1_000_000_000) as i64;
    let nanos = (duration_nanos % 1_000_000_000) as i32;
    WellKnownDuration { seconds, nanos }
}

fn inject_dropped_count_tags(
    tags: &mut Vec<JaegerKeyValue>,
    dropped_attributes_count: u32,
    dropped_events_count: u32,
    dropped_links_count: u32,
) {
    for (dropped_count, key) in [
        (dropped_attributes_count, "otel.dropped_attributes_count"),
        (dropped_events_count, "otel.dropped_events_count"),
        (dropped_links_count, "otel.dropped_links_count"),
    ] {
        if dropped_count > 0 {
            tags.push(JaegerKeyValue {
                key: key.to_string(),
                v_type: ValueType::Int64 as i32,
                v_str: String::new(),
                v_bool: false,
                v_int64: dropped_count as i64,
                v_float64: 0.0,
                v_binary: Vec::new(),
            });
        }
    }
}

/// Injects span kind tag.
/// <https://opentelemetry.io/docs/specs/otel/trace/sdk_exporters/jaeger/#spankind>
fn inject_span_kind_tag(tags: &mut Vec<JaegerKeyValue>, span_kind_id: u32) {
    // OpenTelemetry SpanKind field MUST be encoded as span.kind tag in Jaeger span, except for
    // SpanKind.INTERNAL, which SHOULD NOT be translated to a tag.
    let span_kind = match span_kind_id {
        0 | 1 => return,
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => {
            warn!(span_kind_id=%span_kind_id, "unknown span kind ID");
            return;
        }
    };
    tags.push(JaegerKeyValue {
        key: "span.kind".to_string(),
        v_type: ValueType::String as i32,
        v_str: span_kind.to_string(),
        v_bool: false,
        v_int64: 0,
        v_float64: 0.0,
        v_binary: Vec::new(),
    });
}

/// Injects span status tags.
/// <https://opentelemetry.io/docs/specs/otel/common/mapping-to-non-otlp/#span-status>
fn inject_span_status_tags(tags: &mut Vec<JaegerKeyValue>, span_status: QwSpanStatus) {
    // "Span Status MUST be reported as key-value pairs associated with the Span, unless the Status
    // is UNSET. In the latter case it MUST NOT be reported."
    match span_status.code {
        OtlpStatusCode::Unset => {}
        OtlpStatusCode::Ok => {
            // "Name of the code, either OK or ERROR. MUST NOT be set if the code is UNSET."
            tags.push(JaegerKeyValue {
                key: "otel.status_code".to_string(),
                v_type: ValueType::String as i32,
                v_str: "OK".to_string(),
                v_bool: false,
                v_int64: 0,
                v_float64: 0.0,
                v_binary: Vec::new(),
            });
        }
        OtlpStatusCode::Error => {
            // "Name of the code, either OK or ERROR. MUST NOT be set if the code is UNSET."
            tags.push(JaegerKeyValue {
                key: "otel.status_code".to_string(),
                v_type: ValueType::String as i32,
                v_str: "ERROR".to_string(),
                v_bool: false,
                v_int64: 0,
                v_float64: 0.0,
                v_binary: Vec::new(),
            });
            // "Description of the Status if it has a value otherwise not set."
            if let Some(message) = span_status.message {
                tags.push(JaegerKeyValue {
                    key: "otel.status_description".to_string(),
                    v_type: ValueType::String as i32,
                    v_str: message,
                    v_bool: false,
                    v_int64: 0,
                    v_float64: 0.0,
                    v_binary: Vec::new(),
                });
            }
            // "When Span Status is set to ERROR, an error span tag MUST be added with the Boolean
            // value of true. The added error tag MAY override any previous value."
            tags.push(JaegerKeyValue {
                key: "error".to_string(),
                v_type: ValueType::Bool as i32,
                v_str: String::new(),
                v_bool: true,
                v_int64: 0,
                v_float64: 0.0,
                v_binary: Vec::new(),
            });
        }
    };
}

/// Converts OpenTelemetry attributes to Jaeger tags. Objects are flattened with
/// their keys prefixed with the parent keys delimited by a dot.
///
/// <https://opentelemetry.io/docs/specs/otel/trace/sdk_exporters/jaeger/#attributes>
fn otlp_attributes_to_jaeger_tags(
    attributes: impl IntoIterator<Item = (String, JsonValue)>,
) -> Vec<JaegerKeyValue> {
    otlp_attributes_to_jaeger_tags_inner(attributes, None)
}

/// Inner helper for `otpl_attributes_to_jaeger_tags` recursive call
///
/// PERF: as long as `attributes` IntoIterator implementation correctly sets the
/// lower bound then collect should allocate efficiently. Note that the flat map
/// may cause more allocations as we cannot predict the number of elements in the
/// iterator.
fn otlp_attributes_to_jaeger_tags_inner(
    attributes: impl IntoIterator<Item = (String, JsonValue)>,
    parent_key: Option<&str>,
) -> Vec<JaegerKeyValue> {
    attributes
        .into_iter()
        .map(|(key, value)| {
            let key = parent_key
                .map(|parent_key| format!("{parent_key}.{key}"))
                .unwrap_or(key);
            match value {
                JsonValue::Array(values) => {
                    Either::Left(Some(JaegerKeyValue {
                        key,
                        v_type: ValueType::String as i32,
                        // Array values MUST be serialized to string like a JSON list.
                        v_str: serde_json::to_string(&values).expect(
                            "A vec of `serde_json::Value` values should be JSON serializable.",
                        ),
                        ..Default::default()
                    }))
                }
                JsonValue::Bool(v_bool) => Either::Left(Some(JaegerKeyValue {
                    key,
                    v_type: ValueType::Bool as i32,
                    v_bool,
                    ..Default::default()
                })),
                JsonValue::Number(number) => {
                    let value = if let Some(v_int64) = number.as_i64() {
                        Some(JaegerKeyValue {
                            key,
                            v_type: ValueType::Int64 as i32,
                            v_int64,
                            ..Default::default()
                        })
                    } else if let Some(v_float64) = number.as_f64() {
                        Some(JaegerKeyValue {
                            key,
                            v_type: ValueType::Float64 as i32,
                            v_float64,
                            ..Default::default()
                        })
                    } else {
                        // Print some error rather than silently ignoring the value.
                        warn!("ignoring unrepresentable number value: {number:?}");
                        None
                    };

                    Either::Left(value)
                }
                JsonValue::String(v_str) => Either::Left(Some(JaegerKeyValue {
                    key,
                    v_type: ValueType::String as i32,
                    v_str,
                    ..Default::default()
                })),
                JsonValue::Null => {
                    // No use including null values in the tags, so ignore
                    Either::Left(None)
                }
                JsonValue::Object(value) => {
                    Either::Right(otlp_attributes_to_jaeger_tags_inner(value, Some(&key)))
                }
            }
        })
        .flat_map(|e| e.into_iter())
        .collect()
}

/// Converts OpenTelemetry links to Jaeger span references.
/// <https://opentelemetry.io/docs/specs/otel/trace/sdk_exporters/jaeger/#links>
#[allow(clippy::result_large_err)]
fn otlp_links_to_jaeger_references(
    trace_id: &TraceId,
    parent_span_id_opt: Option<SpanId>,
    links: Vec<QwLink>,
) -> Result<Vec<JaegerSpanRef>, Status> {
    let mut references = Vec::with_capacity(parent_span_id_opt.is_some() as usize + links.len());

    // <https://opentelemetry.io/docs/specs/otel/trace/sdk_exporters/jaeger/#parent-id>
    if let Some(parent_span_id) = parent_span_id_opt {
        let reference = JaegerSpanRef {
            trace_id: trace_id.to_vec(),
            span_id: parent_span_id.to_vec(),
            ref_type: JaegerSpanRefType::ChildOf as i32,
        };
        references.push(reference);
    }
    // "Span references generated from Link(s) MUST be added after the span reference generated from
    // Parent ID, if any."
    for link in links {
        let trace_id = link.link_trace_id.to_vec();
        let span_id = link.link_span_id.to_vec();
        let reference = JaegerSpanRef {
            trace_id,
            span_id,
            ref_type: JaegerSpanRefType::FollowsFrom as i32,
        };
        references.push(reference);
    }
    Ok(references)
}

#[allow(clippy::result_large_err)]
fn qw_event_to_jaeger_log(event: QwEvent) -> Result<JaegerLog, Status> {
    let timestamp = to_well_known_timestamp(event.event_timestamp_nanos);
    // "OpenTelemetry Event’s name field should be added to Jaeger Log’s fields map as follows: name
    // -> event. If OpenTelemetry Event contains an attribute with the key event, it should take
    // precedence over Event’s name field."
    let insert_event_name =
        !event.event_name.is_empty() && !event.event_attributes.contains_key("event");

    let mut fields = otlp_attributes_to_jaeger_tags(event.event_attributes);

    if insert_event_name {
        fields.push(JaegerKeyValue {
            key: "event".to_string(),
            v_type: ValueType::String as i32,
            v_str: event.event_name,
            v_bool: false,
            v_int64: 0,
            v_float64: 0.0,
            v_binary: Vec::new(),
        });
    }
    inject_dropped_count_tags(&mut fields, event.event_dropped_attributes_count, 0, 0);
    let log = JaegerLog {
        timestamp: Some(timestamp),
        fields,
    };
    Ok(log)
}

#[allow(clippy::result_large_err)]
fn collect_trace_ids(
    trace_ids_postcard: &[u8],
) -> Result<(Vec<TraceId>, TimeIntervalSecs), Status> {
    let collector_fruit: <FindTraceIdsCollector as Collector>::Fruit =
        postcard_deserialize(trace_ids_postcard, "trace IDs aggregation")?;
    if collector_fruit.is_empty() {
        return Ok((Vec::new(), 0..=0));
    }
    let mut trace_ids = Vec::with_capacity(collector_fruit.len());
    let mut start = i64::MAX;
    let mut end = i64::MIN;

    for trace_id in collector_fruit {
        trace_ids.push(trace_id.trace_id);
        start = start.min(trace_id.span_timestamp.into_timestamp_secs());
        end = end.max(trace_id.span_timestamp.into_timestamp_secs());
    }
    Ok((trace_ids, start..=end))
}

#[allow(clippy::result_large_err)]
fn json_deserialize<'a, T>(json: &'a str, label: &'static str) -> Result<T, Status>
where T: Deserialize<'a> {
    match serde_json::from_str(json) {
        Ok(deserialized) => Ok(deserialized),
        Err(error) => {
            error!("failed to deserialize {label}: {error:?}");
            Err(Status::internal(format!(
                "Failed to deserialize {label}: {error:?}."
            )))
        }
    }
}

#[allow(clippy::result_large_err)]
fn postcard_deserialize<'a, T>(json: &'a [u8], label: &'static str) -> Result<T, Status>
where T: Deserialize<'a> {
    match postcard::from_bytes(json) {
        Ok(deserialized) => Ok(deserialized),
        Err(error) => {
            error!("failed to deserialize {label}: {error:?}");
            Err(Status::internal(format!(
                "Failed to deserialize {label}: {error:?}."
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_opentelemetry::otlp::{OTEL_TRACES_INDEX_ID_PATTERN, OtelSignal};
    use quickwit_proto::jaeger::api_v2::ValueType;
    use quickwit_search::{MockSearchService, QuickwitAggregations, encode_term_for_test};
    use serde_json::json;

    use super::*;

    #[track_caller]
    fn get_must(ast: QueryAst) -> Vec<QueryAst> {
        match ast {
            QueryAst::Bool(boolean_query) => boolean_query.must,
            _ => panic!("expected `QueryAst::Bool`, got `{ast:?}`"),
        }
    }

    #[track_caller]
    fn get_must_not(ast: QueryAst) -> Vec<QueryAst> {
        match ast {
            QueryAst::Bool(boolean_query) => boolean_query.must_not,
            _ => panic!("expected `QueryAst::Bool`, got `{ast:?}`"),
        }
    }

    #[test]
    fn test_build_query() {
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                ),
                QueryAst::MatchAll,
            );
        }
        {
            let service_name = "quickwit search";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "service_name".to_string(),
                        value: service_name.to_string(),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::from_iter([("_qw_query".to_string(), "query".to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                ),
                quickwit_query::query_ast::UserInputQuery {
                    user_text: "query".to_string(),
                    default_fields: None,
                    default_operator: quickwit_query::BooleanOperand::And,
                    lenient: false,
                }
                .into()
            );
        }
        {
            let service_name = "";
            let span_kind = "client".parse().ok();
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "span_kind".to_string(),
                        value: "3".to_string(),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "GET /config";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "span_name".to_string(),
                        value: span_name.to_string(),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::from_iter([("error".to_string(), "true".to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "span_status.code".to_string(),
                        value: "error".to_string(),
                    }
                    .into(),
                ],
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::from_iter([("error".to_string(), "false".to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must_not(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "span_status.code".to_string(),
                        value: "error".to_string(),
                    }
                    .into(),
                ],
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tag_value = "bar baz";
            let tags = HashMap::from_iter([("foo".to_string(), tag_value.to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let event_name = "Failed to ...";
            let tags = HashMap::from_iter([("event".to_string(), event_name.to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "events.event_name".to_string(),
                        value: event_name.to_string(),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tag_value = "bar";
            let event_name = "Failed to ...";
            let tags = HashMap::from_iter([
                ("event".to_string(), event_name.to_string()),
                ("foo".to_string(), tag_value.to_string()),
            ]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "events.event_name".to_string(),
                        value: event_name.to_string(),
                    }
                    .into(),
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::from_iter([
                ("baz".to_string(), "qux".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.baz".to_string(),
                                value: "qux".to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.baz".to_string(),
                                value: "qux".to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.baz".to_string(),
                                value: "qux".to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into(),
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.foo".to_string(),
                                value: "bar".to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.foo".to_string(),
                                value: "bar".to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.foo".to_string(),
                                value: "bar".to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = Some(3);
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    RangeQuery {
                        field: "span_start_timestamp_nanos".to_string(),
                        lower_bound: Bound::Included("1970-01-01T00:00:03Z".to_string().into()),
                        upper_bound: Bound::Unbounded
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = Some(33);
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    RangeQuery {
                        field: "span_start_timestamp_nanos".to_string(),
                        lower_bound: Bound::Unbounded,
                        upper_bound: Bound::Included("1970-01-01T00:00:33Z".to_string().into()),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = Some(3);
            let max_span_start_timestamp_secs = Some(33);
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    RangeQuery {
                        field: "span_start_timestamp_nanos".to_string(),
                        lower_bound: Bound::Included("1970-01-01T00:00:03Z".to_string().into()),
                        upper_bound: Bound::Included("1970-01-01T00:00:33Z".to_string().into()),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = Some(7);
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    RangeQuery {
                        field: "span_duration_millis".to_string(),
                        lower_bound: Bound::Included(7u64.into()),
                        upper_bound: Bound::Unbounded
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = Some(77);
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    RangeQuery {
                        field: "span_duration_millis".to_string(),
                        lower_bound: Bound::Unbounded,
                        upper_bound: Bound::Included(77u64.into()),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "";
            let span_kind = None;
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = Some(7);
            let max_span_duration_secs = Some(77);
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    RangeQuery {
                        field: "span_duration_millis".to_string(),
                        lower_bound: Bound::Included(7u64.into()),
                        upper_bound: Bound::Included(77u64.into()),
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = None;
            let span_name = "";
            let tag_value = "bar";
            let tags = HashMap::from_iter([("foo".to_string(), tag_value.to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "service_name".to_string(),
                        value: service_name.to_string(),
                    }
                    .into(),
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client".parse().ok();
            let span_name = "";
            let tag_value = "bar";
            let tags = HashMap::from_iter([("foo".to_string(), tag_value.to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "service_name".to_string(),
                        value: service_name.to_string(),
                    }
                    .into(),
                    TermQuery {
                        field: "span_kind".to_string(),
                        value: "3".to_string()
                    }
                    .into(),
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client".parse().ok();
            let span_name = "leaf_search";
            let tag_value = "bar";
            let tags = HashMap::from_iter([("foo".to_string(), tag_value.to_string())]);
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = None;
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "service_name".to_string(),
                        value: service_name.to_string(),
                    }
                    .into(),
                    TermQuery {
                        field: "span_kind".to_string(),
                        value: "3".to_string()
                    }
                    .into(),
                    TermQuery {
                        field: "span_name".to_string(),
                        value: span_name.to_string(),
                    }
                    .into(),
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into()
                ]
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client".parse().ok();
            let span_name = "leaf_search";
            let tag_value = "bar";
            let tags = HashMap::from_iter([("foo".to_string(), tag_value.to_string())]);
            let min_span_start_timestamp_secs = Some(3);
            let max_span_start_timestamp_secs = Some(33);
            let min_span_duration_secs = Some(7);
            let max_span_duration_secs = Some(77);
            assert_eq!(
                get_must(build_search_query(
                    service_name,
                    span_kind,
                    span_name,
                    tags,
                    min_span_start_timestamp_secs,
                    max_span_start_timestamp_secs,
                    min_span_duration_secs,
                    max_span_duration_secs
                )),
                vec![
                    TermQuery {
                        field: "service_name".to_string(),
                        value: service_name.to_string(),
                    }
                    .into(),
                    TermQuery {
                        field: "span_kind".to_string(),
                        value: "3".to_string()
                    }
                    .into(),
                    TermQuery {
                        field: "span_name".to_string(),
                        value: span_name.to_string(),
                    }
                    .into(),
                    BoolQuery {
                        should: vec![
                            TermQuery {
                                field: "resource_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "span_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                            TermQuery {
                                field: "events.event_attributes.foo".to_string(),
                                value: tag_value.to_string(),
                            }
                            .into(),
                        ],
                        ..Default::default()
                    }
                    .into(),
                    RangeQuery {
                        field: "span_start_timestamp_nanos".to_string(),
                        lower_bound: Bound::Included("1970-01-01T00:00:03Z".to_string().into()),
                        upper_bound: Bound::Included("1970-01-01T00:00:33Z".to_string().into()),
                    }
                    .into(),
                    RangeQuery {
                        field: "span_duration_millis".to_string(),
                        lower_bound: Bound::Included(7u64.into()),
                        upper_bound: Bound::Included(77u64.into()),
                    }
                    .into(),
                ]
            );
        }
    }

    #[test]
    fn test_build_aggregations_query() {
        let aggregations_query = build_aggregations_query(77);
        let aggregations: QuickwitAggregations = serde_json::from_str(&aggregations_query).unwrap();
        let QuickwitAggregations::FindTraceIdsAggregation(collector) = aggregations else {
            panic!("Expected find trace IDs aggregation!");
        };
        assert_eq!(collector.num_traces, 77);
        assert_eq!(collector.trace_id_field_name, "trace_id");
        assert_eq!(
            collector.span_timestamp_field_name,
            "span_start_timestamp_nanos"
        );
    }

    #[test]
    fn test_to_duration_millis() {
        {
            let duration = WellKnownDuration {
                seconds: 0,
                nanos: 1,
            };
            let duration_millis = to_duration_millis(&duration);
            assert!(duration_millis.is_none())
        }
        {
            let duration = WellKnownDuration {
                seconds: 1,
                nanos: 1_000_000,
            };
            let duration_millis = to_duration_millis(&duration).unwrap();
            assert_eq!(duration_millis, 1001)
        }
    }

    #[test]
    fn test_to_well_known_duration() {
        let duration = to_well_known_duration(1_000_000_001, 2_000_000_002);
        assert_eq!(duration.seconds, 1);
        assert_eq!(duration.nanos, 1);
    }

    #[test]
    fn test_to_well_known_timestamp() {
        let timestamp = to_well_known_timestamp(1_000_000_001);
        assert_eq!(timestamp.seconds, 1);
        assert_eq!(timestamp.nanos, 1);
    }

    #[test]
    fn test_otlp_attributes_to_jaeger_tags() {
        let mut tags = otlp_attributes_to_jaeger_tags([
            ("array_int".to_string(), json!([1, 2])),
            ("array_str".to_string(), json!(["foo", "bar"])),
            ("bool".to_string(), json!(true)),
            ("float".to_string(), json!(1.0)),
            ("integer".to_string(), json!(1)),
            ("string".to_string(), json!("foo")),
            (
                "object".to_string(),
                json!({
                    "array_int": [1,2],
                    "array_str": ["foo", "bar"],
                    "bool": true,
                    "float": 1.0,
                    "integer": 1,
                    "string": "foo",
                }),
            ),
        ]);
        tags.sort_by(|left, right| left.key.cmp(&right.key));

        // a tag for the 6 keys in the root, plus 6 more for the nested keys
        assert_eq!(tags.len(), 12);

        assert_eq!(tags[0].key, "array_int");
        assert_eq!(tags[0].v_type(), ValueType::String);
        assert_eq!(tags[0].v_str, "[1,2]");

        assert_eq!(tags[1].key, "array_str");
        assert_eq!(tags[1].v_type(), ValueType::String);
        assert_eq!(tags[1].v_str, r#"["foo","bar"]"#);

        assert_eq!(tags[2].key, "bool");
        assert_eq!(tags[2].v_type(), ValueType::Bool);
        assert!(tags[2].v_bool);

        assert_eq!(tags[3].key, "float");
        assert_eq!(tags[3].v_type(), ValueType::Float64);
        assert_eq!(tags[3].v_float64, 1.0);

        assert_eq!(tags[4].key, "integer");
        assert_eq!(tags[4].v_type(), ValueType::Int64);
        assert_eq!(tags[4].v_int64, 1);

        assert_eq!(tags[5].key, "object.array_int");
        assert_eq!(tags[5].v_type(), ValueType::String);
        assert_eq!(tags[5].v_str, "[1,2]");

        assert_eq!(tags[6].key, "object.array_str");
        assert_eq!(tags[6].v_type(), ValueType::String);
        assert_eq!(tags[6].v_str, r#"["foo","bar"]"#);

        assert_eq!(tags[7].key, "object.bool");
        assert_eq!(tags[7].v_type(), ValueType::Bool);
        assert!(tags[7].v_bool);

        assert_eq!(tags[8].key, "object.float");
        assert_eq!(tags[8].v_type(), ValueType::Float64);
        assert_eq!(tags[8].v_float64, 1.0);

        assert_eq!(tags[9].key, "object.integer");
        assert_eq!(tags[9].v_type(), ValueType::Int64);
        assert_eq!(tags[9].v_int64, 1);

        assert_eq!(tags[10].key, "object.string");
        assert_eq!(tags[10].v_type(), ValueType::String);
        assert_eq!(tags[10].v_str, "foo");

        assert_eq!(tags[11].key, "string");
        assert_eq!(tags[11].v_type(), ValueType::String);
        assert_eq!(tags[11].v_str, "foo");
    }

    #[test]
    fn test_inject_dropped_attribute_tag() {
        let mut tags = Vec::new();

        inject_dropped_count_tags(&mut tags, 0, 0, 0);
        assert!(tags.is_empty());

        inject_dropped_count_tags(&mut tags, 1, 2, 3);
        assert_eq!(tags.len(), 3);

        assert_eq!(tags[0].key, "otel.dropped_attributes_count");
        assert_eq!(tags[0].v_type(), ValueType::Int64);
        assert_eq!(tags[0].v_int64, 1);

        assert_eq!(tags[1].key, "otel.dropped_events_count");
        assert_eq!(tags[1].v_type(), ValueType::Int64);
        assert_eq!(tags[1].v_int64, 2);

        assert_eq!(tags[2].key, "otel.dropped_links_count");
        assert_eq!(tags[2].v_type(), ValueType::Int64);
        assert_eq!(tags[2].v_int64, 3);
    }

    #[test]
    fn test_inject_span_kind_tag() {
        {
            let mut tags = Vec::new();
            inject_span_kind_tag(&mut tags, 0);
            assert!(tags.is_empty());
        }
        {
            let mut tags = Vec::new();
            inject_span_kind_tag(&mut tags, 1);
            assert!(tags.is_empty());
        }
        {
            for (expected_span_kind, span_kind_id) in ["server", "client", "producer", "consumer"]
                .iter()
                .zip(2..6)
            {
                let mut tags = Vec::new();
                inject_span_kind_tag(&mut tags, span_kind_id);
                assert_eq!(tags.len(), 1);

                assert_eq!(tags[0].key, "span.kind");
                assert_eq!(tags[0].v_type(), ValueType::String);
                assert_eq!(tags[0].v_str, *expected_span_kind);
            }
        }
    }

    #[test]
    fn test_inject_status_code_tag() {
        {
            let mut tags = Vec::new();
            let span_status = QwSpanStatus {
                code: OtlpStatusCode::Unset,
                message: None,
            };
            inject_span_status_tags(&mut tags, span_status);
            assert!(tags.is_empty());
        }
        {
            let mut tags = Vec::new();
            let span_status = QwSpanStatus {
                code: OtlpStatusCode::Ok,
                message: None,
            };
            inject_span_status_tags(&mut tags, span_status);
            assert_eq!(tags.len(), 1);
            assert_eq!(tags[0].key, "otel.status_code");
            assert_eq!(tags[0].v_type(), ValueType::String);
            assert_eq!(tags[0].v_str, "OK");
        }
        {
            let mut tags = Vec::new();
            let span_status = QwSpanStatus {
                code: OtlpStatusCode::Error,
                message: Some("An error occurred.".to_string()),
            };
            inject_span_status_tags(&mut tags, span_status);
            assert_eq!(tags.len(), 3);

            assert_eq!(tags[0].key, "otel.status_code");
            assert_eq!(tags[0].v_type(), ValueType::String);
            assert_eq!(tags[0].v_str, "ERROR");

            assert_eq!(tags[1].key, "otel.status_description");
            assert_eq!(tags[1].v_type(), ValueType::String);
            assert_eq!(tags[1].v_str, "An error occurred.");

            assert_eq!(tags[2].key, "error");
            assert_eq!(tags[2].v_type(), ValueType::Bool);
            assert!(tags[2].v_bool);
        }
    }

    #[test]
    fn test_qw_event_to_jaeger_logs() {
        {
            let event = QwEvent {
                event_timestamp_nanos: 1_000_000_001,
                event_name: "".to_string(),
                event_attributes: HashMap::from_iter([("foo".to_string(), json!("bar"))]),
                event_dropped_attributes_count: 0,
            };
            let log = qw_event_to_jaeger_log(event).unwrap();
            assert_eq!(
                log.timestamp.unwrap(),
                to_well_known_timestamp(1_000_000_001)
            );
            assert_eq!(log.fields.len(), 1);

            assert_eq!(log.fields[0].key, "foo");
            assert_eq!(log.fields[0].v_type(), ValueType::String);
            assert_eq!(log.fields[0].v_str, "bar");
        }
        {
            let event = QwEvent {
                event_timestamp_nanos: 1_000_000_001,
                event_name: "Failed to ...".to_string(),
                event_attributes: HashMap::from_iter([("foo".to_string(), json!("bar"))]),
                event_dropped_attributes_count: 1,
            };
            let log = qw_event_to_jaeger_log(event).unwrap();
            assert_eq!(log.fields.len(), 3);

            assert_eq!(log.fields[0].key, "foo");
            assert_eq!(log.fields[0].v_type(), ValueType::String);
            assert_eq!(log.fields[0].v_str, "bar");

            assert_eq!(log.fields[1].key, "event");
            assert_eq!(log.fields[1].v_type(), ValueType::String);
            assert_eq!(log.fields[1].v_str, "Failed to ...");

            assert_eq!(log.fields[2].key, "otel.dropped_attributes_count");
            assert_eq!(log.fields[2].v_type(), ValueType::Int64);
            assert_eq!(log.fields[2].v_int64, 1);
        }
        {
            let event = QwEvent {
                event_timestamp_nanos: 1_000_000_001,
                event_name: "Failed to ...".to_string(),
                event_attributes: HashMap::from_iter([("event".to_string(), json!("foo"))]),
                event_dropped_attributes_count: 0,
            };
            let log = qw_event_to_jaeger_log(event).unwrap();
            assert_eq!(log.fields.len(), 1);
            assert_eq!(log.fields[0].key, "event");
            assert_eq!(log.fields[0].v_type(), ValueType::String);
            assert_eq!(log.fields[0].v_str, "foo");
        }
    }

    #[test]
    fn test_qw_span_to_jaeger_span() {
        let qw_span = QwSpan {
            trace_id: TraceId::new([1; 16]),
            trace_state: Some("key1=value1,key2=value2".to_string()),
            service_name: "quickwit".to_string(),
            resource_attributes: HashMap::from_iter([(
                "resource_key".to_string(),
                json!("resource_value"),
            )]),
            resource_dropped_attributes_count: 1,
            scope_name: Some("vector.dev".to_string()),
            scope_version: Some("1.0.0".to_string()),
            scope_attributes: HashMap::from_iter([("scope_key".to_string(), json!("scope_value"))]),
            scope_dropped_attributes_count: 2,
            span_id: SpanId::new([2; 8]),
            span_kind: 2,
            span_name: "publish_split".to_string(),
            span_fingerprint: Some(SpanFingerprint::new("quickwit", 2.into(), "publish_split")),
            span_start_timestamp_nanos: 1_000_000_001,
            span_end_timestamp_nanos: 2_000_000_002,
            span_duration_millis: Some(1_001),
            span_attributes: HashMap::from_iter([("span_key".to_string(), json!("span_value"))]),
            span_dropped_attributes_count: 3,
            span_dropped_events_count: 4,
            span_dropped_links_count: 5,
            span_status: QwSpanStatus {
                code: OtlpStatusCode::Error,
                message: Some("An error occurred.".to_string()),
            },
            parent_span_id: Some(SpanId::new([3; 8])),
            is_root: Some(false),
            events: vec![QwEvent {
                event_timestamp_nanos: 1000500003,
                event_name: "event_name".to_string(),
                event_attributes: HashMap::from_iter([(
                    "event_key".to_string(),
                    json!("event_value"),
                )]),
                event_dropped_attributes_count: 6,
            }],
            event_names: vec!["event_name".to_string()],
            links: vec![QwLink {
                link_trace_id: TraceId::new([4; 16]),
                link_trace_state: Some("link_key1=link_value1,link_key2=link_value2".to_string()),
                link_span_id: SpanId::new([5; 8]),
                link_attributes: HashMap::from_iter([(
                    "link_key".to_string(),
                    json!("link_value"),
                )]),
                link_dropped_attributes_count: 7,
            }],
        };
        let qw_span_json = serde_json::to_string(&qw_span).unwrap();
        let jaeger_span = qw_span_to_jaeger_span(&qw_span_json).unwrap();
        assert_eq!(jaeger_span.trace_id, [1; 16]);
        assert_eq!(jaeger_span.span_id, [2; 8]);
        assert_eq!(jaeger_span.operation_name, "publish_split");
        assert_eq!(
            jaeger_span.references,
            vec![
                JaegerSpanRef {
                    trace_id: vec![1; 16],
                    span_id: vec![3; 8],
                    ref_type: 0,
                },
                JaegerSpanRef {
                    trace_id: vec![4; 16],
                    span_id: vec![5; 8],
                    ref_type: 1,
                }
            ]
        );
        assert_eq!(jaeger_span.flags, 0);
        assert_eq!(
            jaeger_span.start_time.unwrap(),
            WellKnownTimestamp {
                seconds: 1,
                nanos: 1,
            }
        );
        assert_eq!(
            jaeger_span.duration.unwrap(),
            WellKnownDuration {
                seconds: 1,
                nanos: 1,
            }
        );
        assert_eq!(
            jaeger_span.tags,
            vec![
                JaegerKeyValue {
                    key: "span_key".to_string(),
                    v_type: 0,
                    v_str: "span_value".to_string(),
                    v_bool: false,
                    v_int64: 0,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
                JaegerKeyValue {
                    key: "otel.dropped_attributes_count".to_string(),
                    v_type: 2,
                    v_str: String::new(),
                    v_bool: false,
                    v_int64: 3,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
                JaegerKeyValue {
                    key: "otel.dropped_events_count".to_string(),
                    v_type: 2,
                    v_str: String::new(),
                    v_bool: false,
                    v_int64: 4,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
                JaegerKeyValue {
                    key: "otel.dropped_links_count".to_string(),
                    v_type: 2,
                    v_str: String::new(),
                    v_bool: false,
                    v_int64: 5,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
                JaegerKeyValue {
                    key: "span.kind".to_string(),
                    v_type: 0,
                    v_str: "server".to_string(),
                    v_bool: false,
                    v_int64: 0,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
                JaegerKeyValue {
                    key: "otel.status_code".to_string(),
                    v_type: 0,
                    v_str: "ERROR".to_string(),
                    v_bool: false,
                    v_int64: 0,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
                JaegerKeyValue {
                    key: "otel.status_description".to_string(),
                    v_type: 0,
                    v_str: "An error occurred.".to_string(),
                    v_bool: false,
                    v_int64: 0,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
                JaegerKeyValue {
                    key: "error".to_string(),
                    v_type: 1,
                    v_str: String::new(),
                    v_bool: true,
                    v_int64: 0,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                },
            ]
        );
        assert_eq!(
            jaeger_span.logs,
            vec![JaegerLog {
                timestamp: Some(WellKnownTimestamp {
                    seconds: 1,
                    nanos: 500003,
                }),
                fields: vec![
                    JaegerKeyValue {
                        key: "event_key".to_string(),
                        v_type: 0,
                        v_str: "event_value".to_string(),
                        v_bool: false,
                        v_int64: 0,
                        v_float64: 0.0,
                        v_binary: Vec::new()
                    },
                    JaegerKeyValue {
                        key: "event".to_string(),
                        v_type: 0,
                        v_str: "event_name".to_string(),
                        v_bool: false,
                        v_int64: 0,
                        v_float64: 0.0,
                        v_binary: Vec::new()
                    },
                    JaegerKeyValue {
                        key: "otel.dropped_attributes_count".to_string(),
                        v_type: 2,
                        v_str: String::new(),
                        v_bool: false,
                        v_int64: 6,
                        v_float64: 0.0,
                        v_binary: Vec::new()
                    },
                ],
            }]
        );
        assert_eq!(
            jaeger_span.process.unwrap(),
            JaegerProcess {
                service_name: "quickwit".to_string(),
                tags: vec![JaegerKeyValue {
                    key: "resource_key".to_string(),
                    v_type: 0,
                    v_str: "resource_value".to_string(),
                    v_bool: false,
                    v_int64: 0,
                    v_float64: 0.0,
                    v_binary: Vec::new()
                }]
            }
        );
        assert!(jaeger_span.warnings.is_empty());
    }

    #[test]
    fn test_otlp_links_to_jaeger_references() {
        let trace_id = TraceId::new([1; 16]);
        let parent_span_id = SpanId::new([3; 8]);
        let links = vec![QwLink {
            link_trace_id: TraceId::new([4; 16]),
            link_trace_state: Some("link_key1=link_value1,link_key2=link_value2".to_string()),
            link_span_id: SpanId::new([5; 8]),
            link_attributes: HashMap::from_iter([("link_key".to_string(), json!("link_value"))]),
            link_dropped_attributes_count: 7,
        }];
        let jaeger_references =
            otlp_links_to_jaeger_references(&trace_id, Some(parent_span_id), links).unwrap();
        assert_eq!(
            jaeger_references,
            vec![
                JaegerSpanRef {
                    trace_id: vec![1; 16],
                    span_id: vec![3; 8],
                    ref_type: 0,
                },
                JaegerSpanRef {
                    trace_id: vec![4; 16],
                    span_id: vec![5; 8],
                    ref_type: 1,
                }
            ]
        );
    }

    #[test]
    fn test_collect_trace_ids() {
        use quickwit_opentelemetry::otlp::TraceId;
        use quickwit_search::Span;
        use tantivy::DateTime;
        {
            let agg_result: Vec<Span> = Vec::new();
            let agg_result_postcard = postcard::to_stdvec(&agg_result).unwrap();
            let (trace_ids, _span_timestamps_range) =
                collect_trace_ids(&agg_result_postcard).unwrap();
            assert!(trace_ids.is_empty());
        }
        {
            let agg_result = vec![Span {
                trace_id: TraceId::new([
                    0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
                    0x01, 0x01, 0x01,
                ]),
                span_timestamp: DateTime::from_timestamp_nanos(1684857492783747000),
            }];
            let agg_result_postcard = postcard::to_stdvec(&agg_result).unwrap();
            let (trace_ids, span_timestamps_range) =
                collect_trace_ids(&agg_result_postcard).unwrap();
            assert_eq!(trace_ids.len(), 1);
            assert_eq!(span_timestamps_range, 1684857492..=1684857492);
        }
        {
            let agg_result = vec![
                Span {
                    trace_id: TraceId::new([
                        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                        0x0d, 0x0e, 0x0f, 0x10,
                    ]),
                    span_timestamp: DateTime::from_timestamp_nanos(1684857492783747000),
                },
                Span {
                    trace_id: TraceId::new([
                        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
                        0x02, 0x02, 0x02, 0x02,
                    ]),
                    span_timestamp: DateTime::from_timestamp_nanos(1684857826019627000),
                },
            ];
            let agg_result_postcard = postcard::to_stdvec(&agg_result).unwrap();
            let (trace_ids, span_timestamps_range) =
                collect_trace_ids(&agg_result_postcard).unwrap();
            assert_eq!(trace_ids.len(), 2);
            assert_eq!(span_timestamps_range, 1684857492..=1684857826);
        }
    }

    #[tokio::test]
    async fn test_get_services() {
        let mut service = MockSearchService::new();
        service
            .expect_root_list_terms()
            .withf(|req| {
                req.index_id_patterns == vec![OTEL_TRACES_INDEX_ID_PATTERN]
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

        let service = Arc::new(service);
        let jaeger = JaegerService::new(JaegerConfig::default(), service);

        let request = tonic::Request::new(GetServicesRequest {});
        let response = jaeger.get_services(request).await.unwrap().into_inner();
        assert_eq!(response.services, &["service1", "service2", "service3"]);
    }

    #[tokio::test]
    async fn test_get_services_on_custom_indexes() {
        let mut service = MockSearchService::new();
        service
            .expect_root_list_terms()
            .withf(|req| {
                req.index_id_patterns == vec!["index-1", "index-3*"]
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

        let service = Arc::new(service);
        let jaeger = JaegerService::new(JaegerConfig::default(), service);

        let mut request = tonic::Request::new(GetServicesRequest {});
        request.metadata_mut().insert(
            OtelSignal::Traces.header_name(),
            "index-1,index-3*".parse().unwrap(),
        );
        let response = jaeger.get_services(request).await.unwrap().into_inner();
        assert_eq!(response.services, &["service1", "service2", "service3"]);
    }
}
