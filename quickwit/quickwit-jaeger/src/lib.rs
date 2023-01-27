// Copyright (C) 2022 Quickwit, Inc.
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
use std::fmt::Write;
use std::mem;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use base64::prelude::{Engine, BASE64_STANDARD};
use itertools::Itertools;
use prost::Message;
use prost_types::{Duration as WellKnownDuration, Timestamp as WellKnownTimestamp};
use quickwit_config::JaegerConfig;
use quickwit_opentelemetry::otlp::{
    Event as QwEvent, Link as QwLink, Span as QwSpan, SpanStatus as QwSpanStatus,
    OTEL_TRACE_INDEX_ID,
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
use quickwit_proto::SearchRequest;
use quickwit_search::SearchService;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{debug, error, instrument, warn, Span as RuntimeSpan};

use crate::metrics::JAEGER_SERVICE_METRICS;

mod metrics;

// OpenTelemetry to Jaeger Transformation
// <https://opentelemetry.io/docs/reference/specification/trace/sdk_exporters/jaeger/>

const TRACE_INDEX_ID: &str = "otel-trace-v0";

/// A base64-encoded 16-byte array.
type TraceId = String;

type TimeIntervalSecs = RangeInclusive<i64>;

type JaegerResult<T> = Result<T, Status>;

type SpanStream = ReceiverStream<Result<SpansResponseChunk, Status>>;

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
    async fn get_services_inner(
        &self,
        request: GetServicesRequest,
    ) -> JaegerResult<GetServicesResponse> {
        debug!(request=?request, "`get_services` request");

        let index_id = TRACE_INDEX_ID.to_string();
        let query = "*".to_string();
        let max_hits = 1_000;
        let start_timestamp =
            Some(OffsetDateTime::now_utc().unix_timestamp() - self.lookback_period_secs);

        let search_request = SearchRequest {
            index_id,
            query,
            max_hits,
            start_timestamp,
            end_timestamp: None,
            search_fields: Vec::new(),
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self.search_service.root_search(search_request).await?;
        let services: Vec<String> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen!")
            })
            .flat_map(extract_service_name)
            .sorted()
            .dedup()
            .collect();
        debug!(services=?services, "`get_services` response");
        let response = GetServicesResponse { services };
        Ok(response)
    }

    #[instrument("get_operations", skip_all, fields(service=%request.service, span_kind=%request.span_kind))]
    async fn get_operations_inner(
        &self,
        request: GetOperationsRequest,
    ) -> JaegerResult<GetOperationsResponse> {
        debug!(request=?request, "`get_operations` request");

        let current_span = RuntimeSpan::current();
        current_span.record("service", &request.service);

        let index_id = TRACE_INDEX_ID.to_string();
        let query = build_search_query(
            &request.service,
            &request.span_kind,
            "",
            HashMap::new(),
            None,
            None,
            None,
            None,
        );
        let max_hits = 1_000;
        let start_timestamp =
            Some(OffsetDateTime::now_utc().unix_timestamp() - self.lookback_period_secs);

        let search_request = SearchRequest {
            index_id,
            query,
            max_hits,
            start_timestamp,
            end_timestamp: None,
            search_fields: Vec::new(),
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self.search_service.root_search(search_request).await?;
        let operations: Vec<Operation> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen!")
            })
            .flat_map(extract_operation)
            .sorted()
            .dedup()
            .collect();
        debug!(operations=?operations, "`get_operations` response");
        let response = GetOperationsResponse {
            operations,
            operation_names: Vec::new(), // `operation_names` is deprecated.
        };
        Ok(response)
    }

    // Instrumentation happens in `find_trace_ids`.
    async fn find_trace_ids_inner(
        &self,
        request: FindTraceIDsRequest,
    ) -> JaegerResult<FindTraceIDsResponse> {
        debug!(request=?request, "`find_trace_ids` request");

        let trace_query = request
            .query
            .ok_or_else(|| Status::invalid_argument("Query is empty."))?;

        let (trace_ids_b64, _) = self.find_trace_ids(trace_query).await?;
        debug!(trace_ids=?trace_ids_b64, "`find_trace_ids` response");

        let trace_ids = trace_ids_b64
            .into_iter()
            .map(|trace_id_b64| base64_decode(trace_id_b64.as_bytes(), "trace ID"))
            .collect::<Result<_, _>>()?;
        let response = FindTraceIDsResponse { trace_ids };
        Ok(response)
    }

    #[instrument("find_traces", skip_all)]
    async fn find_traces_inner(
        &self,
        request: FindTracesRequest,
        operation_name: &'static str,
        request_start: Instant,
    ) -> JaegerResult<SpanStream> {
        debug!(request=?request, "`find_traces` request");

        let trace_query = request
            .query
            .ok_or_else(|| Status::invalid_argument("Trace query is empty."))?;
        let (trace_ids, span_timestamps_range) = self.find_trace_ids(trace_query).await?;
        let start = span_timestamps_range.start() - self.max_trace_duration_secs;
        let end = span_timestamps_range.end() + self.max_trace_duration_secs;
        let search_window = start..=end;
        let response = self
            .stream_spans(&trace_ids, search_window, operation_name, request_start)
            .await?;
        Ok(response)
    }

    #[instrument("find_traces", skip_all)]
    async fn get_trace_inner(
        &self,
        request: GetTraceRequest,
        operation_name: &'static str,
        request_start: Instant,
    ) -> JaegerResult<SpanStream> {
        debug!(request=?request, "`get_trace` request");
        let trace_id = BASE64_STANDARD.encode(request.trace_id);
        let end = OffsetDateTime::now_utc().unix_timestamp();
        let start = end - self.lookback_period_secs;
        let search_window = start..=end;
        let response = self
            .stream_spans(&[trace_id], search_window, operation_name, request_start)
            .await?;
        Ok(response)
    }

    #[instrument("find_trace_ids", skip_all fields(service_name=%trace_query.service_name, operation_name=%trace_query.operation_name))]
    async fn find_trace_ids(
        &self,
        trace_query: TraceQueryParameters,
    ) -> Result<(Vec<TraceId>, TimeIntervalSecs), Status> {
        let index_id = TRACE_INDEX_ID.to_string();
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
            "",
            &trace_query.operation_name,
            trace_query.tags,
            min_span_start_timestamp_secs_opt,
            max_span_start_timestamp_secs_opt,
            min_span_duration_millis_opt,
            max_span_duration_millis_opt,
        );
        let aggregation_query = build_aggregations_query(trace_query.num_traces as usize);
        let max_hits = 0;
        let search_request = SearchRequest {
            index_id,
            query,
            aggregation_request: Some(aggregation_query),
            max_hits,
            start_timestamp: min_span_start_timestamp_secs_opt,
            end_timestamp: max_span_start_timestamp_secs_opt,
            search_fields: Vec::new(),
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self.search_service.root_search(search_request).await?;

        let Some(agg_result_json) = search_response.aggregation else {
            debug!("The query matched no traces.");
            return Ok((Vec::new(), 0..=0));
        };
        let trace_ids = collect_trace_ids(&agg_result_json)?;
        debug!(trace_ids=?trace_ids.0, "The query matched {} traces.", trace_ids.0.len());

        Ok(trace_ids)
    }

    #[instrument("stream_spans", skip_all, fields(num_traces=%trace_ids.len(), num_spans=Empty, num_bytes=Empty))]
    async fn stream_spans(
        &self,
        trace_ids: &[TraceId],
        search_window: TimeIntervalSecs,
        operation_name: &'static str,
        request_start: Instant,
    ) -> Result<SpanStream, Status> {
        if trace_ids.is_empty() {
            let (_tx, rx) = mpsc::channel(1);
            return Ok(ReceiverStream::new(rx));
        }
        let num_traces = trace_ids.len() as u64;
        let mut query = String::new();

        for (i, trace_id) in trace_ids.iter().enumerate() {
            if i > 0 {
                query.push_str(" OR ");
            }
            query.push_str("trace_id:");
            query.push_str(trace_id);
        }
        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query,
            search_fields: Vec::new(),
            start_timestamp: Some(*search_window.start()),
            end_timestamp: Some(*search_window.end()),
            max_hits: self.max_fetch_spans,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = match self.search_service.root_search(search_request).await {
            Ok(search_response) => search_response,
            Err(search_error) => {
                error!("Failed to fetch spans: {search_error:?}");
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
            const CHUNK_SIZE: usize = 1_000;

            let chunk_size = spans.len().min(CHUNK_SIZE);
            let mut chunk = Vec::with_capacity(chunk_size);
            let mut chunk_num_bytes = 0;
            let mut num_spans_total = 0;
            let mut num_bytes_total = 0;

            while let Some(span) = spans.pop() {
                chunk_num_bytes += span.encoded_len();
                chunk.push(span);

                if chunk.len() == CHUNK_SIZE {
                    num_spans_total += chunk.len();
                    num_bytes_total += chunk_num_bytes;

                    let chunk_size = spans.len().min(CHUNK_SIZE);
                    let chunk = mem::replace(&mut chunk, Vec::with_capacity(chunk_size));
                    if let Err(send_error) = tx.send(Ok(SpansResponseChunk { spans: chunk })).await
                    {
                        debug!("Client disconnected: {send_error:?}");
                        return;
                    }
                    record_send(operation_name, CHUNK_SIZE, chunk_num_bytes);
                    chunk_num_bytes = 0;
                }
            }
            if !chunk.is_empty() {
                let num_spans = chunk.len();
                num_spans_total += num_spans;
                num_bytes_total += chunk_num_bytes;

                if let Err(send_error) = tx.send(Ok(SpansResponseChunk { spans: chunk })).await {
                    debug!("Client disconnected: {send_error:?}");
                    return;
                }
                record_send(operation_name, num_spans, chunk_num_bytes);
            }
            current_span.record("num_spans", num_spans_total);
            current_span.record("num_bytes", num_bytes_total);

            JAEGER_SERVICE_METRICS
                .fetched_traces_total
                .with_label_values([operation_name, OTEL_TRACE_INDEX_ID])
                .inc_by(num_traces);

            let elapsed = request_start.elapsed().as_secs_f64();
            JAEGER_SERVICE_METRICS
                .request_duration_seconds
                .with_label_values([operation_name, OTEL_TRACE_INDEX_ID, "false"])
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
        .with_label_values([operation_name, OTEL_TRACE_INDEX_ID])
        .inc();

    let elapsed = request_start.elapsed().as_secs_f64();
    JAEGER_SERVICE_METRICS
        .request_duration_seconds
        .with_label_values([operation_name, OTEL_TRACE_INDEX_ID, "true"])
        .observe(elapsed);
}

fn record_send(operation_name: &'static str, num_spans: usize, num_bytes: usize) {
    JAEGER_SERVICE_METRICS
        .fetched_spans_total
        .with_label_values([operation_name, OTEL_TRACE_INDEX_ID])
        .inc_by(num_spans as u64);
    JAEGER_SERVICE_METRICS
        .transferred_bytes_total
        .with_label_values([operation_name, OTEL_TRACE_INDEX_ID])
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
        metrics!(
            self.get_services_inner(request.into_inner()).await,
            [get_services, OTEL_TRACE_INDEX_ID]
        );
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        metrics!(
            self.get_operations_inner(request.into_inner()).await,
            [get_operations, OTEL_TRACE_INDEX_ID]
        );
    }

    async fn find_trace_i_ds(
        &self,
        request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        metrics!(
            self.find_trace_ids_inner(request.into_inner()).await,
            [find_trace_ids, OTEL_TRACE_INDEX_ID]
        );
    }

    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        self.find_traces_inner(request.into_inner(), "find_traces", Instant::now())
            .await
            .map(Response::new)
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<Self::GetTraceStream>, Status> {
        self.get_trace_inner(request.into_inner(), "get_trace", Instant::now())
            .await
            .map(Response::new)
    }
}

fn extract_service_name(mut doc: JsonValue) -> Option<String> {
    match doc["service_name"].take() {
        JsonValue::String(service_name) => Some(service_name),
        _ => None,
    }
}

fn extract_operation(mut doc: JsonValue) -> Option<Operation> {
    match (doc["span_name"].take(), doc["span_kind"].take()) {
        (JsonValue::String(span_name), JsonValue::Number(span_kind_number)) => {
            let span_kind_id = span_kind_number
                .as_u64()
                .expect("Span kind should be stored as u64.");
            let span_kind = to_span_kind(span_kind_id).to_string();
            Some(Operation {
                name: span_name,
                span_kind,
            })
        }
        _ => None,
    }
}

// TODO: builder pattern + query DSL
#[allow(clippy::too_many_arguments)]
fn build_search_query(
    service_name: &str,
    span_kind: &str,
    span_name: &str,
    mut tags: HashMap<String, String>,
    min_span_start_timestamp_secs_opt: Option<i64>,
    max_span_start_timestamp_secs_opt: Option<i64>,
    min_span_duration_millis_opt: Option<i64>,
    max_span_duration_millis_opt: Option<i64>,
) -> String {
    if let Some(qw_query) = tags.remove("_qw_query") {
        return qw_query;
    }
    let mut query = String::new();

    if !service_name.is_empty() {
        query.push_str("service_name:");
        query.push_str(service_name);
    }
    if !span_kind.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_kind:");
        query.push_str(to_span_kind_id(span_kind));
    }
    if !span_name.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_name:");
        query.push_str(span_name);
    }
    if !tags.is_empty() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        // Sort the tags for deterministic tests.
        for (i, (key, value)) in tags.iter().sorted().enumerate() {
            if i > 0 {
                query.push_str(" AND ");
            }
            // In Jaeger land, `event` is a regular event attribute whereas in OpenTelemetry land,
            // it is an event top-level field named `name`. In Quickwit, it is stored as
            // `event_name` to distinguish it from the span top-level field `name`.
            if key == "event" {
                query.push_str("events.event_name:\"");
                query.push_str(value);
                query.push('\"');
            } else {
                query.push_str("(span_attributes.");
                query.push_str(key);
                query.push_str(":\"");
                query.push_str(value);
                query.push_str("\" OR events.event_attributes.");
                query.push_str(key);
                query.push_str(":\"");
                query.push_str(value);
                query.push_str("\")");
            }
        }
    }
    if min_span_start_timestamp_secs_opt.is_some() || max_span_start_timestamp_secs_opt.is_some() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_start_timestamp_secs:[");

        if let Some(min_span_start_timestamp_secs) = min_span_start_timestamp_secs_opt {
            let min_span_start_datetime =
                OffsetDateTime::from_unix_timestamp(min_span_start_timestamp_secs).expect("");
            let min_span_start_datetime_rfc3339 =
                min_span_start_datetime.format(&Rfc3339).expect("");
            query.push_str(&min_span_start_datetime_rfc3339);
        } else {
            query.push('*');
        }
        query.push_str(" TO ");

        if let Some(max_span_start_timestamp_secs) = max_span_start_timestamp_secs_opt {
            let max_span_start_datetime =
                OffsetDateTime::from_unix_timestamp(max_span_start_timestamp_secs).expect("");
            let max_span_start_datetime_rfc3339 =
                max_span_start_datetime.format(&Rfc3339).expect("");
            query.push_str(&max_span_start_datetime_rfc3339);
        } else {
            query.push('*');
        }
        query.push(']');
    }
    if min_span_duration_millis_opt.is_some() || max_span_duration_millis_opt.is_some() {
        if !query.is_empty() {
            query.push_str(" AND ");
        }
        query.push_str("span_duration_millis:[");

        if let Some(min_span_duration_millis) = min_span_duration_millis_opt {
            write!(query, "{}", min_span_duration_millis)
                .expect("Writing to a string should not fail.");
        } else {
            query.push('*');
        }
        query.push_str(" TO ");

        if let Some(max_span_duration_millis) = max_span_duration_millis_opt {
            write!(query, "{}", max_span_duration_millis)
                .expect("Writing to a string should not fail.");
        } else {
            query.push('*');
        }
        query.push(']');
    }
    if query.is_empty() {
        query.push('*');
    }
    debug!(query=%query, "Search query");
    query
}

fn build_aggregations_query(num_traces: usize) -> String {
    // DANGER: The fast field is truncated to seconds but the aggregation returns timestamps in
    // microseconds by appending a bunch of zeros.
    let query = format!(
        r#"{{
        "trace_ids": {{
            "terms": {{
                "field": "trace_id",
                "size": {num_traces},
                "order": {{
                    "max_span_start_timestamp_micros": "desc"
                }}
            }},
            "aggs": {{
                "max_span_start_timestamp_micros": {{
                    "max": {{
                        "field": "span_start_timestamp_secs"
                    }}
                }}
            }}
        }}
    }}"#,
    );
    debug!(query=%query, "Aggregations query");
    query
}

fn qw_span_to_jaeger_span(qw_span: &str) -> Result<JaegerSpan, Status> {
    let mut span: QwSpan = json_deserialize(qw_span, "span")?;
    let trace_id = base64_decode(span.trace_id.as_bytes(), "trace ID")?;
    let span_id = base64_decode(span.span_id.as_bytes(), "span ID")?;

    let start_time = Some(to_well_known_timestamp(span.span_start_timestamp_nanos));
    let duration = Some(to_well_known_duration(
        span.span_start_timestamp_nanos,
        span.span_end_timestamp_nanos,
    ));
    span.resource_attributes.remove("service.name");
    let process = Some(JaegerProcess {
        service_name: span.service_name,
        tags: otlp_attributes_to_jaeger_tags(span.resource_attributes)?,
    });
    let logs: Vec<JaegerLog> = span
        .events
        .into_iter()
        .map(qw_event_to_jaeger_log)
        .collect::<Result<_, _>>()?;

    // From <https://opentelemetry.io/docs/reference/specification/trace/sdk_exporters/jaeger/#spankind>
    let mut tags = otlp_attributes_to_jaeger_tags(span.span_attributes)?;
    inject_dropped_count_tags(
        &mut tags,
        span.span_dropped_attributes_count,
        span.span_dropped_events_count,
        span.span_dropped_links_count,
    );
    inject_span_kind_tag(&mut tags, span.span_kind);
    inject_span_status_tags(&mut tags, span.span_status);

    let references = otlp_links_to_jaeger_references(&trace_id, span.parent_span_id, span.links)?;

    let span = JaegerSpan {
        trace_id,
        span_id,
        operation_name: span.span_name,
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
    dropped_attributes_count: u64,
    dropped_events_count: u64,
    dropped_links_count: u64,
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

fn inject_span_kind_tag(tags: &mut Vec<JaegerKeyValue>, span_kind_id: u64) {
    // OpenTelemetry SpanKind field MUST be encoded as span.kind tag in Jaeger span, except for
    // SpanKind.INTERNAL, which SHOULD NOT be translated to a tag.
    let span_kind = match span_kind_id {
        0 | 1 => return,
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => {
            warn!("Unknown span kind ID: `{span_kind_id}`.");
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

fn inject_span_status_tags(tags: &mut Vec<JaegerKeyValue>, span_status_opt: Option<QwSpanStatus>) {
    // Span Status MUST be reported as key-value pairs associated with the Span, unless the Status
    // is UNSET. In the latter case it MUST NOT be reported.
    if let Some(span_status) = span_status_opt {
        // Description of the Status if it has a value otherwise not set.
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
        // Name of the code, either OK or ERROR. MUST NOT be set if the code is UNSET.
        let status_code = match span_status.code {
            0 => return,
            1 => "OK",
            2 => "ERROR",
            _ => {
                warn!(status_code=%span_status.code, "Unknown span status code.");
                return;
            }
        };
        tags.push(JaegerKeyValue {
            key: "otel.status_code".to_string(),
            v_type: ValueType::String as i32,
            v_str: status_code.to_string(),
            v_bool: false,
            v_int64: 0,
            v_float64: 0.0,
            v_binary: Vec::new(),
        });
        // "When Span Status is set to ERROR, an error span tag MUST be added with the Boolean value
        // of true. The added error tag MAY override any previous value."
        if status_code == "ERROR" {
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
    }
}

fn otlp_attributes_to_jaeger_tags(
    attributes: HashMap<String, JsonValue>,
) -> Result<Vec<JaegerKeyValue>, Status> {
    let mut tags = Vec::with_capacity(attributes.len());
    for (key, value) in attributes {
        let mut tag = JaegerKeyValue {
            key,
            v_type: ValueType::String as i32,
            v_str: String::new(),
            v_bool: false,
            v_int64: 0,
            v_float64: 0.0,
            v_binary: Vec::new(),
        };
        match value {
            JsonValue::String(value) => tag.v_str = value,
            JsonValue::Bool(value) => {
                tag.v_type = ValueType::Bool as i32;
                tag.v_bool = value;
            }
            JsonValue::Number(number) => {
                if let Some(value) = number.as_i64() {
                    tag.v_type = ValueType::Int64 as i32;
                    tag.v_int64 = value;
                } else if let Some(value) = number.as_f64() {
                    tag.v_type = ValueType::Float64 as i32;
                    tag.v_float64 = value
                }
            }
            _ => {
                return Err(Status::internal(format!(
                    "Failed to serialize attributes: unexpected type `{value:?}`"
                )))
            }
        };
        tags.push(tag);
    }
    Ok(tags)
}

fn otlp_links_to_jaeger_references(
    trace_id: &[u8],
    parent_span_id_opt: Option<String>,
    links: Vec<QwLink>,
) -> Result<Vec<JaegerSpanRef>, Status> {
    let mut references = Vec::with_capacity(parent_span_id_opt.is_some() as usize + links.len());

    if let Some(parent_span_id) = parent_span_id_opt {
        let parent_span_id = base64_decode(parent_span_id.as_bytes(), "parent span ID")?;
        let reference = JaegerSpanRef {
            trace_id: trace_id.to_vec(),
            span_id: parent_span_id,
            ref_type: JaegerSpanRefType::ChildOf as i32,
        };
        references.push(reference);
    }
    // "Span references generated from Link(s) MUST be added after the span reference generated from
    // Parent ID, if any."
    for link in links {
        let trace_id = base64_decode(link.link_trace_id.as_bytes(), "link trace ID")?;
        let span_id = base64_decode(link.link_span_id.as_bytes(), "link span ID")?;
        let reference = JaegerSpanRef {
            trace_id,
            span_id,
            ref_type: JaegerSpanRefType::FollowsFrom as i32,
        };
        references.push(reference);
    }
    Ok(references)
}

fn to_span_kind(span_kind_id: u64) -> &'static str {
    match span_kind_id {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}

fn to_span_kind_id(span_kind: &str) -> &'static str {
    match span_kind {
        "0" | "unspecified" => "0",
        "1" | "internal" => "1",
        "2" | "server" => "2",
        "3" | "client" => "3",
        "4" | "producer" => "4",
        "5" | "consumer" => "5",
        _ => {
            warn!("Unknown span kind `{span_kind}`.");
            "*"
        }
    }
}

fn qw_event_to_jaeger_log(event: QwEvent) -> Result<JaegerLog, Status> {
    let timestamp = to_well_known_timestamp(event.event_timestamp_nanos);
    // "OpenTelemetry Event’s name field should be added to Jaeger Log’s fields map as follows: name
    // -> event. If OpenTelemetry Event contains an attribute with the key event, it should take
    // precedence over Event’s name field."
    let insert_event_name =
        !event.event_name.is_empty() && !event.event_attributes.contains_key("event");

    let mut fields = otlp_attributes_to_jaeger_tags(event.event_attributes)?;

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

#[derive(Deserialize)]
struct TraceIdsAggResult {
    trace_ids: TraceIdBuckets,
}

#[derive(Deserialize)]
struct TraceIdBuckets {
    #[serde(default)]
    buckets: Vec<TraceIdBucket>,
}

#[derive(Deserialize)]
struct TraceIdBucket {
    key: String,
    max_span_start_timestamp_micros: MetricValue,
}

#[derive(Deserialize)]
struct MetricValue {
    value: f64,
}

fn collect_trace_ids(agg_result_json: &str) -> Result<(Vec<TraceId>, TimeIntervalSecs), Status> {
    let agg_result: TraceIdsAggResult = json_deserialize(agg_result_json, "trace IDs aggregation")?;
    if agg_result.trace_ids.buckets.is_empty() {
        return Ok((Vec::new(), 0..=0));
    }
    let mut trace_ids = Vec::with_capacity(agg_result.trace_ids.buckets.len());
    let mut start = i64::MAX;
    let mut end = i64::MIN;

    for bucket in agg_result.trace_ids.buckets {
        trace_ids.push(bucket.key);
        start = start.min(bucket.max_span_start_timestamp_micros.value as i64);
        end = end.max(bucket.max_span_start_timestamp_micros.value as i64);
    }
    let start = start / 1_000_000;
    let end = end / 1_000_000;
    Ok((trace_ids, start..=end))
}

fn base64_decode(encoded: &[u8], label: &'static str) -> Result<Vec<u8>, Status> {
    match BASE64_STANDARD.decode(encoded) {
        Ok(decoded) => Ok(decoded),
        Err(error) => {
            error!("Failed to base64 decode {label}: {error:?}",);
            Err(Status::internal(format!(
                "Failed to base64 decode: {error:?}"
            )))
        }
    }
}

fn json_deserialize<'a, T>(json: &'a str, label: &'static str) -> Result<T, Status>
where T: Deserialize<'a> {
    match serde_json::from_str(json) {
        Ok(deserialized) => Ok(deserialized),
        Err(error) => {
            error!("Failed to deserialize {label}: {error:?}",);
            Err(Status::internal(format!("Failed to deserialize {json}.")))
        }
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::jaeger::api_v2::ValueType;
    use serde_json::json;
    use tantivy::aggregation::agg_req::{
        Aggregation, Aggregations, BucketAggregationType, MetricAggregation,
    };

    use super::*;

    #[test]
    fn test_build_query() {
        {
            let service_name = "";
            let span_kind = "";
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
                "*"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
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
                "service_name:quickwit"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
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
                "query"
            );
        }
        {
            let service_name = "";
            let span_kind = "client";
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
                "span_kind:3"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "leaf_search";
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
                "span_name:leaf_search"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar baz".to_string())]);
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
                r#"(span_attributes.foo:"bar baz" OR events.event_attributes.foo:"bar baz")"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("event".to_string(), "Failed to ...".to_string())]);
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
                r#"events.event_name:"Failed to ...""#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([
                ("event".to_string(), "Failed to ...".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]);
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
                r#"events.event_name:"Failed to ..." AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
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
                r#"(span_attributes.baz:"qux" OR events.event_attributes.baz:"qux") AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = Some(3);
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
                r#"span_start_timestamp_secs:[1970-01-01T00:00:03Z TO *]"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = Some(33);
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
                r#"span_start_timestamp_secs:[* TO 1970-01-01T00:00:33Z]"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = Some(3);
            let max_span_start_timestamp_secs = Some(33);
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
                r#"span_start_timestamp_secs:[1970-01-01T00:00:03Z TO 1970-01-01T00:00:33Z]"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = Some(7);
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
                r#"span_duration_millis:[7 TO *]"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = None;
            let max_span_duration_secs = Some(77);
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
                r#"span_duration_millis:[* TO 77]"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            let min_span_start_timestamp_secs = None;
            let max_span_start_timestamp_secs = None;
            let min_span_duration_secs = Some(7);
            let max_span_duration_secs = Some(77);
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
                r#"span_duration_millis:[7 TO 77]"#
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
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
                r#"service_name:quickwit AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
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
                r#"service_name:quickwit AND span_kind:3 AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "leaf_search";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
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
                r#"service_name:quickwit AND span_kind:3 AND span_name:leaf_search AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "leaf_search";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
            let min_span_start_timestamp_secs = Some(3);
            let max_span_start_timestamp_secs = Some(33);
            let min_span_duration_secs = Some(7);
            let max_span_duration_secs = Some(77);
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
                r#"service_name:quickwit AND span_kind:3 AND span_name:leaf_search AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar") AND span_start_timestamp_secs:[1970-01-01T00:00:03Z TO 1970-01-01T00:00:33Z] AND span_duration_millis:[7 TO 77]"#
            );
        }
    }

    #[test]
    fn test_build_aggregations_query() {
        let aggregations_query = build_aggregations_query(77);
        let aggregations: Aggregations = serde_json::from_str(&aggregations_query).unwrap();
        let aggregation = aggregations.get("trace_ids").unwrap();
        let Aggregation::Bucket(ref bucket_aggregation) = aggregation else {
            panic!("Expected a bucket aggregation!");
        };
        let BucketAggregationType::Terms(ref terms_aggregation) = bucket_aggregation.bucket_agg else {
            panic!("Expected a terms aggregation!");
        };
        assert_eq!(terms_aggregation.field, "trace_id");
        assert_eq!(terms_aggregation.size.unwrap(), 77);

        let Aggregation::Metric(MetricAggregation::Max(max_aggregation)) = bucket_aggregation.sub_aggregation.get("max_span_start_timestamp_micros").unwrap() else {
            panic!("Expected a max metric aggregation!");
        };
        assert_eq!(max_aggregation.field, "span_start_timestamp_secs");
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
        let attributes = HashMap::from_iter([
            ("bool".to_string(), json!(true)),
            ("float".to_string(), json!(1.0)),
            ("integer".to_string(), json!(1)),
            ("string".to_string(), json!("foo")),
        ]);
        let mut tags = otlp_attributes_to_jaeger_tags(attributes).unwrap();
        tags.sort_by(|left, right| left.key.cmp(&right.key));

        assert_eq!(tags.len(), 4);

        assert_eq!(tags[0].key, "bool");
        assert_eq!(tags[0].v_type(), ValueType::Bool);
        assert!(tags[0].v_bool);

        assert_eq!(tags[1].key, "float");
        assert_eq!(tags[1].v_type(), ValueType::Float64);
        assert_eq!(tags[1].v_float64, 1.0);

        assert_eq!(tags[2].key, "integer");
        assert_eq!(tags[2].v_type(), ValueType::Int64);
        assert_eq!(tags[2].v_int64, 1);

        assert_eq!(tags[3].key, "string");
        assert_eq!(tags[3].v_type(), ValueType::String);
        assert_eq!(tags[3].v_str, "foo");
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
            inject_span_status_tags(&mut tags, None);
            assert!(tags.is_empty());
        }
        {
            let mut tags = Vec::new();
            let span_status = QwSpanStatus {
                code: 0,
                message: None,
            };
            inject_span_status_tags(&mut tags, Some(span_status));
            assert!(tags.is_empty());
        }
        {
            let mut tags = Vec::new();
            let span_status = QwSpanStatus {
                code: 0,
                message: Some("foo".to_string()),
            };
            inject_span_status_tags(&mut tags, Some(span_status));
            assert_eq!(tags.len(), 1);
            assert_eq!(tags[0].key, "otel.status_description");
            assert_eq!(tags[0].v_type(), ValueType::String);
            assert_eq!(tags[0].v_str, "foo");
        }
        {
            let mut tags = Vec::new();
            let span_status = QwSpanStatus {
                code: 1,
                message: Some("Ok".to_string()),
            };
            inject_span_status_tags(&mut tags, Some(span_status));
            assert_eq!(tags.len(), 2);

            assert_eq!(tags[0].key, "otel.status_description");
            assert_eq!(tags[0].v_type(), ValueType::String);
            assert_eq!(tags[0].v_str, "Ok");

            assert_eq!(tags[1].key, "otel.status_code");
            assert_eq!(tags[1].v_type(), ValueType::String);
            assert_eq!(tags[1].v_str, "OK");
        }
        {
            let mut tags = Vec::new();
            let span_status = QwSpanStatus {
                code: 2,
                message: Some("Error".to_string()),
            };
            inject_span_status_tags(&mut tags, Some(span_status));
            assert_eq!(tags.len(), 3);

            assert_eq!(tags[0].key, "otel.status_description");
            assert_eq!(tags[0].v_type(), ValueType::String);
            assert_eq!(tags[0].v_str, "Error");

            assert_eq!(tags[1].key, "otel.status_code");
            assert_eq!(tags[1].v_type(), ValueType::String);
            assert_eq!(tags[1].v_str, "ERROR");

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
    fn test_collect_trace_ids() {
        {
            let agg_result_json = r#"{"trace_ids": {}}"#;
            let (trace_ids, _span_timestamps_range) = collect_trace_ids(agg_result_json).unwrap();
            assert!(trace_ids.is_empty());
        }
        {
            let agg_result_json = r#"{
                "trace_ids": {
                    "buckets": [
                        {"key": "jIr1E97+2DJBcBnOb/wjQg==", "doc_count": 3, "max_span_start_timestamp_micros": {"value": 1674611393000000.0 }}]}}"#;
            let (trace_ids, span_timestamps_range) = collect_trace_ids(agg_result_json).unwrap();
            assert_eq!(trace_ids, &["jIr1E97+2DJBcBnOb/wjQg=="]);
            assert_eq!(span_timestamps_range, 1674611393..=1674611393);
        }
        {
            let agg_result_json = r#"{
                "trace_ids": {
                    "buckets": [
                        {"key": "FKvicG794620BNsewGCknA==", "doc_count": 7, "max_span_start_timestamp_micros": { "value": 1674611388000000.0 }},
                        {"key": "jIr1E97+2DJBcBnOb/wjQg==", "doc_count": 3, "max_span_start_timestamp_micros": { "value": 1674611393000000.0 }}]}}"#;
            let (trace_ids, span_timestamps_range) = collect_trace_ids(agg_result_json).unwrap();
            assert_eq!(
                trace_ids,
                &["FKvicG794620BNsewGCknA==", "jIr1E97+2DJBcBnOb/wjQg=="]
            );
            assert_eq!(span_timestamps_range, 1674611388..=1674611393);
        }
    }
}
