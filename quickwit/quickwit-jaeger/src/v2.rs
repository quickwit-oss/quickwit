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

// Jaeger v2 API implementation (TraceReader)
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use prost_types::Timestamp as WellKnownTimestamp;
use quickwit_opentelemetry::otlp::{
    OTEL_TRACES_INDEX_ID, Span as QwSpan, TraceId,
    extract_otel_traces_index_id_patterns_from_metadata,
};
use quickwit_proto::jaeger::storage::v2::trace_reader_server::TraceReader;
use quickwit_proto::jaeger::storage::v2::{
    FindTracesRequest, FoundTraceId, GetOperationsRequest, GetOperationsResponse,
    GetServicesRequest, GetServicesResponse, GetTracesRequest, Operation,
};
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtelValue;
use quickwit_proto::opentelemetry::proto::common::v1::{
    AnyValue as OtelAnyValue, InstrumentationScope, KeyValue as OtelKeyValue,
};
use quickwit_proto::opentelemetry::proto::resource::v1::Resource as OtelResource;
use quickwit_proto::opentelemetry::proto::trace::v1 as otel_trace;
use quickwit_proto::opentelemetry::proto::trace::v1::status::StatusCode as OtelStatusCode;
use quickwit_proto::opentelemetry::proto::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtelSpan, Status as OtelStatus,
};
use quickwit_proto::search::{CountHits, SearchRequest};
use quickwit_query::BooleanOperand;
use quickwit_query::query_ast::{BoolQuery, QueryAst, TermQuery, UserInputQuery};
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{Span as RuntimeSpan, debug, error, instrument};

use crate::metrics::JAEGER_SERVICE_METRICS;
use crate::{
    JaegerService, TimeIntervalSecs, TracesDataStream, get_operations_impl, get_services_impl,
    json_deserialize, record_error, record_send, to_duration_millis,
};

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

#[async_trait]
impl TraceReader for JaegerService {
    async fn get_services(
        &self,
        request: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;

        let services = get_services_impl(
            self.search_service.clone(),
            self.lookback_period_secs,
            index_id_patterns,
        )
        .await?;

        let response = GetServicesResponse { services };
        metrics!(Ok(response), [get_services_v2, OTEL_TRACES_INDEX_ID]);
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;

        let req = request.into_inner();

        let operations = get_operations_impl(
            self.search_service.clone(),
            self.lookback_period_secs,
            req.service,
            req.span_kind,
            index_id_patterns,
        )
        .await?
        .into_iter()
        .map(|op| Operation {
            name: op.name,
            span_kind: op.span_kind,
        })
        .collect();

        let response = GetOperationsResponse { operations };
        metrics!(Ok(response), [get_operations_v2, OTEL_TRACES_INDEX_ID]);
    }

    type GetTracesStream = TracesDataStream;

    async fn get_traces(
        &self,
        request: Request<GetTracesRequest>,
    ) -> Result<Response<Self::GetTracesStream>, Status> {
        let request_start = Instant::now();
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;

        let (tx, rx) = mpsc::channel(2);
        let search_service = self.search_service.clone();
        let max_fetch_spans = self.max_fetch_spans;
        let lookback_period_secs = self.lookback_period_secs;
        let query_list = request.into_inner().query;

        tokio::task::spawn(async move {
            for query_params in query_list {
                let trace_id = match TraceId::try_from(query_params.trace_id) {
                    Ok(id) => id,
                    Err(error) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(error.to_string())))
                            .await;
                        return;
                    }
                };

                let end = OffsetDateTime::now_utc().unix_timestamp();
                let search_window = (end - lookback_period_secs)..=end;

                let otel_spans = match stream_otel_spans_impl(
                    search_service.clone(),
                    max_fetch_spans,
                    &[trace_id],
                    search_window,
                    "get_traces_v2",
                    request_start,
                    index_id_patterns.clone(),
                    false,
                )
                .await
                {
                    Ok(spans) => spans,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };

                if tx
                    .send(Ok(qw_spans_to_otel_traces_data(otel_spans)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type FindTracesStream = TracesDataStream;

    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        let request_start = Instant::now();

        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;

        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Query is empty."))?;

        let (trace_ids, span_timestamps_range) = find_trace_ids_impl(
            self.search_service.clone(),
            self.max_trace_duration_secs,
            query,
            index_id_patterns.clone(),
        )
        .await?;

        let search_window = (span_timestamps_range.start() - self.max_trace_duration_secs)
            ..=(span_timestamps_range.end() + self.max_trace_duration_secs);

        let (tx, rx) = mpsc::channel(2);
        let search_service = self.search_service.clone();
        let max_fetch_spans = self.max_fetch_spans;

        tokio::task::spawn(async move {
            let all_spans = match stream_otel_spans_impl(
                search_service,
                max_fetch_spans,
                &trace_ids,
                search_window,
                "find_traces_v2",
                request_start,
                index_id_patterns,
                false,
            )
            .await
            {
                Ok(spans) => spans,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            // Group by trace_id and send each trace
            let mut spans_by_trace: HashMap<Vec<u8>, Vec<QwSpan>> = HashMap::new();
            for span in all_spans {
                spans_by_trace
                    .entry(span.trace_id.to_vec())
                    .or_default()
                    .push(span);
            }

            for spans in spans_by_trace.into_values() {
                if tx
                    .send(Ok(qw_spans_to_otel_traces_data(spans)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn find_trace_i_ds(
        &self,
        request: Request<quickwit_proto::jaeger::storage::v2::FindTracesRequest>,
    ) -> Result<Response<quickwit_proto::jaeger::storage::v2::FindTraceIDsResponse>, Status> {
        let index_id_patterns =
            extract_otel_traces_index_id_patterns_from_metadata(request.metadata())?;

        let query = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("Query is empty."))?;

        let (trace_ids, time_range) = find_trace_ids_impl(
            self.search_service.clone(),
            self.max_trace_duration_secs,
            query,
            index_id_patterns,
        )
        .await?;

        let trace_ids = trace_ids
            .into_iter()
            .map(|trace_id| FoundTraceId {
                trace_id: trace_id.to_vec(),
                start: Some(WellKnownTimestamp {
                    seconds: *time_range.start(),
                    nanos: 0,
                }),
                end: Some(WellKnownTimestamp {
                    seconds: *time_range.end(),
                    nanos: 0,
                }),
            })
            .collect();

        let response = quickwit_proto::jaeger::storage::v2::FindTraceIDsResponse { trace_ids };
        metrics!(Ok(response), [find_trace_ids_v2, OTEL_TRACES_INDEX_ID]);
    }
}

// === Helper functions ===
#[instrument("find_trace_ids", skip_all)]
async fn find_trace_ids_impl(
    search_service: Arc<dyn SearchService>,
    _max_trace_duration_secs: i64,
    query: quickwit_proto::jaeger::storage::v2::TraceQueryParameters,
    index_id_patterns: Vec<String>,
) -> Result<(Vec<TraceId>, TimeIntervalSecs), Status> {
    debug!(service_name=%query.service_name, operation_name=%query.operation_name, "`find_trace_ids` request");

    let min_start_secs = query.start_time_min.as_ref().map(|ts| ts.seconds);
    let max_start_secs = query.start_time_max.as_ref().map(|ts| ts.seconds);
    let min_duration_millis = query.duration_min.as_ref().and_then(to_duration_millis);
    let max_duration_millis = query.duration_max.as_ref().and_then(to_duration_millis);
    let tags = convert_v2_attributes_to_v1_tags(query.attributes);

    crate::find_trace_ids_common(
        search_service,
        &query.service_name,
        &query.operation_name,
        tags,
        min_start_secs,
        max_start_secs,
        min_duration_millis,
        max_duration_millis,
        query.search_depth as usize,
        index_id_patterns,
    )
    .await
}

#[instrument("stream_otel_spans", skip_all, fields(num_traces=%trace_ids.len(), num_spans=Empty, num_bytes=Empty))]
#[allow(clippy::too_many_arguments)]
async fn stream_otel_spans_impl(
    search_service: Arc<dyn SearchService>,
    max_fetch_spans: u64,
    trace_ids: &[TraceId],
    search_window: TimeIntervalSecs,
    operation_name: &'static str,
    request_start: Instant,
    index_id_patterns: Vec<String>,
    root_only: bool,
) -> Result<Vec<QwSpan>, Status> {
    if trace_ids.is_empty() {
        return Ok(Vec::new());
    }

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
        max_hits: max_fetch_spans,
        count_hits: CountHits::Underestimate.into(),
        ..Default::default()
    };

    let search_response = match search_service.root_search(search_request).await {
        Ok(search_response) => search_response,
        Err(search_error) => {
            error!(search_error=?search_error, "failed to fetch spans");
            record_error(operation_name, request_start);
            return Err(Status::internal("Failed to fetch spans."));
        }
    };

    let mut qw_spans: Vec<QwSpan> = Vec::with_capacity(search_response.hits.len());

    for hit in search_response.hits {
        match qw_span_from_json(&hit.json) {
            Ok(span) => {
                qw_spans.push(span);
            }
            Err(status) => {
                record_error(operation_name, request_start);
                return Err(status);
            }
        };
    }

    if trace_ids.len() > 1 {
        qw_spans.sort_unstable_by(|left, right| left.trace_id.cmp(&right.trace_id));
    }

    let num_spans = qw_spans.len();
    let num_bytes = qw_spans
        .iter()
        .map(|span| serde_json::to_string(span).unwrap_or_default().len())
        .sum::<usize>();

    RuntimeSpan::current().record("num_spans", num_spans);
    RuntimeSpan::current().record("num_bytes", num_bytes);

    record_send(operation_name, num_spans, num_bytes);

    JAEGER_SERVICE_METRICS
        .fetched_traces_total
        .with_label_values([operation_name, OTEL_TRACES_INDEX_ID])
        .inc_by(trace_ids.len() as u64);

    let elapsed = request_start.elapsed().as_secs_f64();
    JAEGER_SERVICE_METRICS
        .request_duration_seconds
        .with_label_values([operation_name, OTEL_TRACES_INDEX_ID, "false"])
        .observe(elapsed);

    Ok(qw_spans)
}

// === Conversion functions ===
// Note: record_error and record_send are now shared in lib.rs

/// Direct conversion from Quickwit's native OpenTelemetry span to Jaeger v2's OpenTelemetry format
fn qw_spans_to_otel_traces_data(
    qw_spans: Vec<QwSpan>,
) -> quickwit_proto::opentelemetry::proto::trace::v1::TracesData {
    // Group spans by service
    let mut spans_by_service: HashMap<String, Vec<QwSpan>> = HashMap::new();
    for span in qw_spans {
        spans_by_service
            .entry(span.service_name.clone())
            .or_default()
            .push(span);
    }

    let resource_spans = spans_by_service
        .into_iter()
        .map(|(service_name, spans)| {
            // Get resource attributes from first span before grouping
            let first_span_attrs = spans
                .first()
                .map(|span| span.resource_attributes.clone())
                .unwrap_or_default();

            // Group by scope
            let mut spans_by_scope: HashMap<(Option<String>, Option<String>), Vec<QwSpan>> =
                HashMap::new();
            for span in spans {
                let key = (span.scope_name.clone(), span.scope_version.clone());
                spans_by_scope.entry(key).or_default().push(span);
            }

            let scope_spans = spans_by_scope
                .into_iter()
                .map(|((scope_name, scope_version), spans)| {
                    let otel_spans = spans.into_iter().map(qw_span_to_otel_span).collect();

                    ScopeSpans {
                        scope: Some(InstrumentationScope {
                            name: scope_name.unwrap_or_default(),
                            version: scope_version.unwrap_or_default(),
                            attributes: vec![],
                            dropped_attributes_count: 0,
                        }),
                        spans: otel_spans,
                        schema_url: String::new(),
                    }
                })
                .collect();

            let mut resource_attrs = vec![OtelKeyValue {
                key: "service.name".to_string(),
                value: Some(OtelAnyValue {
                    value: Some(OtelValue::StringValue(service_name)),
                }),
            }];

            // Add other resource attributes
            for (key, value) in first_span_attrs {
                resource_attrs.push(json_value_to_otel_kv(key, value));
            }

            ResourceSpans {
                resource: Some(OtelResource {
                    attributes: resource_attrs,
                    dropped_attributes_count: 0,
                }),
                scope_spans,
                schema_url: String::new(),
            }
        })
        .collect();

    quickwit_proto::opentelemetry::proto::trace::v1::TracesData { resource_spans }
}

/// Convert a Quickwit span (native OTEL format) to Jaeger v2 OTEL span
fn qw_span_to_otel_span(qw_span: QwSpan) -> OtelSpan {
    OtelSpan {
        trace_id: qw_span.trace_id.to_vec(),
        span_id: qw_span.span_id.to_vec(),
        trace_state: qw_span.trace_state.unwrap_or_default(),
        parent_span_id: qw_span
            .parent_span_id
            .map(|id| id.to_vec())
            .unwrap_or_default(),
        name: qw_span.span_name,
        kind: qw_span.span_kind as i32,
        start_time_unix_nano: qw_span.span_start_timestamp_nanos,
        end_time_unix_nano: qw_span.span_end_timestamp_nanos,
        attributes: qw_span
            .span_attributes
            .into_iter()
            .map(|(k, v)| json_value_to_otel_kv(k, v))
            .collect(),
        dropped_attributes_count: qw_span.span_dropped_attributes_count,
        events: qw_span
            .events
            .into_iter()
            .map(|event| otel_trace::span::Event {
                time_unix_nano: event.event_timestamp_nanos,
                name: event.event_name,
                attributes: event
                    .event_attributes
                    .into_iter()
                    .map(|(k, v)| json_value_to_otel_kv(k, v))
                    .collect(),
                dropped_attributes_count: event.event_dropped_attributes_count,
            })
            .collect(),
        dropped_events_count: qw_span.span_dropped_events_count,
        links: qw_span
            .links
            .into_iter()
            .map(|link| otel_trace::span::Link {
                trace_id: link.link_trace_id.to_vec(),
                span_id: link.link_span_id.to_vec(),
                trace_state: link.link_trace_state.unwrap_or_default(),
                attributes: link
                    .link_attributes
                    .into_iter()
                    .map(|(k, v)| json_value_to_otel_kv(k, v))
                    .collect(),
                dropped_attributes_count: link.link_dropped_attributes_count,
            })
            .collect(),
        dropped_links_count: qw_span.span_dropped_links_count,
        status: Some(OtelStatus {
            message: qw_span.span_status.message.unwrap_or_default(),
            code: match qw_span.span_status.code {
                quickwit_proto::opentelemetry::proto::trace::v1::status::StatusCode::Unset => {
                    OtelStatusCode::Unset as i32
                }
                quickwit_proto::opentelemetry::proto::trace::v1::status::StatusCode::Ok => {
                    OtelStatusCode::Ok as i32
                }
                quickwit_proto::opentelemetry::proto::trace::v1::status::StatusCode::Error => {
                    OtelStatusCode::Error as i32
                }
            },
        }),
    }
}

fn json_value_to_otel_kv(key: String, value: JsonValue) -> OtelKeyValue {
    let otel_value = match value {
        JsonValue::String(s) => OtelValue::StringValue(s),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                OtelValue::IntValue(i)
            } else if let Some(f) = n.as_f64() {
                OtelValue::DoubleValue(f)
            } else {
                OtelValue::StringValue(n.to_string())
            }
        }
        JsonValue::Bool(b) => OtelValue::BoolValue(b),
        JsonValue::Array(_) | JsonValue::Object(_) => OtelValue::StringValue(value.to_string()),
        JsonValue::Null => OtelValue::StringValue(String::new()),
    };

    OtelKeyValue {
        key,
        value: Some(OtelAnyValue {
            value: Some(otel_value),
        }),
    }
}

#[allow(clippy::result_large_err)]
fn qw_span_from_json(qw_span_json: &str) -> Result<QwSpan, Status> {
    json_deserialize(qw_span_json, "span")
}

pub(crate) fn convert_v2_attributes_to_v1_tags(
    attributes: Vec<quickwit_proto::jaeger::storage::v2::KeyValue>,
) -> HashMap<String, String> {
    attributes
        .into_iter()
        .filter_map(|kv| {
            let value = kv.value?.value?;
            let string_value = match value {
                quickwit_proto::jaeger::storage::v2::any_value::Value::StringValue(s) => s,
                quickwit_proto::jaeger::storage::v2::any_value::Value::IntValue(i) => i.to_string(),
                quickwit_proto::jaeger::storage::v2::any_value::Value::DoubleValue(d) => {
                    d.to_string()
                }
                quickwit_proto::jaeger::storage::v2::any_value::Value::BoolValue(b) => {
                    b.to_string()
                }
                _ => return None,
            };
            Some((kv.key, string_value))
        })
        .collect()
}
