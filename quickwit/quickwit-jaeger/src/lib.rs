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

#![deny(clippy::disallowed_methods)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use prost_types::{Duration as WellKnownDuration, Timestamp as WellKnownTimestamp};
use quickwit_opentelemetry::otlp::{Event as QwEvent, Link as QwLink, Span as QwSpan};
use quickwit_proto::jaeger::api_v2::{
    KeyValue as JaegerKeyValue, Log as JaegerLog, Process as JaegerProcess, Span as JaegerSpan,
    SpanRef as JaegerSpanRef, SpanRefType as JaegerSpanRefType, ValueType,
};
use quickwit_proto::jaeger::storage::v1::span_reader_plugin_server::SpanReaderPlugin;
use quickwit_proto::jaeger::storage::v1::{
    FindTraceIDsRequest, FindTraceIDsResponse, FindTracesRequest, GetOperationsRequest,
    GetOperationsResponse, GetServicesRequest, GetServicesResponse, GetTraceRequest, Operation,
    SpansResponseChunk,
};
use quickwit_proto::opentelemetry::proto::trace::v1::Status as OtlpStatus;
use quickwit_proto::SearchRequest;
use quickwit_search::SearchService;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

// OpenTelemetry to Jaeger Transformation
// <https://opentelemetry.io/docs/reference/specification/trace/sdk_exporters/jaeger/>

const TRACE_INDEX_ID: &str = "otel-trace-v0";

pub struct JaegerService {
    search_service: Arc<dyn SearchService>,
}

impl JaegerService {
    pub fn new(search_service: Arc<dyn SearchService>) -> Self {
        Self { search_service }
    }
}

type SpanStream = ReceiverStream<Result<SpansResponseChunk, Status>>;

#[async_trait]
impl SpanReaderPlugin for JaegerService {
    type GetTraceStream = SpanStream;

    type FindTracesStream = SpanStream;

    async fn get_services(
        &self,
        request: Request<GetServicesRequest>,
    ) -> Result<Response<GetServicesResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_services` request");

        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query("", "", "", HashMap::new()),
            search_fields: Vec::new(),
            start_timestamp: None, // TODO: limit to last 24h?
            end_timestamp: None,
            max_hits: 1_000,
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
        let response = GetServicesResponse { services };
        debug!(response=?response, "`get_services` response");
        Ok(Response::new(response))
    }

    async fn get_operations(
        &self,
        request: Request<GetOperationsRequest>,
    ) -> Result<Response<GetOperationsResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_operations` request");

        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&request.service, &request.span_kind, "", HashMap::new()),
            search_fields: Vec::new(),
            start_timestamp: None, // TODO: limit to last 24h?
            end_timestamp: None,
            max_hits: 1_000,
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
        let response = GetOperationsResponse {
            operations,
            operation_names: Vec::new(), // `operation_names` is deprecated.
        };
        debug!(response=?response, "`get_operations` response");
        Ok(Response::new(response))
    }

    async fn find_traces(
        &self,
        request: Request<FindTracesRequest>,
    ) -> Result<Response<Self::FindTracesStream>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`find_traces` request");

        let query = request
            .query
            .ok_or_else(|| Status::invalid_argument("Trace query is empty."))?;
        let start_timestamp = query.start_time_min.map(|ts| ts.seconds);
        let end_timestamp = query.start_time_max.map(|ts| ts.seconds);
        // TODO: Push span duration filter.
        let max_hits = query.num_traces as u64;
        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&query.service_name, "", &query.operation_name, query.tags),
            search_fields: Vec::new(),
            start_timestamp,
            end_timestamp,
            max_hits,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self.search_service.root_search(search_request).await?;
        let trace_ids: HashSet<String> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen!")
            })
            .flat_map(extract_trace_id)
            .collect();
        debug!(trace_ids=?trace_ids, "`find_traces` matched trace IDs");
        if trace_ids.is_empty() {
            let (_tx, rx) = mpsc::channel(1);
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
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
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self.search_service.root_search(search_request).await?;
        let spans = search_response
            .hits
            .into_iter()
            .map(|hit| qw_span_to_jaeger_span(&hit.json))
            .collect::<Result<_, _>>()?;
        debug!(spans=?spans, "`find_traces` response");
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(SpansResponseChunk { spans }))
            .await
            .expect("The channel should be opened and empty.");
        let response = ReceiverStream::new(rx);
        Ok(Response::new(response))
    }

    async fn find_trace_i_ds(
        &self,
        request: Request<FindTraceIDsRequest>,
    ) -> Result<Response<FindTraceIDsResponse>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`find_trace_ids` request");
        let query = request
            .query
            .ok_or_else(|| Status::invalid_argument("Query is empty."))?;
        let start_timestamp = query.start_time_min.map(|ts| ts.seconds);
        let end_timestamp = query.start_time_max.map(|ts| ts.seconds);
        // TODO: Push span duration filter.
        let max_hits = query.num_traces as u64;
        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query: build_query(&query.service_name, "", &query.operation_name, query.tags),
            search_fields: Vec::new(),
            start_timestamp,
            end_timestamp,
            max_hits,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self.search_service.root_search(search_request).await?;
        let trace_ids: Vec<Vec<u8>> = search_response
            .hits
            .into_iter()
            .map(|hit| {
                serde_json::from_str::<JsonValue>(&hit.json)
                    .expect("Failed to deserialize hit. This should never happen.")
            })
            .filter_map(extract_trace_id)
            .sorted()
            .dedup()
            .map(|trace_id| {
                base64::decode(&trace_id)
                    .expect("Failed to decode trace ID. This should never happen!")
            })
            .collect();
        let response = FindTraceIDsResponse { trace_ids };
        debug!(response=?response, "`find_trace_ids` response");
        Ok(Response::new(response))
    }

    async fn get_trace(
        &self,
        request: Request<GetTraceRequest>,
    ) -> Result<Response<Self::GetTraceStream>, Status> {
        let request = request.into_inner();
        debug!(request=?request, "`get_trace` request");
        let query = format!("trace_id:{}", base64::encode(request.trace_id));
        let search_request = SearchRequest {
            index_id: TRACE_INDEX_ID.to_string(),
            query,
            search_fields: Vec::new(),
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 1_000,
            start_offset: 0,
            sort_order: None,
            sort_by_field: None,
            aggregation_request: None,
            snippet_fields: Vec::new(),
        };
        let search_response = self.search_service.root_search(search_request).await?;
        let spans = search_response
            .hits
            .into_iter()
            .map(|hit| qw_span_to_jaeger_span(&hit.json))
            .collect::<Result<_, _>>()?;
        debug!(spans=?spans, "`get_trace` response");
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(SpansResponseChunk { spans }))
            .await
            .expect("The channel should be opened and empty.");
        let response = ReceiverStream::new(rx);
        Ok(Response::new(response))
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

fn extract_trace_id(mut doc: JsonValue) -> Option<String> {
    match doc["trace_id"].take() {
        JsonValue::String(trace_id) => Some(trace_id),
        _ => None,
    }
}

fn build_query(
    service_name: &str,
    span_kind: &str,
    span_name: &str,
    mut tags: HashMap<String, String>,
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
    if query.is_empty() {
        query.push('*');
    }
    debug!(query=%query, "Search query");
    query
}

fn qw_span_to_jaeger_span(qw_span: &str) -> Result<JaegerSpan, Status> {
    let mut span = serde_json::from_str::<QwSpan>(qw_span)
        .map_err(|error| Status::internal(format!("Failed to deserialize span: {error:?}")))?;
    let trace_id = base64::decode(span.trace_id).map_err(|error| {
        Status::internal(format!("Failed to base64 decode trace ID: {error:?}"))
    })?;
    let span_id = base64::decode(span.span_id)
        .map_err(|error| Status::internal(format!("Failed to base64 decode span ID: {error:?}")))?;

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

fn to_well_known_timestamp(timestamp_nanos: i64) -> WellKnownTimestamp {
    let seconds = timestamp_nanos / 1_000_000_000;
    let nanos = (timestamp_nanos % 1_000_000_000) as i32;
    WellKnownTimestamp { seconds, nanos }
}

fn to_well_known_duration(
    start_timestamp_nanos: i64,
    end_timestamp_nanos: i64,
) -> WellKnownDuration {
    let duration_nanos = end_timestamp_nanos - start_timestamp_nanos;
    let seconds = duration_nanos / 1_000_000_000;
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

fn inject_span_status_tags(tags: &mut Vec<JaegerKeyValue>, span_status_opt: Option<OtlpStatus>) {
    // Span Status MUST be reported as key-value pairs associated with the Span, unless the Status
    // is UNSET. In the latter case it MUST NOT be reported.
    if let Some(span_status) = span_status_opt {
        // Description of the Status if it has a value otherwise not set.
        if !span_status.message.is_empty() {
            tags.push(JaegerKeyValue {
                key: "otel.status_description".to_string(),
                v_type: ValueType::String as i32,
                v_str: span_status.message,
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
        let parent_span_id = base64::decode(parent_span_id).map_err(|error| {
            Status::internal(format!("Failed to decode parent span ID: {error:?}"))
        })?;
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
        let trace_id = base64::decode(link.link_trace_id).map_err(|error| {
            Status::internal(format!("Failed to decode parent span ID: {error:?}"))
        })?;
        let span_id = base64::decode(link.link_span_id).map_err(|error| {
            Status::internal(format!("Failed to decode parent span ID: {error:?}"))
        })?;
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

#[cfg(test)]
mod tests {
    use quickwit_proto::jaeger::api_v2::ValueType;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_build_query() {
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            assert_eq!(build_query(service_name, span_kind, span_name, tags), "*");
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "service_name:quickwit"
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("_qw_query".to_string(), "query".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "query"
            );
        }
        {
            let service_name = "";
            let span_kind = "client";
            let span_name = "";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_kind:3"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "leaf_search";
            let tags = HashMap::new();
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                "span_name:leaf_search"
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar baz".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                r#"(span_attributes.foo:"bar baz" OR events.event_attributes.foo:"bar baz")"#
            );
        }
        {
            let service_name = "";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("event".to_string(), "Failed to ...".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
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
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
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
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                r#"(span_attributes.baz:"qux" OR events.event_attributes.baz:"qux") AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                r#"service_name:quickwit AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                r#"service_name:quickwit AND span_kind:3 AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
        }
        {
            let service_name = "quickwit";
            let span_kind = "client";
            let span_name = "leaf_search";
            let tags = HashMap::from_iter([("foo".to_string(), "bar".to_string())]);
            assert_eq!(
                build_query(service_name, span_kind, span_name, tags),
                r#"service_name:quickwit AND span_kind:3 AND span_name:leaf_search AND (span_attributes.foo:"bar" OR events.event_attributes.foo:"bar")"#
            );
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
            let span_status = OtlpStatus {
                code: 0,
                message: "".to_string(),
            };
            inject_span_status_tags(&mut tags, Some(span_status));
            assert!(tags.is_empty());
        }
        {
            let mut tags = Vec::new();
            let span_status = OtlpStatus {
                code: 0,
                message: "foo".to_string(),
            };
            inject_span_status_tags(&mut tags, Some(span_status));
            assert_eq!(tags.len(), 1);
            assert_eq!(tags[0].key, "otel.status_description");
            assert_eq!(tags[0].v_type(), ValueType::String);
            assert_eq!(tags[0].v_str, "foo");
        }
        {
            let mut tags = Vec::new();
            let span_status = OtlpStatus {
                code: 1,
                message: "Ok".to_string(),
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
            let span_status = OtlpStatus {
                code: 2,
                message: "Error".to_string(),
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
}
