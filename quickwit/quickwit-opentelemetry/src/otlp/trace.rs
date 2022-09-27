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

use async_trait::async_trait;
use base64;
use quickwit_actors::Mailbox;
use quickwit_ingest_api::IngestApiService;
use quickwit_proto::ingest_api::{DocBatch, IngestRequest};
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceService;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpValue;
use quickwit_proto::opentelemetry::proto::common::v1::KeyValue;
use quickwit_proto::opentelemetry::proto::trace::v1::Status;
use serde::Serialize;
use serde_json::{Number as JsonNumber, Value as JsonValue};
use tracing::{error, warn};

const TRACE_INDEX_ID: &str = "otel-trace";

#[derive(Clone)]
pub struct OtlpGrpcTraceService {
    ingest_api_service: Mailbox<IngestApiService>,
}

impl OtlpGrpcTraceService {
    // TODO: remove and use registry
    pub fn new(ingest_api_service: Mailbox<IngestApiService>) -> Self {
        Self { ingest_api_service }
    }
}

type Base64 = String;

#[derive(Debug, Serialize)]
struct FlattenedSpan {
    trace_id: Base64,
    span_id: Base64,
    trace_state: String,
    parent_span_id: Option<Base64>,
    name: String,
    kind: SpanKind,
    service_name: Option<String>,
    start_timestamp_nanos: i64,
    end_timestamp_nanos: i64,
    attributes: HashMap<String, JsonValue>,
    dropped_attributes_count: u64,
    events: Vec<Event>,
    dropped_events_count: u64,
    links: Vec<Link>,
    dropped_links_count: u64,
    status: Option<Status>,
}

#[derive(Debug, Serialize)]
struct SpanKind {
    id: i32,
    name: &'static str,
}

#[derive(Debug, Serialize)]
struct Event;

#[derive(Debug, Serialize)]
struct Link;

fn extract_attributes(attributes: Vec<KeyValue>) -> HashMap<String, JsonValue> {
    let mut attrs = HashMap::new();
    for attribute in attributes {
        // Filtering out empty attribute values is fine according to the OTel spec: <https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/common#attribute>
        if let Some(value) = attribute
            .value
            .and_then(|value| value.value)
            .and_then(to_json_value)
        {
            attrs.insert(attribute.key, value);
        }
    }
    attrs
}

fn extract_value(attributes: Vec<KeyValue>, key: &str) -> Option<OtlpValue> {
    attributes
        .iter()
        .find(|attribute| attribute.key == key)
        .and_then(|attribute| attribute.value.clone())
        .and_then(|value| value.value)
}

fn to_json_value(value: OtlpValue) -> Option<JsonValue> {
    match value {
        OtlpValue::StringValue(value) => Some(JsonValue::String(value)),
        OtlpValue::BoolValue(value) => Some(JsonValue::Bool(value)),
        OtlpValue::IntValue(value) => Some(JsonValue::Number(JsonNumber::from(value))),
        OtlpValue::DoubleValue(value) => JsonNumber::from_f64(value).map(JsonValue::Number),
        OtlpValue::ArrayValue(_) | OtlpValue::BytesValue(_) | OtlpValue::KvlistValue(_) => {
            warn!(value=?value, "Skipping unsupported OTLP value type");
            None
        }
    }
}

fn to_span_kind(id: i32) -> SpanKind {
    let name = match id {
        0 => "unspecified",
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => panic!("Unknown span kind: `{id}`."),
    };
    SpanKind { id, name }
}

#[async_trait]
impl TraceService for OtlpGrpcTraceService {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let request = request.into_inner();
        let mut doc_batch = DocBatch {
            index_id: TRACE_INDEX_ID.to_string(),
            ..Default::default()
        };
        for resource_span in request.resource_spans {
            println!("Resource: {:?}", resource_span.resource);
            let service_name = match resource_span
                .resource
                .and_then(|resource| extract_value(resource.attributes, "service.name"))
            {
                Some(OtlpValue::StringValue(service_name)) => Some(service_name),
                _ => None,
            };
            for scope_span in resource_span.scope_spans {
                println!("\tScope: {:?}", scope_span.scope);
                for span in scope_span.spans {
                    println!("\t\tSpan: {:?}", span);
                    let trace_id = base64::encode(span.trace_id);
                    let span_id = base64::encode(span.span_id);
                    let parent_span_id = if !span.parent_span_id.is_empty() {
                        Some(base64::encode(span.parent_span_id))
                    } else {
                        None
                    };
                    let start_timestamp_nanos = (span.start_time_unix_nano / 1_000) as i64; // TODO: use nanos
                    let end_timestamp_nanos = (span.end_time_unix_nano / 1_000) as i64; // TOOD: use nanos
                    let attributes = extract_attributes(span.attributes);
                    let flattened_span = FlattenedSpan {
                        trace_id,
                        span_id,
                        trace_state: span.trace_state,
                        parent_span_id,
                        name: span.name,
                        kind: to_span_kind(span.kind),
                        service_name: service_name.clone(),
                        start_timestamp_nanos,
                        end_timestamp_nanos,
                        attributes,
                        dropped_attributes_count: span.dropped_attributes_count as u64,
                        events: Vec::new(), // TODO: populate events
                        dropped_events_count: span.dropped_events_count as u64,
                        links: Vec::new(), // TODO: populate links
                        dropped_links_count: span.dropped_links_count as u64,
                        status: span.status,
                    };
                    let flattened_span_json = serde_json::to_vec(&flattened_span).expect("");
                    let flattened_span_json_len = flattened_span_json.len() as u64;
                    doc_batch
                        .concat_docs
                        .extend_from_slice(&flattened_span_json);
                    doc_batch.doc_lens.push(flattened_span_json_len);
                }
            }
        }
        let ingest_request = IngestRequest {
            doc_batches: vec![doc_batch],
        };
        // TODO: return appropriate tonic status
        if let Err(error) = self.ingest_api_service.ask_for_res(ingest_request).await {
            error!(error=?error, "Failed to ingest trace");
        }
        let response = ExportTraceServiceResponse::default();
        Ok(tonic::Response::new(response))
    }
}
