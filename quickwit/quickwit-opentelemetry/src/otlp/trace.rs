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
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use quickwit_proto::opentelemetry::proto::trace::v1::Status;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tracing::error;

use crate::otlp::extract_attributes;

const TRACE_INDEX_ID: &str = "otel-trace-v0";

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

pub type B64String = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: B64String,
    pub trace_state: String,
    pub resource_attributes: HashMap<String, JsonValue>,
    pub resource_dropped_attributes_count: u64,
    pub service_name: String,
    pub span_id: B64String,
    pub span_kind: u64,
    pub span_name: String,
    pub span_start_timestamp_secs: i64,
    pub span_start_timestamp_nanos: i64,
    pub span_end_timestamp_nanos: i64,
    pub span_duration_secs: i64,
    pub span_attributes: HashMap<String, JsonValue>,
    pub span_dropped_attributes_count: u64,
    pub span_dropped_events_count: u64,
    pub span_dropped_links_count: u64,
    pub span_status: Option<Status>,
    pub parent_span_id: Option<B64String>,
    #[serde(default)]
    pub events: Vec<Event>,
    #[serde(default)]
    pub links: Vec<Link>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub event_timestamp_nanos: i64,
    pub event_name: String,
    pub event_attributes: HashMap<String, JsonValue>,
    pub event_dropped_attributes_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Link {
    pub link_trace_id: B64String,
    pub link_trace_state: String,
    pub link_span_id: B64String,
    pub link_attributes: HashMap<String, JsonValue>,
    pub link_dropped_attributes_count: u64,
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
        let mut num_spans = 0;
        let mut rejected_spans = 0;
        let mut error_message = String::new();

        for resource_span in request.resource_spans {
            let resource_attributes = extract_attributes(
                resource_span
                    .resource
                    .clone()
                    .map(|rsrc| rsrc.attributes)
                    .unwrap_or_else(Vec::new),
            );
            let resource_dropped_attributes_count = resource_span
                .resource
                .map(|rsrc| rsrc.dropped_attributes_count)
                .unwrap_or(0) as u64;
            let service_name = match resource_attributes.get("service.name") {
                Some(JsonValue::String(value)) => value.to_string(),
                _ => "unknown".to_string(),
            };
            for scope_span in resource_span.scope_spans {
                for span in scope_span.spans {
                    num_spans += 1;

                    let trace_id = base64::encode(span.trace_id);
                    let span_id = base64::encode(span.span_id);
                    let parent_span_id = if !span.parent_span_id.is_empty() {
                        Some(base64::encode(span.parent_span_id))
                    } else {
                        None
                    };
                    let span_name = if !span.name.is_empty() {
                        span.name
                    } else {
                        "unknown".to_string()
                    };
                    let span_start_timestamp_nanos = span.start_time_unix_nano as i64;
                    let span_start_timestamp_secs = span_start_timestamp_nanos / 1_000_000_000;
                    let span_end_timestamp_nanos = span.end_time_unix_nano as i64;
                    let span_duration_nanos = span_end_timestamp_nanos - span_start_timestamp_nanos;
                    let span_duration_secs = span_duration_nanos / 1_000_000_000 + 1;
                    let span_attributes = extract_attributes(span.attributes);

                    let events = span
                        .events
                        .into_iter()
                        .map(|event| Event {
                            event_timestamp_nanos: event.time_unix_nano as i64,
                            event_name: event.name,
                            event_attributes: extract_attributes(event.attributes),
                            event_dropped_attributes_count: event.dropped_attributes_count as u64,
                        })
                        .collect();
                    let links = span
                        .links
                        .into_iter()
                        .map(|link| Link {
                            link_trace_id: base64::encode(link.trace_id),
                            link_trace_state: link.trace_state,
                            link_span_id: base64::encode(link.span_id),
                            link_attributes: extract_attributes(link.attributes),
                            link_dropped_attributes_count: link.dropped_attributes_count as u64,
                        })
                        .collect();
                    let span = Span {
                        trace_id,
                        trace_state: span.trace_state,
                        resource_attributes: resource_attributes.clone(),
                        resource_dropped_attributes_count,
                        service_name: service_name.clone(),
                        span_id,
                        span_kind: span.kind as u64,
                        span_name,
                        span_start_timestamp_secs,
                        span_start_timestamp_nanos,
                        span_end_timestamp_nanos,
                        span_duration_secs,
                        span_attributes,
                        span_dropped_attributes_count: span.dropped_attributes_count as u64,
                        span_dropped_events_count: span.dropped_events_count as u64,
                        span_dropped_links_count: span.dropped_links_count as u64,
                        span_status: span.status,
                        parent_span_id,
                        events,
                        links,
                    };
                    let span_json = match serde_json::to_vec(&span) {
                        Ok(span_json) => span_json,
                        Err(err) => {
                            error!(error=?err, "Failed to serialize span.");
                            error_message = format!("Failed to serialize span: {err:?}");
                            rejected_spans += 1;
                            continue;
                        }
                    };
                    let span_json_len = span_json.len() as u64;
                    doc_batch.concat_docs.extend_from_slice(&span_json);
                    doc_batch.doc_lens.push(span_json_len);
                }
            }
        }
        if rejected_spans == num_spans {
            return Err(tonic::Status::internal(error_message));
        }
        let ingest_request = IngestRequest {
            doc_batches: vec![doc_batch],
        };
        self.ingest_api_service
            .ask_for_res(ingest_request)
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let response = ExportTraceServiceResponse {
            // `rejected_spans=0` and `error_message=""` is consided a "full" success.
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans,
                error_message,
            }),
        };
        Ok(tonic::Response::new(response))
    }
}
