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
use base64::prelude::{Engine, BASE64_STANDARD};
use quickwit_actors::Mailbox;
use quickwit_ingest_api::IngestApiService;
use quickwit_proto::ingest_api::{DocBatch, IngestRequest};
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceService;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use quickwit_proto::opentelemetry::proto::trace::v1::Status as OtlpStatus;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{error, instrument, Span as RuntimeSpan};

use crate::otlp::extract_attributes;
use crate::otlp::metrics::OTLP_SERVICE_METRICS;

pub const OTEL_TRACE_INDEX_ID: &str = "otel-trace-v0";

pub const OTEL_TRACE_INDEX_CONFIG: &str = r#"
version: 0.4

index_id: otel-trace-v0

doc_mapping:
  mode: strict
  field_mappings:
    - name: trace_id
      type: text
      tokenizer: raw
      fast: true
    - name: trace_state
      type: text
      indexed: false
    - name: service_name
      type: text
      tokenizer: raw
    - name: resource_attributes
      type: json
      tokenizer: raw
    - name: resource_dropped_attributes_count
      type: u64
      indexed: false
    - name: scope_name
      type: text
      indexed: false
    - name: scope_version
      type: text
      indexed: false
    - name: scope_attributes
      type: json
      indexed: false
    - name: scope_dropped_attributes_count
      type: u64
      indexed: false
    - name: span_id
      type: text
      tokenizer: raw
    - name: span_kind
      type: u64
    - name: span_name
      type: text
      tokenizer: raw
    - name: span_start_timestamp_nanos
      type: u64
      indexed: false
    - name: span_end_timestamp_nanos
      type: u64
      indexed: false
    - name: span_start_timestamp_secs
      type: datetime
      input_formats:
        - unix_timestamp
      indexed: false
      fast: true
      precision: seconds
      stored: false
    - name: span_duration_millis
      type: u64
      indexed: false
      fast: true
      stored: false
    - name: span_attributes
      type: json
      tokenizer: raw
    - name: span_dropped_attributes_count
      type: u64
      indexed: false
    - name: span_dropped_events_count
      type: u64
      indexed: false
    - name: span_dropped_links_count
      type: u64
      indexed: false
    - name: span_status
      type: json
      indexed: false
    - name: parent_span_id
      type: text
      indexed: false
    - name: events
      type: array<json>
      tokenizer: raw
    - name: event_names
      type: array<text>
      tokenizer: default
      record: position
      stored: false
    - name: links
      type: array<json>
      tokenizer: raw

  timestamp_field: span_start_timestamp_secs

  partition_key: service_name
  max_num_partitions: 200

indexing_settings:
  commit_timeout_secs: 30

search_settings:
  default_search_fields: []
"#;

pub type Base64 = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: Base64,
    pub trace_state: Option<String>,
    pub service_name: String,
    pub resource_attributes: HashMap<String, JsonValue>,
    pub resource_dropped_attributes_count: u64,
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    pub scope_attributes: HashMap<String, JsonValue>,
    pub scope_dropped_attributes_count: u64,
    pub span_id: Base64,
    pub span_kind: u64,
    pub span_name: String,
    /// Span start timestamp in nanoseconds. Stored as a `u64` instead of a `datetime` to avoid the
    /// truncation to microseconds. This field is stored but not indexed.
    pub span_start_timestamp_nanos: u64,
    /// Span start timestamp in nanoseconds. Stored as a `u64` instead of a `datetime` to avoid the
    /// truncation to microseconds. This field is stored but not indexed.
    pub span_end_timestamp_nanos: u64,
    /// Span start timestamp in seconds used for aggregations and range queries. This field is
    /// stored as a fast field but not indexed.
    pub span_start_timestamp_secs: Option<u64>,
    pub span_duration_millis: Option<u64>,
    pub span_attributes: HashMap<String, JsonValue>,
    pub span_dropped_attributes_count: u64,
    pub span_dropped_events_count: u64,
    pub span_dropped_links_count: u64,
    pub span_status: Option<SpanStatus>,
    pub parent_span_id: Option<Base64>,
    #[serde(default)]
    pub events: Vec<Event>,
    #[serde(default)]
    pub event_names: Vec<String>,
    #[serde(default)]
    pub links: Vec<Link>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpanStatus {
    pub code: i32,
    pub message: Option<String>,
}

impl From<OtlpStatus> for SpanStatus {
    fn from(value: OtlpStatus) -> Self {
        let message = if value.message.is_empty() {
            None
        } else {
            Some(value.message)
        };
        Self {
            code: value.code,
            message,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub event_timestamp_nanos: u64,
    pub event_name: String,
    pub event_attributes: HashMap<String, JsonValue>,
    pub event_dropped_attributes_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Link {
    pub link_trace_id: Base64,
    pub link_trace_state: String,
    pub link_span_id: Base64,
    pub link_attributes: HashMap<String, JsonValue>,
    pub link_dropped_attributes_count: u64,
}

struct ParsedSpans {
    doc_batch: DocBatch,
    num_spans: u64,
    num_parse_errors: u64,
    error_message: String,
}

#[derive(Clone)]
pub struct OtlpGrpcTraceService {
    ingest_api_service: Mailbox<IngestApiService>,
}

impl OtlpGrpcTraceService {
    // TODO: remove and use registry
    pub fn new(ingest_api_service: Mailbox<IngestApiService>) -> Self {
        Self { ingest_api_service }
    }

    async fn export_inner(
        &self,
        request: ExportTraceServiceRequest,
        labels: [&'static str; 4],
    ) -> Result<ExportTraceServiceResponse, Status> {
        let ParsedSpans {
            doc_batch,
            num_spans,
            num_parse_errors,
            error_message,
        } = tokio::task::spawn_blocking({
            let parent_span = RuntimeSpan::current();
            || Self::parse_spans(request, parent_span)
        })
        .await
        .map_err(|join_error| {
            error!("Failed to parse spans: {join_error:?}");
            Status::internal("Failed to parse spans.")
        })??;
        if num_spans == num_parse_errors {
            return Err(tonic::Status::internal(error_message));
        }
        let num_bytes = doc_batch.concat_docs.len() as u64;
        self.store_spans(doc_batch).await?;

        OTLP_SERVICE_METRICS
            .ingested_spans_total
            .with_label_values(labels)
            .inc_by(num_spans);
        OTLP_SERVICE_METRICS
            .ingested_bytes_total
            .with_label_values(labels)
            .inc_by(num_bytes);

        let response = ExportTraceServiceResponse {
            // `rejected_spans=0` and `error_message=""` is consided a "full" success.
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: num_parse_errors as i64,
                error_message,
            }),
        };
        Ok(response)
    }

    #[instrument(skip_all, parent = parent_span, fields(num_spans = Empty, num_bytes = Empty, num_parse_errors = Empty))]
    fn parse_spans(
        request: ExportTraceServiceRequest,
        parent_span: RuntimeSpan,
    ) -> Result<ParsedSpans, Status> {
        let mut doc_batch = DocBatch {
            index_id: OTEL_TRACE_INDEX_ID.to_string(),
            ..Default::default()
        };
        let mut num_spans = 0;
        let mut num_parse_errors = 0;
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
                let scope_name = scope_span.scope.as_ref().map(|scope| &scope.name);
                let scope_version = scope_span.scope.as_ref().map(|scope| &scope.version);
                let scope_attributes = extract_attributes(
                    scope_span
                        .scope
                        .clone()
                        .map(|scope| scope.attributes)
                        .unwrap_or_else(Vec::new),
                );
                let scope_dropped_attributes_count = scope_span
                    .scope
                    .as_ref()
                    .map(|scope| scope.dropped_attributes_count)
                    .unwrap_or(0) as u64;
                for span in scope_span.spans {
                    num_spans += 1;

                    let trace_id = BASE64_STANDARD.encode(span.trace_id);
                    let span_id = BASE64_STANDARD.encode(span.span_id);
                    let parent_span_id = if !span.parent_span_id.is_empty() {
                        Some(BASE64_STANDARD.encode(span.parent_span_id))
                    } else {
                        None
                    };
                    let span_name = if !span.name.is_empty() {
                        span.name
                    } else {
                        "unknown".to_string()
                    };
                    let span_start_timestamp_secs = Some(span.start_time_unix_nano / 1_000_000_000);
                    let span_duration_nanos = span.end_time_unix_nano - span.start_time_unix_nano;
                    let span_duration_millis = Some(span_duration_nanos / 1_000_000);
                    let span_attributes = extract_attributes(span.attributes);

                    let events: Vec<Event> = span
                        .events
                        .into_iter()
                        .map(|event| Event {
                            event_timestamp_nanos: event.time_unix_nano,
                            event_name: event.name,
                            event_attributes: extract_attributes(event.attributes),
                            event_dropped_attributes_count: event.dropped_attributes_count as u64,
                        })
                        .collect();
                    let event_names: Vec<String> = events
                        .iter()
                        .map(|event| event.event_name.clone())
                        .collect();
                    let links: Vec<Link> = span
                        .links
                        .into_iter()
                        .map(|link| Link {
                            link_trace_id: BASE64_STANDARD.encode(link.trace_id),
                            link_trace_state: link.trace_state,
                            link_span_id: BASE64_STANDARD.encode(link.span_id),
                            link_attributes: extract_attributes(link.attributes),
                            link_dropped_attributes_count: link.dropped_attributes_count as u64,
                        })
                        .collect();
                    let trace_state = if span.trace_state.is_empty() {
                        None
                    } else {
                        Some(span.trace_state)
                    };
                    let span = Span {
                        trace_id,
                        trace_state,
                        service_name: service_name.clone(),
                        resource_attributes: resource_attributes.clone(),
                        resource_dropped_attributes_count,
                        scope_name: scope_name.cloned(),
                        scope_version: scope_version.cloned(),
                        scope_attributes: scope_attributes.clone(),
                        scope_dropped_attributes_count,
                        span_id,
                        span_kind: span.kind as u64,
                        span_name,
                        span_start_timestamp_nanos: span.start_time_unix_nano,
                        span_end_timestamp_nanos: span.end_time_unix_nano,
                        span_start_timestamp_secs,
                        span_duration_millis,
                        span_attributes,
                        span_dropped_attributes_count: span.dropped_attributes_count as u64,
                        span_dropped_events_count: span.dropped_events_count as u64,
                        span_dropped_links_count: span.dropped_links_count as u64,
                        span_status: span.status.map(|status| status.into()),
                        parent_span_id,
                        events,
                        event_names,
                        links,
                    };
                    let doc_batch_len = doc_batch.concat_docs.len();

                    match serde_json::to_writer(&mut doc_batch.concat_docs, &span) {
                        Ok(span_json) => span_json,
                        Err(error) => {
                            error!(error=?error, "Failed to JSON serialize span.");
                            error_message = format!("Failed to JSON serialize span: {error:?}");
                            num_parse_errors += 1;
                            continue;
                        }
                    }
                    let span_json_len = doc_batch.concat_docs.len() - doc_batch_len;
                    doc_batch.doc_lens.push(span_json_len as u64);
                }
            }
        }
        let current_span = RuntimeSpan::current();
        current_span.record("num_spans", num_spans);
        current_span.record("num_bytes", doc_batch.concat_docs.len());
        current_span.record("num_parse_errors", num_parse_errors);

        let parsed_spans = ParsedSpans {
            doc_batch,
            num_spans,
            num_parse_errors,
            error_message,
        };
        Ok(parsed_spans)
    }

    #[instrument(skip_all, fields(num_bytes = doc_batch.concat_docs.len()))]
    async fn store_spans(&self, doc_batch: DocBatch) -> Result<(), tonic::Status> {
        let ingest_request = IngestRequest {
            doc_batches: vec![doc_batch],
        };
        self.ingest_api_service
            .ask_for_res(ingest_request)
            .await
            .map_err(|ask_error| {
                error!("Failed to store spans: {ask_error:?}");
                tonic::Status::internal("Failed to store spans.")
            })?;
        Ok(())
    }

    async fn export_instrumented(
        &self,
        request: ExportTraceServiceRequest,
    ) -> Result<ExportTraceServiceResponse, Status> {
        let start = std::time::Instant::now();

        let labels = ["trace", OTEL_TRACE_INDEX_ID, "grpc", "protobuf"];

        OTLP_SERVICE_METRICS
            .requests_total
            .with_label_values(labels)
            .inc();
        let (export_res, is_error) = match self.export_inner(request, labels).await {
            ok @ Ok(_) => (ok, "false"),
            err @ Err(_) => {
                OTLP_SERVICE_METRICS
                    .request_errors_total
                    .with_label_values(labels)
                    .inc();
                (err, "true")
            }
        };
        let elapsed = start.elapsed().as_secs_f64();
        let labels = ["trace", OTEL_TRACE_INDEX_ID, "grpc", "protobuf", is_error];
        OTLP_SERVICE_METRICS
            .request_duration_seconds
            .with_label_values(labels)
            .observe(elapsed);

        export_res
    }
}

#[async_trait]
impl TraceService for OtlpGrpcTraceService {
    #[instrument(name = "ingest_spans", skip_all)]
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let request = request.into_inner();
        self.export_instrumented(request).await.map(Response::new)
    }
}
