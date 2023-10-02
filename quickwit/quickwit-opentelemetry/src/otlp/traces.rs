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

use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::collections::{BTreeSet, HashMap};
use std::str::FromStr;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::{load_index_config_from_user_config, ConfigFormat, IndexConfig};
use quickwit_ingest::{
    CommitType, DocBatch, DocBatchBuilder, IngestRequest, IngestService, IngestServiceClient,
};
use quickwit_proto::opentelemetry::proto::collector::trace::v1::trace_service_server::TraceService;
use quickwit_proto::opentelemetry::proto::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use quickwit_proto::opentelemetry::proto::common::v1::InstrumentationScope;
use quickwit_proto::opentelemetry::proto::resource::v1::Resource as OtlpResource;
use quickwit_proto::opentelemetry::proto::trace::v1::span::Link as OtlpLink;
use quickwit_proto::opentelemetry::proto::trace::v1::status::StatusCode as OtlpStatusCode;
use quickwit_proto::opentelemetry::proto::trace::v1::{Span as OtlpSpan, Status as OtlpStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{error, instrument, warn, Span as RuntimeSpan};

use super::is_zero;
use crate::otlp::metrics::OTLP_SERVICE_METRICS;
use crate::otlp::{extract_attributes, SpanId, TraceId};

pub const OTEL_TRACES_INDEX_ID: &str = "otel-traces-v0_6";

const OTEL_TRACES_INDEX_CONFIG: &str = r#"
version: 0.6

index_id: ${INDEX_ID}

doc_mapping:
  mode: strict
  field_mappings:
    - name: trace_id
      type: bytes
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
      type: bytes
    - name: span_kind
      type: u64
    - name: span_name
      type: text
      tokenizer: raw
    - name: span_fingerprint
      type: text
      tokenizer: raw
    - name: span_start_timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
      indexed: false
      fast: true
      precision: milliseconds
    - name: span_end_timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
      indexed: false
      fast: false
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
      indexed: true
    - name: parent_span_id
      type: bytes
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

  timestamp_field: span_start_timestamp_nanos

  # partition_key: hash_mod(service_name, 100)
  # tag_fields: [service_name]

indexing_settings:
  commit_timeout_secs: 5

search_settings:
  default_search_fields: []
"#;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: TraceId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_state: Option<String>,
    pub service_name: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub resource_attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub resource_dropped_attributes_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_version: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub scope_attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub scope_dropped_attributes_count: u32,
    pub span_id: SpanId,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub span_kind: u32,
    pub span_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_fingerprint: Option<SpanFingerprint>,
    pub span_start_timestamp_nanos: u64,
    pub span_end_timestamp_nanos: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_duration_millis: Option<u64>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub span_attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub span_dropped_attributes_count: u32,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub span_dropped_events_count: u32,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub span_dropped_links_count: u32,
    #[serde(default)]
    #[serde(skip_serializing_if = "SpanStatus::is_unset")]
    pub span_status: SpanStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<SpanId>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<Event>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub event_names: Vec<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,
}

impl Span {
    fn from_otlp(span: OtlpSpan, resource: &Resource, scope: &Scope) -> Result<Self, Status> {
        let trace_id = TraceId::try_from(span.trace_id)?;
        let span_id = SpanId::try_from(span.span_id)?;
        let parent_span_id = if !span.parent_span_id.is_empty() {
            Some(SpanId::try_from(span.parent_span_id)?)
        } else {
            None
        };
        let span_name = if !span.name.is_empty() {
            span.name
        } else {
            "unknown".to_string()
        };
        let span_fingerprint =
            SpanFingerprint::new(&resource.service_name, span.kind.into(), &span_name);
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
                event_dropped_attributes_count: event.dropped_attributes_count,
            })
            .collect();
        let event_names: Vec<String> = events
            .iter()
            .map(|event| event.event_name.clone())
            .collect();
        let links: Vec<Link> = span
            .links
            .into_iter()
            .map(Link::try_from_otlp)
            .collect::<Result<_, _>>()?;
        let trace_state = if span.trace_state.is_empty() {
            None
        } else {
            Some(span.trace_state)
        };
        let span = Span {
            trace_id,
            trace_state,
            service_name: resource.service_name.clone(),
            resource_attributes: resource.attributes.clone(),
            resource_dropped_attributes_count: resource.dropped_attributes_count,
            scope_name: scope.name.clone(),
            scope_version: scope.version.clone(),
            scope_attributes: scope.attributes.clone(),
            scope_dropped_attributes_count: scope.dropped_attributes_count,
            span_id,
            span_kind: span.kind as u32,
            span_name,
            span_fingerprint: Some(span_fingerprint),
            span_start_timestamp_nanos: span.start_time_unix_nano,
            span_end_timestamp_nanos: span.end_time_unix_nano,
            span_duration_millis,
            span_attributes,
            span_dropped_attributes_count: span.dropped_attributes_count,
            span_dropped_events_count: span.dropped_events_count,
            span_dropped_links_count: span.dropped_links_count,
            span_status: span.status.map(SpanStatus::from_otlp).unwrap_or_default(),
            parent_span_id,
            events,
            event_names,
            links,
        };
        Ok(span)
    }
}

/// A wrapper around `Span` that implements `Ord` to allow insertion of spans into a `BTreeSet`.
#[derive(Debug)]
struct OrdSpan(Span);

impl Ord for OrdSpan {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .trace_id
            .cmp(&other.0.trace_id)
            .then(self.0.span_name.cmp(&other.0.span_name))
            .then(
                self.0
                    .span_start_timestamp_nanos
                    .cmp(&other.0.span_start_timestamp_nanos),
            )
            .then(self.0.span_id.cmp(&other.0.span_id))
    }
}

impl PartialOrd for OrdSpan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrdSpan {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for OrdSpan {}

#[derive(Debug, Clone)]
pub struct SpanKind(i32);

impl SpanKind {
    pub fn as_char(&self) -> char {
        match self.0 {
            0 => '0',
            1 => '1',
            2 => '2',
            3 => '3',
            4 => '4',
            5 => '5',
            _ => {
                panic!("Unexpected span kind: {}", self.0);
            }
        }
    }
    pub fn as_jaeger(&self) -> &'static str {
        match self.0 {
            0 => "unspecified",
            1 => "internal",
            2 => "server",
            3 => "client",
            4 => "producer",
            5 => "consumer",
            _ => {
                panic!("Unexpected span kind: {}", self.0);
            }
        }
    }

    pub fn as_otlp(&self) -> &'static str {
        match self.0 {
            0 => "SPAN_KIND_UNSPECIFIED",
            1 => "SPAN_KIND_INTERNAL",
            2 => "SPAN_KIND_SERVER",
            3 => "SPAN_KIND_CLIENT",
            4 => "SPAN_KIND_PRODUCER",
            5 => "SPAN_KIND_CONSUMER",
            _ => {
                panic!("Unexpected span kind: {}", self.0);
            }
        }
    }
}

impl From<i32> for SpanKind {
    fn from(span_kind: i32) -> Self {
        Self(span_kind)
    }
}

impl FromStr for SpanKind {
    type Err = String;

    fn from_str(span_kind: &str) -> Result<Self, Self::Err> {
        let span_kind_i32 = match span_kind {
            "0" | "unspecified" | "SPAN_KIND_UNSPECIFIED" => 0,
            "1" | "internal" | "SPAN_KIND_INTERNAL" => 1,
            "2" | "server" | "SPAN_KIND_SERVER" => 2,
            "3" | "client" | "SPAN_KIND_CLIENT" => 3,
            "4" | "producer" | "SPAN_KIND_PRODUCER" => 4,
            "5" | "consumer" | "SPAN_KIND_CONSUMER" => 5,
            _ => {
                if !span_kind.is_empty() {
                    warn!("Unexpected span kind: {}", span_kind);
                }
                return Err(format!("Unexpected span kind: {span_kind}"));
            }
        };
        Ok(Self(span_kind_i32))
    }
}

/// Concatenation of the service name, span kind, and span name.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpanFingerprint(String);

impl SpanFingerprint {
    /// Null character used to separate the service name, span kind, and span name.
    const NULL_CHAR: char = '\u{0}';

    /// Start of heading character, the next character after null.
    const SOH_CHAR: char = '\u{1}';

    pub fn new(service_name: &str, span_kind: SpanKind, span_name: &str) -> Self {
        Self(format!(
            "{service_name}{separator}{span_kind}{separator}{span_name}",
            separator = Self::NULL_CHAR,
            span_kind = span_kind.0
        ))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn from_string(fingerprint: String) -> Self {
        Self(fingerprint)
    }

    pub fn service_name(&self) -> Option<&str> {
        self.0.split(Self::NULL_CHAR).next()
    }

    pub fn span_kind(&self) -> Option<SpanKind> {
        self.0
            .split(Self::NULL_CHAR)
            .nth(1)
            .and_then(|span_kind| SpanKind::from_str(span_kind).ok())
    }

    pub fn span_name(&self) -> Option<&str> {
        self.0.split(Self::NULL_CHAR).nth(2)
    }

    pub fn start_key(service_name: &str, span_kind_opt: Option<SpanKind>) -> Option<Vec<u8>> {
        if service_name.is_empty() {
            return None;
        }
        let mut start_key = service_name.as_bytes().to_vec();
        start_key.push(Self::NULL_CHAR as u8);

        if let Some(span_kind) = span_kind_opt {
            start_key.push(span_kind.as_char() as u8);
            start_key.push(Self::NULL_CHAR as u8);
        }
        Some(start_key)
    }

    pub fn end_key(service_name: &str, span_kind_opt: Option<SpanKind>) -> Option<Vec<u8>> {
        if service_name.is_empty() {
            return None;
        }
        let mut end_key = service_name.as_bytes().to_vec();

        if let Some(span_kind) = span_kind_opt {
            end_key.push(Self::NULL_CHAR as u8);
            end_key.push(span_kind.as_char() as u8);
        }
        end_key.push(Self::SOH_CHAR as u8);
        Some(end_key)
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SpanStatus {
    pub code: OtlpStatusCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl SpanStatus {
    pub fn is_unset(&self) -> bool {
        self.code == OtlpStatusCode::Unset
    }

    fn from_otlp(span_status: OtlpStatus) -> Self {
        if span_status.code() == OtlpStatusCode::Ok {
            Self {
                code: OtlpStatusCode::Ok,
                message: None,
            }
        } else if span_status.code() == OtlpStatusCode::Error {
            let message = if span_status.message.is_empty() {
                None
            } else {
                Some(span_status.message)
            };
            Self {
                code: OtlpStatusCode::Error,
                message,
            }
        } else {
            Self::default()
        }
    }
}

impl Default for SpanStatus {
    fn default() -> Self {
        Self {
            code: OtlpStatusCode::Unset,
            message: None,
        }
    }
}

const UNKNOWN_SERVICE: &str = "unknown_service";

const SERVICE_NAME_KEY: &str = "service.name";

struct Resource {
    service_name: String,
    attributes: HashMap<String, JsonValue>,
    dropped_attributes_count: u32,
}

impl Default for Resource {
    fn default() -> Self {
        Self {
            service_name: UNKNOWN_SERVICE.to_string(),
            attributes: HashMap::new(),
            dropped_attributes_count: 0,
        }
    }
}

impl Resource {
    fn from_otlp(resource: OtlpResource) -> Self {
        let mut attributes = extract_attributes(resource.attributes);
        let service_name = match attributes.remove(SERVICE_NAME_KEY) {
            Some(JsonValue::String(value)) => value,
            _ => UNKNOWN_SERVICE.to_string(),
        };
        Self {
            service_name,
            attributes,
            dropped_attributes_count: resource.dropped_attributes_count,
        }
    }
}

#[derive(Default)]
struct Scope {
    name: Option<String>,
    version: Option<String>,
    attributes: HashMap<String, JsonValue>,
    dropped_attributes_count: u32,
}

impl Scope {
    fn from_otlp(scope: InstrumentationScope) -> Self {
        let name = Some(scope.name).filter(|name| !name.is_empty());
        let version = Some(scope.version).filter(|version| !version.is_empty());
        let attributes = extract_attributes(scope.attributes);
        Self {
            name,
            version,
            attributes,
            dropped_attributes_count: scope.dropped_attributes_count,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub event_timestamp_nanos: u64,
    pub event_name: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub event_attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub event_dropped_attributes_count: u32,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Link {
    pub link_trace_id: TraceId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_trace_state: Option<String>,
    pub link_span_id: SpanId,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub link_attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub link_dropped_attributes_count: u32,
}

impl Link {
    fn try_from_otlp(link: OtlpLink) -> tonic::Result<Link> {
        let link_trace_id = TraceId::try_from(link.trace_id)?;
        let link_span_id = SpanId::try_from(link.span_id)?;
        let link = Link {
            link_trace_id,
            link_trace_state: if !link.trace_state.is_empty() {
                Some(link.trace_state)
            } else {
                None
            },
            link_span_id,
            link_attributes: extract_attributes(link.attributes),
            link_dropped_attributes_count: link.dropped_attributes_count,
        };
        Ok(link)
    }
}

struct ParsedSpans {
    doc_batch: DocBatch,
    num_spans: u64,
    num_parse_errors: u64,
    error_message: String,
}

#[derive(Debug, Clone)]
pub struct OtlpGrpcTracesService {
    ingest_service: IngestServiceClient,
    commit_type: CommitType,
}

impl OtlpGrpcTracesService {
    pub fn new(ingest_service: IngestServiceClient, commit_type_opt: Option<CommitType>) -> Self {
        Self {
            ingest_service,
            commit_type: commit_type_opt.unwrap_or_default(),
        }
    }

    pub fn index_config(default_index_root_uri: &Uri) -> anyhow::Result<IndexConfig> {
        let index_config_str =
            OTEL_TRACES_INDEX_CONFIG.replace("${INDEX_ID}", OTEL_TRACES_INDEX_ID);
        let index_config = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            index_config_str.as_bytes(),
            default_index_root_uri,
        )?;
        Ok(index_config)
    }

    pub async fn export_inner(
        &mut self,
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
        if num_spans == 0 {
            return Err(tonic::Status::invalid_argument("The request is empty."));
        }
        if num_spans == num_parse_errors {
            return Err(tonic::Status::internal(error_message));
        }
        let num_bytes = doc_batch.num_bytes() as u64;
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
        let mut ordered_spans = BTreeSet::new();
        let mut num_spans = 0;
        let mut num_parse_errors = 0;
        let mut error_message = String::new();

        for resource_spans in request.resource_spans {
            let resource = resource_spans
                .resource
                .map(Resource::from_otlp)
                .unwrap_or_default();
            for scope_spans in resource_spans.scope_spans {
                let scope = scope_spans.scope.map(Scope::from_otlp).unwrap_or_default();
                for span in scope_spans.spans {
                    num_spans += 1;
                    let span = Span::from_otlp(span, &resource, &scope)?;
                    ordered_spans.insert(OrdSpan(span));
                }
            }
        }
        let mut doc_batch_builder =
            DocBatchBuilder::new(OTEL_TRACES_INDEX_ID.to_string()).json_writer();
        for span in ordered_spans {
            if let Err(error) = doc_batch_builder.ingest_doc(&span.0) {
                error!(error=?error, "Failed to JSON serialize span.");
                error_message = format!("Failed to JSON serialize span: {error:?}");
                num_parse_errors += 1;
            }
        }
        let doc_batch = doc_batch_builder.build();
        let current_span = RuntimeSpan::current();
        current_span.record("num_spans", num_spans);
        current_span.record("num_bytes", doc_batch.num_bytes());
        current_span.record("num_parse_errors", num_parse_errors);

        let parsed_spans = ParsedSpans {
            doc_batch,
            num_spans,
            num_parse_errors,
            error_message,
        };
        Ok(parsed_spans)
    }

    #[instrument(skip_all, fields(num_bytes = doc_batch.num_bytes()))]
    async fn store_spans(&mut self, doc_batch: DocBatch) -> Result<(), tonic::Status> {
        let ingest_request = IngestRequest {
            doc_batches: vec![doc_batch],
            commit: self.commit_type.into(),
        };
        self.ingest_service.ingest(ingest_request).await?;
        Ok(())
    }

    async fn export_instrumented(
        &mut self,
        request: ExportTraceServiceRequest,
    ) -> Result<ExportTraceServiceResponse, Status> {
        let start = std::time::Instant::now();

        let labels = ["trace", OTEL_TRACES_INDEX_ID, "grpc", "protobuf"];

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
        let labels = ["trace", OTEL_TRACES_INDEX_ID, "grpc", "protobuf", is_error];
        OTLP_SERVICE_METRICS
            .request_duration_seconds
            .with_label_values(labels)
            .observe(elapsed);

        export_res
    }
}

#[async_trait]
impl TraceService for OtlpGrpcTracesService {
    #[instrument(name = "ingest_spans", skip_all)]
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let request = request.into_inner();
        self.clone()
            .export_instrumented(request)
            .await
            .map(Response::new)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_metastore::metastore_for_test;
    use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpAnyValueValue;
    use quickwit_proto::opentelemetry::proto::common::v1::{
        AnyValue as OtlpAnyValue, KeyValue as OtlpKeyValue,
    };
    use quickwit_proto::opentelemetry::proto::trace::v1::span::{
        Event as OtlpEvent, Link as OtlpLink,
    };
    use serde_json::json;

    use super::*;

    #[test]
    fn test_index_config_is_valid() {
        let index_config =
            OtlpGrpcTracesService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        assert_eq!(index_config.index_id, OTEL_TRACES_INDEX_ID);
    }

    #[tokio::test]
    async fn test_create_index() {
        let metastore = metastore_for_test();
        let index_config =
            OtlpGrpcTracesService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        metastore.create_index(index_config).await.unwrap();
    }

    #[test]
    fn test_resource_from_otlp() {
        let otlp_resource = OtlpResource {
            attributes: vec![
                OtlpKeyValue {
                    key: "service.name".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue("quickwit".to_string())),
                    }),
                },
                OtlpKeyValue {
                    key: "key".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue("value".to_string())),
                    }),
                },
            ],
            dropped_attributes_count: 1,
        };
        let resource = Resource::from_otlp(otlp_resource);
        assert_eq!(
            resource.attributes,
            HashMap::from_iter([("key".to_string(), json!("value"))])
        );
        assert_eq!(resource.service_name, "quickwit");
        assert_eq!(resource.dropped_attributes_count, 1);
    }

    #[test]
    fn test_scope_from_otlp() {
        let otlp_scope = InstrumentationScope {
            name: "opentelemetry-otlp".to_string(),
            version: "0.11.0".to_string(),
            attributes: vec![OtlpKeyValue {
                key: "key".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::StringValue("value".to_string())),
                }),
            }],
            dropped_attributes_count: 1,
        };
        let scope = Scope::from_otlp(otlp_scope);
        assert_eq!(scope.name.unwrap(), "opentelemetry-otlp");
        assert_eq!(scope.version.unwrap(), "0.11.0");
        assert_eq!(
            scope.attributes,
            HashMap::from_iter([("key".to_string(), json!("value"))])
        );
        assert_eq!(scope.dropped_attributes_count, 1);
    }

    #[test]
    fn test_span_from_otlp() {
        {
            // Test minimal span.
            let otlp_span = OtlpSpan {
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                parent_span_id: vec![3; 8],
                trace_state: "".to_string(),
                name: "publish_split".to_string(),
                kind: 2, // Server
                start_time_unix_nano: 1_000_000_001,
                end_time_unix_nano: 1_001_000_002,
                attributes: Vec::new(),
                dropped_attributes_count: 3,
                events: Vec::new(),
                dropped_events_count: 4,
                links: Vec::new(),
                dropped_links_count: 5,
                status: None,
            };
            let span = Span::from_otlp(otlp_span, &Resource::default(), &Scope::default()).unwrap();

            assert_eq!(span.service_name, UNKNOWN_SERVICE);
            assert!(span.resource_attributes.is_empty());
            assert_eq!(span.resource_dropped_attributes_count, 0);

            assert!(span.scope_name.is_none());
            assert!(span.scope_version.is_none());
            assert!(span.scope_attributes.is_empty());
            assert_eq!(span.scope_dropped_attributes_count, 0);

            assert_eq!(span.trace_id, TraceId::new([1; 16]));
            assert!(span.trace_state.is_none());

            assert_eq!(span.parent_span_id, Some(SpanId::new([3; 8])));
            assert_eq!(span.span_id, SpanId::new([2; 8]));
            assert_eq!(span.span_kind, 2);
            assert_eq!(span.span_name, "publish_split");
            assert_eq!(
                span.span_fingerprint.unwrap(),
                SpanFingerprint::new(UNKNOWN_SERVICE, SpanKind(2), "publish_split")
            );
            assert_eq!(span.span_start_timestamp_nanos, 1_000_000_001);
            assert_eq!(span.span_end_timestamp_nanos, 1_001_000_002);
            assert_eq!(span.span_duration_millis.unwrap(), 1);
            assert!(span.span_attributes.is_empty());
            assert_eq!(span.span_dropped_attributes_count, 3);
            assert_eq!(span.span_status.code, OtlpStatusCode::Unset);

            assert!(span.events.is_empty());
            assert!(span.event_names.is_empty());
            assert_eq!(span.span_dropped_events_count, 4);

            assert!(span.links.is_empty());
            assert_eq!(span.span_dropped_links_count, 5);
        }
        {
            let resource = Resource {
                service_name: "quickwit".to_string(),
                attributes: HashMap::from_iter([(
                    "resource_key".to_string(),
                    json!("resource_value"),
                )]),
                dropped_attributes_count: 1,
            };
            let scope = Scope {
                name: Some("opentelemetry-otlp".to_string()),
                version: Some("0.11.0".to_string()),
                attributes: HashMap::from_iter([("scope_key".to_string(), json!("scope_value"))]),
                dropped_attributes_count: 2,
            };

            let events = vec![OtlpEvent {
                name: "event_name".to_string(),
                time_unix_nano: 1_000_500_003,
                attributes: vec![OtlpKeyValue {
                    key: "event_key".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue("event_value".to_string())),
                    }),
                }],
                dropped_attributes_count: 6,
            }];
            let links = vec![OtlpLink {
                trace_id: vec![4; 16],
                span_id: vec![5; 8],
                trace_state: "link_key1=link_value1,link_key2=link_value2".to_string(),
                attributes: vec![OtlpKeyValue {
                    key: "link_key".to_string(),
                    value: Some(OtlpAnyValue {
                        value: Some(OtlpAnyValueValue::StringValue("link_value".to_string())),
                    }),
                }],
                dropped_attributes_count: 7,
            }];
            let attributes = vec![OtlpKeyValue {
                key: "span_key".to_string(),
                value: Some(OtlpAnyValue {
                    value: Some(OtlpAnyValueValue::StringValue("span_value".to_string())),
                }),
            }];
            let otlp_span = OtlpSpan {
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                parent_span_id: vec![3; 8],
                trace_state: "key1=value1,key2=value2".to_string(),
                name: "publish_split".to_string(),
                kind: 2, // Server
                start_time_unix_nano: 1_000_000_001,
                end_time_unix_nano: 1_001_000_002,
                attributes,
                dropped_attributes_count: 3,
                events,
                dropped_events_count: 4,
                links,
                dropped_links_count: 5,
                status: Some(OtlpStatus {
                    code: 2,
                    message: "An error occurred.".to_string(),
                }),
            };
            let span = Span::from_otlp(otlp_span, &resource, &scope).unwrap();

            assert_eq!(span.service_name, "quickwit");
            assert_eq!(
                span.resource_attributes,
                HashMap::from_iter([("resource_key".to_string(), json!("resource_value"))],)
            );
            assert_eq!(span.resource_dropped_attributes_count, 1);

            assert_eq!(span.scope_name.unwrap(), "opentelemetry-otlp");
            assert_eq!(span.scope_version.unwrap(), "0.11.0");
            assert_eq!(
                span.scope_attributes,
                HashMap::from_iter([("scope_key".to_string(), json!("scope_value"))])
            );
            assert_eq!(span.scope_dropped_attributes_count, 2);

            assert_eq!(span.trace_id, TraceId::new([1; 16]));
            assert_eq!(span.trace_state.unwrap(), "key1=value1,key2=value2");

            assert_eq!(span.parent_span_id, Some(SpanId::new([3; 8])));
            assert_eq!(span.span_id, SpanId::new([2; 8]));
            assert_eq!(span.span_kind, 2);
            assert_eq!(span.span_name, "publish_split");
            assert_eq!(
                span.span_fingerprint.unwrap(),
                SpanFingerprint::new("quickwit", SpanKind(2), "publish_split")
            );
            assert_eq!(span.span_start_timestamp_nanos, 1_000_000_001);
            assert_eq!(span.span_end_timestamp_nanos, 1_001_000_002);
            assert_eq!(span.span_duration_millis.unwrap(), 1);
            assert_eq!(
                span.span_attributes,
                HashMap::from_iter([("span_key".to_string(), json!("span_value"))])
            );
            assert_eq!(span.span_dropped_attributes_count, 3);
            assert_eq!(span.span_status.code, OtlpStatusCode::Error);
            assert_eq!(span.span_status.message.unwrap(), "An error occurred.");

            assert_eq!(
                span.events,
                vec![Event {
                    event_name: "event_name".to_string(),
                    event_timestamp_nanos: 1_000_500_003,
                    event_attributes: HashMap::from_iter([(
                        "event_key".to_string(),
                        json!("event_value")
                    )]),
                    event_dropped_attributes_count: 6,
                }]
            );
            assert_eq!(span.event_names, vec!["event_name".to_string()]);
            assert_eq!(span.span_dropped_events_count, 4);

            assert_eq!(
                span.links,
                vec![Link {
                    link_trace_id: TraceId::new([4; 16]),
                    link_span_id: SpanId::new([5; 8]),
                    link_trace_state: Some(
                        "link_key1=link_value1,link_key2=link_value2".to_string()
                    ),
                    link_attributes: HashMap::from_iter([(
                        "link_key".to_string(),
                        json!("link_value")
                    )]),
                    link_dropped_attributes_count: 7,
                }]
            );
            assert_eq!(span.span_dropped_links_count, 5);
        }
    }

    #[test]
    fn test_span_fingerprint() {
        let span_fingerprint = SpanFingerprint::new("quickwit", SpanKind(2), "publish_split");
        assert_eq!(
            span_fingerprint.as_str(),
            "quickwit\u{0}2\u{0}publish_split"
        );

        let start_key_opt = SpanFingerprint::start_key("", None);
        assert!(start_key_opt.is_none());

        let start_key = SpanFingerprint::start_key("quickwit", None)
            .map(String::from_utf8)
            .unwrap()
            .unwrap();
        assert_eq!(start_key, "quickwit\u{0}");
        let end_key = SpanFingerprint::end_key("quickwit", None)
            .map(String::from_utf8)
            .unwrap()
            .unwrap();
        assert_eq!(end_key, "quickwit\u{1}");

        let start_key = SpanFingerprint::start_key("quickwit", Some(SpanKind::from(1)))
            .map(String::from_utf8)
            .unwrap()
            .unwrap();
        assert_eq!(start_key, "quickwit\u{0}1\u{0}");
        let end_key = SpanFingerprint::end_key("quickwit", Some(SpanKind::from(1)))
            .map(String::from_utf8)
            .unwrap()
            .unwrap();
        assert_eq!(end_key, "quickwit\u{0}1\u{1}");
    }

    #[test]
    fn test_span_status_from_otlp() {
        let otlp_status = OtlpStatus {
            code: 0,
            message: "".to_string(),
        };
        assert!(SpanStatus::from_otlp(otlp_status).is_unset());

        let otlp_status = OtlpStatus {
            code: 1,
            message: "".to_string(),
        };
        let span_status = SpanStatus::from_otlp(otlp_status);
        assert_eq!(span_status.code, OtlpStatusCode::Ok);
        assert!(span_status.message.is_none());

        let otlp_status = OtlpStatus {
            code: 2,
            message: "An error occurred.".to_string(),
        };
        let span_status = SpanStatus::from_otlp(otlp_status);
        assert_eq!(span_status.code, OtlpStatusCode::Error);
        assert_eq!(span_status.message.unwrap(), "An error occurred.");
    }

    #[test]
    fn test_span_serde() {
        {
            let expected_span = Span {
                trace_id: TraceId::new([1; 16]),
                trace_state: None,
                service_name: "quickwit".to_string(),
                resource_attributes: HashMap::new(),
                resource_dropped_attributes_count: 0,
                scope_name: None,
                scope_version: None,
                scope_attributes: HashMap::new(),
                scope_dropped_attributes_count: 0,
                span_id: SpanId::new([2; 8]),
                span_kind: 0,
                span_name: "publish_split".to_string(),
                span_fingerprint: Some(SpanFingerprint::new(
                    "quickwit",
                    SpanKind(2),
                    "publish_splits",
                )),
                span_start_timestamp_nanos: 0,
                span_end_timestamp_nanos: 1_000,
                span_duration_millis: Some(1),
                span_attributes: HashMap::new(),
                span_dropped_attributes_count: 0,
                span_dropped_events_count: 0,
                span_dropped_links_count: 0,
                span_status: SpanStatus::default(),
                parent_span_id: None,
                events: Vec::new(),
                event_names: Vec::new(),
                links: Vec::new(),
            };
            let span_json = serde_json::to_string_pretty(&expected_span).unwrap();
            let span = serde_json::from_str::<Span>(&span_json).unwrap();
            assert_eq!(span, expected_span);
        }
        {
            let expected_span = Span {
                trace_id: TraceId::new([1; 16]),
                trace_state: Some("key1=value1,key2=value2".to_string()),
                service_name: "quickwit".to_string(),
                resource_attributes: HashMap::from([(
                    "resource_key".to_string(),
                    json!("resource_value"),
                )]),
                resource_dropped_attributes_count: 1,
                scope_name: Some("scope_name".to_string()),
                scope_version: Some("scope_version".to_string()),
                scope_attributes: HashMap::from([("scope_key".to_string(), json!("scope_value"))]),
                scope_dropped_attributes_count: 1,
                span_id: SpanId::new([2; 8]),
                span_kind: 1,
                span_name: "publish_split".to_string(),
                span_fingerprint: Some(SpanFingerprint::new(
                    "quickwit",
                    SpanKind(2),
                    "publish_splits",
                )),
                span_start_timestamp_nanos: 0,
                span_end_timestamp_nanos: 1_000,
                span_duration_millis: Some(1),
                span_attributes: HashMap::from([("span_key".to_string(), json!("span_value"))]),
                span_dropped_attributes_count: 1,
                span_dropped_events_count: 1,
                span_dropped_links_count: 1,
                span_status: SpanStatus {
                    code: OtlpStatusCode::Ok,
                    message: None,
                },
                parent_span_id: Some(SpanId::new([3; 8])),
                events: vec![Event {
                    event_timestamp_nanos: 1,
                    event_name: "event_name".to_string(),
                    event_attributes: HashMap::new(),
                    event_dropped_attributes_count: 0,
                }],
                event_names: vec!["event_name".to_string()],
                links: vec![Link {
                    link_trace_id: TraceId::new([1; 16]),
                    link_span_id: SpanId::new([4; 8]),
                    link_trace_state: None,
                    link_attributes: HashMap::new(),
                    link_dropped_attributes_count: 0,
                }],
            };
            let span_json = serde_json::to_string_pretty(&expected_span).unwrap();
            let span = serde_json::from_str::<Span>(&span_json).unwrap();
            assert_eq!(span, expected_span);
        }
    }
}
