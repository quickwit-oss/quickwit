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

use async_trait::async_trait;
use base64::prelude::{Engine, BASE64_STANDARD};
use quickwit_ingest::{
    CommitType, DocBatch, DocBatchBuilder, IngestRequest, IngestService, IngestServiceClient,
};
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsService;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{error, instrument, warn, Span as RuntimeSpan};

use super::{parse_log_record_body, TraceId};
use crate::otlp::extract_attributes;
use crate::otlp::metrics::OTLP_SERVICE_METRICS;

pub const OTEL_LOGS_INDEX_ID: &str = "otel-logs-v0";

pub const OTEL_LOGS_INDEX_CONFIG: &str = r#"
version: 0.5

index_id: otel-logs-v0

doc_mapping:
  mode: strict
  field_mappings:
    - name: timestamp_secs
      type: datetime
      input_formats: [unix_timestamp]
      indexed: false
      fast: true
      precision: seconds
      stored: false
    - name: timestamp_nanos
      type: u64
      indexed: false
    - name: observed_timestamp_nanos
      type: u64
      indexed: false
    - name: service_name
      type: text
      tokenizer: raw
    - name: severity_text
      type: text
      tokenizer: raw
    - name: severity_number
      type: u64
    - name: body
      type: json
    - name: attributes
      type: json
      tokenizer: raw
    - name: dropped_attributes_count
      type: u64
      indexed: false
    - name: trace_id
      type: text
      tokenizer: raw
    - name: span_id
      type: text
      tokenizer: raw
    - name: trace_flags
      type: u64
      indexed: false
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

  timestamp_field: timestamp_secs

  # partition_key: hash_mod(service_name, 100)
  # tag_fields: [service_name]

indexing_settings:
  commit_timeout_secs: 5

search_settings:
  default_search_fields: []
"#;

pub type Base64 = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord {
    pub timestamp_secs: Option<u64>,
    pub timestamp_nanos: u64,
    pub observed_timestamp_nanos: u64,
    pub service_name: String,
    pub severity_text: Option<String>,
    pub severity_number: i32,
    pub body: Option<JsonValue>,
    pub attributes: HashMap<String, JsonValue>,
    pub dropped_attributes_count: u32,
    pub trace_id: Option<TraceId>,
    pub span_id: Option<Base64>,
    pub trace_flags: Option<u32>,
    pub resource_attributes: HashMap<String, JsonValue>,
    pub resource_dropped_attributes_count: u32,
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    pub scope_attributes: HashMap<String, JsonValue>,
    pub scope_dropped_attributes_count: u32,
}

/// A wrapper around `LogRecord` that implements `Ord` to allow insertion of log records into a
/// `BTreeSet`.
#[derive(Debug)]
struct OrdLogRecord(LogRecord);

impl Ord for OrdLogRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .service_name
            .cmp(&other.0.service_name)
            .then(self.0.timestamp_nanos.cmp(&other.0.timestamp_nanos))
    }
}

impl PartialOrd for OrdLogRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for OrdLogRecord {
    fn eq(&self, other: &Self) -> bool {
        self.0.timestamp_nanos == other.0.timestamp_nanos
            && self.0.service_name == other.0.service_name
            && self.0.body == other.0.body
    }
}

impl Eq for OrdLogRecord {}

struct ParsedLogRecords {
    doc_batch: DocBatch,
    num_log_records: u64,
    num_parse_errors: u64,
    error_message: String,
}

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    ingest_service: IngestServiceClient,
}

impl OtlpGrpcLogsService {
    // TODO: remove and use registry
    pub fn new(ingest_service: IngestServiceClient) -> Self {
        Self { ingest_service }
    }

    async fn export_inner(
        &mut self,
        request: ExportLogsServiceRequest,
        labels: [&'static str; 4],
    ) -> Result<ExportLogsServiceResponse, Status> {
        let ParsedLogRecords {
            doc_batch,
            num_log_records,
            num_parse_errors,
            error_message,
        } = tokio::task::spawn_blocking({
            let parent_span = RuntimeSpan::current();
            || Self::parse_logs(request, parent_span)
        })
        .await
        .map_err(|join_error| {
            error!("Failed to parse log records: {join_error:?}");
            Status::internal("Failed to parse log records.")
        })??;
        if num_log_records == num_parse_errors {
            return Err(tonic::Status::internal(error_message));
        }
        let num_bytes = doc_batch.concat_docs.len() as u64;
        self.store_logs(doc_batch).await?;

        OTLP_SERVICE_METRICS
            .ingested_log_records_total
            .with_label_values(labels)
            .inc_by(num_log_records);
        OTLP_SERVICE_METRICS
            .ingested_bytes_total
            .with_label_values(labels)
            .inc_by(num_bytes);

        let response = ExportLogsServiceResponse {
            // `rejected_spans=0` and `error_message=""` is consided a "full" success.
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: num_parse_errors as i64,
                error_message,
            }),
        };
        Ok(response)
    }

    #[instrument(skip_all, parent = parent_span, fields(num_spans = Empty, num_bytes = Empty, num_parse_errors = Empty))]
    fn parse_logs(
        request: ExportLogsServiceRequest,
        parent_span: RuntimeSpan,
    ) -> Result<ParsedLogRecords, Status> {
        let mut log_records = BTreeSet::new();
        let mut num_log_records = 0;
        let mut num_parse_errors = 0;
        let mut error_message = String::new();

        for resource_log in request.resource_logs {
            let mut resource_attributes = extract_attributes(
                resource_log
                    .resource
                    .clone()
                    .map(|rsrc| rsrc.attributes)
                    .unwrap_or_else(Vec::new),
            );
            let resource_dropped_attributes_count = resource_log
                .resource
                .map(|rsrc| rsrc.dropped_attributes_count)
                .unwrap_or(0);

            let service_name = match resource_attributes.remove("service.name") {
                Some(JsonValue::String(value)) => value.to_string(),
                _ => "unknown_service".to_string(),
            };
            for scope_log in resource_log.scope_logs {
                let scope_name = scope_log
                    .scope
                    .as_ref()
                    .map(|scope| &scope.name)
                    .filter(|name| !name.is_empty());
                let scope_version = scope_log
                    .scope
                    .as_ref()
                    .map(|scope| &scope.version)
                    .filter(|version| !version.is_empty());
                let scope_attributes = extract_attributes(
                    scope_log
                        .scope
                        .clone()
                        .map(|scope| scope.attributes)
                        .unwrap_or_else(Vec::new),
                );
                let scope_dropped_attributes_count = scope_log
                    .scope
                    .as_ref()
                    .map(|scope| scope.dropped_attributes_count)
                    .unwrap_or(0);

                for log_record in scope_log.log_records {
                    num_log_records += 1;

                    let timestamp_nanos = log_record.time_unix_nano;
                    let timestamp_secs = Some(timestamp_nanos / 1_000_000_000);
                    let observed_timestamp_nanos = log_record.observed_time_unix_nano;

                    let trace_id = if log_record.trace_id.iter().any(|&byte| byte != 0) {
                        let b64trace_id = TraceId::try_from(log_record.trace_id)
                            .map_err(|error| Status::invalid_argument(error.to_string()))?;
                        Some(b64trace_id)
                    } else {
                        None
                    };
                    let span_id = if log_record.span_id.iter().any(|&byte| byte != 0) {
                        Some(BASE64_STANDARD.encode(log_record.span_id))
                    } else {
                        None
                    };
                    let trace_flags = Some(log_record.flags);

                    let severity_text = if !log_record.severity_text.is_empty() {
                        Some(log_record.severity_text)
                    } else {
                        None
                    };
                    let severity_number = log_record.severity_number;
                    let body = log_record.body.and_then(parse_log_record_body);
                    let attributes = extract_attributes(log_record.attributes);
                    let dropped_attributes_count = log_record.dropped_attributes_count;

                    let log_record = LogRecord {
                        timestamp_secs,
                        timestamp_nanos,
                        observed_timestamp_nanos,
                        service_name: service_name.clone(),
                        severity_text,
                        severity_number,
                        body,
                        attributes,
                        trace_id,
                        span_id,
                        trace_flags,
                        dropped_attributes_count,
                        resource_attributes: resource_attributes.clone(),
                        resource_dropped_attributes_count,
                        scope_name: scope_name.cloned(),
                        scope_version: scope_version.cloned(),
                        scope_attributes: scope_attributes.clone(),
                        scope_dropped_attributes_count,
                    };
                    log_records.insert(OrdLogRecord(log_record));
                }
            }
        }
        let mut doc_batch = DocBatchBuilder::new(OTEL_LOGS_INDEX_ID.to_string()).json_writer();
        for log_record in log_records {
            if let Err(error) = doc_batch.ingest_doc(&log_record.0) {
                error!(error=?error, "Failed to JSON serialize span.");
                error_message = format!("Failed to JSON serialize span: {error:?}");
                num_parse_errors += 1;
            }
        }
        let doc_batch = doc_batch.build();
        let current_span = RuntimeSpan::current();
        current_span.record("num_log_records", num_log_records);
        current_span.record("num_bytes", doc_batch.num_bytes());
        current_span.record("num_parse_errors", num_parse_errors);

        let parsed_spans = ParsedLogRecords {
            doc_batch,
            num_log_records,
            num_parse_errors,
            error_message,
        };
        Ok(parsed_spans)
    }

    #[instrument(skip_all, fields(num_bytes = doc_batch.concat_docs.len()))]
    async fn store_logs(&mut self, doc_batch: DocBatch) -> Result<(), tonic::Status> {
        let ingest_request = IngestRequest {
            doc_batches: vec![doc_batch],
            commit: CommitType::Auto as u32,
        };
        self.ingest_service.ingest(ingest_request).await?;
        Ok(())
    }

    async fn export_instrumented(
        &mut self,
        request: ExportLogsServiceRequest,
    ) -> Result<ExportLogsServiceResponse, Status> {
        let start = std::time::Instant::now();

        let labels = ["logs", OTEL_LOGS_INDEX_ID, "grpc", "protobuf"];

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
        let labels = ["logs", OTEL_LOGS_INDEX_ID, "grpc", "protobuf", is_error];
        OTLP_SERVICE_METRICS
            .request_duration_seconds
            .with_label_values(labels)
            .observe(elapsed);

        export_res
    }
}

#[async_trait]
impl LogsService for OtlpGrpcLogsService {
    #[instrument(name = "ingest_logs", skip_all)]
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let request = request.into_inner();
        self.clone()
            .export_instrumented(request)
            .await
            .map(Response::new)
    }
}
