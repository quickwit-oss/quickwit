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

use async_trait::async_trait;
use prost::Message;
use quickwit_common::thread_pool::run_cpu_intensive;
use quickwit_common::uri::Uri;
use quickwit_config::{ConfigFormat, IndexConfig, load_index_config_from_user_config};
use quickwit_ingest::{CommitType, JsonDocBatchV2Builder};
use quickwit_proto::ingest::DocBatchV2;
use quickwit_proto::ingest::router::IngestRouterServiceClient;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsService;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use quickwit_proto::types::{DocUidGenerator, IndexId};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use time::OffsetDateTime;
use tonic::{Request, Response, Status};
use tracing::field::Empty;
use tracing::{Span as RuntimeSpan, error, instrument, warn};

use super::{
    OtelSignal, SpanId, TraceId, TryFromSpanIdError, TryFromTraceIdError,
    extract_otel_index_id_from_metadata, ingest_doc_batch_v2, is_zero, parse_log_record_body,
};
use crate::otlp::extract_attributes;
use crate::otlp::metrics::OTLP_SERVICE_METRICS;

pub const OTEL_LOGS_INDEX_ID: &str = "otel-logs-v0_9";

const OTEL_LOGS_INDEX_CONFIG: &str = r#"
version: 0.8

index_id: ${INDEX_ID}

doc_mapping:
  mode: strict
  field_mappings:
    - name: timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
      indexed: false
      fast: true
      fast_precision: milliseconds
    - name: observed_timestamp_nanos
      type: datetime
      input_formats: [unix_timestamp]
      output_format: unix_timestamp_nanos
    - name: service_name
      type: text
      tokenizer: raw
      fast: true
    - name: severity_text
      type: text
      tokenizer: raw
      fast: true
    - name: severity_number
      type: u64
      fast: true
    - name: body
      type: json
      tokenizer: default
    - name: attributes
      type: json
      tokenizer: raw
      fast: true
    - name: dropped_attributes_count
      type: u64
      indexed: false
    - name: trace_id
      type: bytes
      input_format: hex
      output_format: hex
    - name: span_id
      type: bytes
      input_format: hex
      output_format: hex
    - name: trace_flags
      type: u64
      indexed: false
    - name: resource_attributes
      type: json
      tokenizer: raw
      fast: true
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

  timestamp_field: timestamp_nanos

  # partition_key: hash_mod(service_name, 100)
  # tag_fields: [service_name]

indexing_settings:
  commit_timeout_secs: 5

search_settings:
  default_search_fields: [body.message]
"#;

#[derive(Debug, thiserror::Error)]
pub enum OtlpLogsError {
    #[error("failed to deserialize JSON log records: `{0}`")]
    Json(#[from] serde_json::Error),
    #[error("failed to deserialize Protobuf log records: `{0}`")]
    Protobuf(#[from] prost::DecodeError),
    #[error("failed to parse log record: `{0}`")]
    SpanId(#[from] TryFromSpanIdError),
    #[error("failed to parse log record: `{0}`")]
    TraceId(#[from] TryFromTraceIdError),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord {
    pub timestamp_nanos: u64,
    pub observed_timestamp_nanos: u64,
    #[serde(default)]
    #[serde(skip_serializing_if = "String::is_empty")]
    pub service_name: String,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity_text: Option<String>,
    pub severity_number: i32,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub dropped_attributes_count: u32,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<TraceId>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<SpanId>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_flags: Option<u32>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub resource_attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub resource_dropped_attributes_count: u32,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_name: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_version: Option<String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub scope_attributes: HashMap<String, JsonValue>,
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub scope_dropped_attributes_count: u32,
}

struct ParsedLogRecords {
    doc_batch: DocBatchV2,
    num_log_records: u64,
    num_parse_errors: u64,
    error_message: String,
}

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    ingest_router: IngestRouterServiceClient,
}

impl OtlpGrpcLogsService {
    pub fn new(ingest_router: IngestRouterServiceClient) -> Self {
        Self { ingest_router }
    }

    pub fn index_config(default_index_root_uri: &Uri) -> anyhow::Result<IndexConfig> {
        let index_config_str = OTEL_LOGS_INDEX_CONFIG.replace("${INDEX_ID}", OTEL_LOGS_INDEX_ID);
        let index_config = load_index_config_from_user_config(
            ConfigFormat::Yaml,
            index_config_str.as_bytes(),
            default_index_root_uri,
        )?;
        Ok(index_config)
    }

    async fn export_inner(
        &mut self,
        request: ExportLogsServiceRequest,
        index_id: IndexId,
        labels: [&str; 4],
    ) -> Result<ExportLogsServiceResponse, Status> {
        let ParsedLogRecords {
            doc_batch,
            num_log_records,
            num_parse_errors,
            error_message,
        } = run_cpu_intensive({
            let parent_span = RuntimeSpan::current();
            || Self::parse_logs(request, parent_span)
        })
        .await
        .map_err(|join_error| {
            error!(error=?join_error, "failed to parse log records");
            Status::internal("failed to parse log records")
        })??;
        if num_log_records == num_parse_errors {
            return Err(tonic::Status::internal(error_message));
        }
        let num_bytes = doc_batch.num_bytes() as u64;
        self.store_logs(index_id, doc_batch).await?;

        OTLP_SERVICE_METRICS
            .ingested_log_records_total
            .with_label_values(labels)
            .inc_by(num_log_records);
        OTLP_SERVICE_METRICS
            .ingested_bytes_total
            .with_label_values(labels)
            .inc_by(num_bytes);

        let response = ExportLogsServiceResponse {
            // `rejected_log_records=0` and `error_message=""` is consided a "full" success.
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: num_parse_errors as i64,
                error_message,
            }),
        };
        Ok(response)
    }

    #[instrument(skip_all, parent = parent_span, fields(num_log_records = Empty, num_bytes = Empty, num_parse_errors = Empty))]
    #[allow(clippy::result_large_err)]
    fn parse_logs(
        request: ExportLogsServiceRequest,
        parent_span: RuntimeSpan,
    ) -> tonic::Result<ParsedLogRecords> {
        let log_records = parse_otlp_logs(request)?;
        let mut num_parse_errors = 0;
        let num_log_records = log_records.len() as u64;
        let mut error_message = String::new();

        let mut doc_batch_builder = JsonDocBatchV2Builder::with_num_docs(num_log_records as usize);
        let mut doc_uid_generator = DocUidGenerator::default();
        for log_record in log_records {
            let doc_uid = doc_uid_generator.next_doc_uid();
            if let Err(error) = doc_batch_builder.add_doc(doc_uid, log_record) {
                error!(error=?error, "failed to JSON serialize span");
                error_message = format!("failed to JSON serialize span: {error:?}");
                num_parse_errors += 1;
            }
        }
        let doc_batch = doc_batch_builder.build();
        let current_span = RuntimeSpan::current();
        current_span.record("num_log_records", num_log_records);
        current_span.record("num_bytes", doc_batch.num_bytes());
        current_span.record("num_parse_errors", num_parse_errors);

        let parsed_logs = ParsedLogRecords {
            doc_batch,
            num_log_records,
            num_parse_errors,
            error_message,
        };
        Ok(parsed_logs)
    }

    #[instrument(skip_all, fields(num_bytes = doc_batch.num_bytes()))]
    async fn store_logs(
        &mut self,
        index_id: String,
        doc_batch: DocBatchV2,
    ) -> Result<(), tonic::Status> {
        ingest_doc_batch_v2(
            self.ingest_router.clone(),
            index_id,
            doc_batch,
            CommitType::Auto,
        )
        .await?;
        Ok(())
    }

    async fn export_instrumented(
        &mut self,
        request: ExportLogsServiceRequest,
        index_id: IndexId,
    ) -> Result<ExportLogsServiceResponse, Status> {
        let start = std::time::Instant::now();

        let labels = ["logs", &index_id, "grpc", "protobuf"];

        OTLP_SERVICE_METRICS
            .requests_total
            .with_label_values(labels)
            .inc();
        let (export_res, is_error) =
            match self.export_inner(request, index_id.clone(), labels).await {
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
        let labels = ["logs", &index_id, "grpc", "protobuf", is_error];
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
        let index_id = extract_otel_index_id_from_metadata(request.metadata(), OtelSignal::Logs)?;
        let request = request.into_inner();
        self.clone()
            .export_instrumented(request, index_id)
            .await
            .map(Response::new)
    }
}

fn parse_otlp_logs(request: ExportLogsServiceRequest) -> Result<Vec<LogRecord>, OtlpLogsError> {
    let num_log_records = request
        .resource_logs
        .iter()
        .flat_map(|resource_log| resource_log.scope_logs.iter())
        .map(|scope_logs| scope_logs.log_records.len())
        .sum();
    let mut log_records = Vec::with_capacity(num_log_records);

    for resource_logs in request.resource_logs {
        let mut resource_attributes = extract_attributes(
            resource_logs
                .resource
                .clone()
                .map(|rsrc| rsrc.attributes)
                .unwrap_or_default(),
        );
        let resource_dropped_attributes_count = resource_logs
            .resource
            .map(|rsrc| rsrc.dropped_attributes_count)
            .unwrap_or(0);

        let service_name = match resource_attributes.remove("service.name") {
            Some(JsonValue::String(value)) => value.to_string(),
            _ => "unknown_service".to_string(),
        };
        for scope_logs in resource_logs.scope_logs {
            let scope_name = scope_logs
                .scope
                .as_ref()
                .map(|scope| &scope.name)
                .filter(|name| !name.is_empty());
            let scope_version = scope_logs
                .scope
                .as_ref()
                .map(|scope| &scope.version)
                .filter(|version| !version.is_empty());
            let scope_attributes = extract_attributes(
                scope_logs
                    .scope
                    .clone()
                    .map(|scope| scope.attributes)
                    .unwrap_or_default(),
            );
            let scope_dropped_attributes_count = scope_logs
                .scope
                .as_ref()
                .map(|scope| scope.dropped_attributes_count)
                .unwrap_or(0);

            for log_record in scope_logs.log_records {
                let observed_timestamp_nanos = if log_record.observed_time_unix_nano == 0 {
                    // As per OTEL model spec, this field SHOULD be set once the
                    // event is observed by OpenTelemetry. If it's not set, we
                    // consider ourselves as the first OTEL observers.
                    OffsetDateTime::now_utc().unix_timestamp_nanos() as u64
                } else {
                    log_record.observed_time_unix_nano
                };

                let timestamp_nanos = if log_record.time_unix_nano == 0 {
                    observed_timestamp_nanos
                } else {
                    // When only one timestamp is supported by a recipients, the
                    // OTEL spec recommends using the `Timestamp` field if
                    // present, otherwise `ObservedTimestamp`. Even though our
                    // model supports multiple timestamps, we have only one
                    // field that that can be our `timestamp_field` and it
                    // should be the one that is commonly used for queries.
                    log_record.time_unix_nano
                };

                let trace_id = if log_record.trace_id.iter().any(|&byte| byte != 0) {
                    let trace_id = TraceId::try_from(log_record.trace_id)?;
                    Some(trace_id)
                } else {
                    None
                };
                let span_id = if log_record.span_id.iter().any(|&byte| byte != 0) {
                    let span_id = SpanId::try_from(log_record.span_id)?;
                    Some(span_id)
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
                log_records.push(log_record);
            }
        }
    }
    Ok(log_records)
}

/// An iterator of JSON OTLP log records for use in the doc processor.
pub struct JsonLogIterator {
    logs: std::vec::IntoIter<LogRecord>,
    current_log_idx: usize,
    num_logs: usize,
    avg_log_size: usize,
    avg_log_size_rem: usize,
}

impl JsonLogIterator {
    fn new(logs: Vec<LogRecord>, num_bytes: usize) -> Self {
        let num_logs = logs.len();
        let avg_log_size = num_bytes.checked_div(num_logs).unwrap_or(0);
        let avg_log_size_rem = avg_log_size + num_bytes.checked_rem(num_logs).unwrap_or(0);

        Self {
            logs: logs.into_iter(),
            current_log_idx: 0,
            num_logs,
            avg_log_size,
            avg_log_size_rem,
        }
    }
}

impl Iterator for JsonLogIterator {
    type Item = (JsonValue, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let log_opt = self
            .logs
            .next()
            .map(|log| serde_json::to_value(log).expect("`LogRecord` should be JSON serializable"));
        if log_opt.is_some() {
            self.current_log_idx += 1;
        }
        if self.current_log_idx < self.num_logs {
            log_opt.map(|span| (span, self.avg_log_size))
        } else {
            log_opt.map(|span| (span, self.avg_log_size_rem))
        }
    }
}

pub fn parse_otlp_logs_json(payload_json: &[u8]) -> Result<JsonLogIterator, OtlpLogsError> {
    let request: ExportLogsServiceRequest = serde_json::from_slice(payload_json)?;
    let log_records = parse_otlp_logs(request)?;
    Ok(JsonLogIterator::new(log_records, payload_json.len()))
}

pub fn parse_otlp_logs_protobuf(payload_proto: &[u8]) -> Result<JsonLogIterator, OtlpLogsError> {
    let request = ExportLogsServiceRequest::decode(payload_proto)?;
    let log_records = parse_otlp_logs(request)?;
    Ok(JsonLogIterator::new(log_records, payload_proto.len()))
}

#[cfg(test)]
mod tests {
    use quickwit_metastore::{CreateIndexRequestExt, metastore_for_test};
    use quickwit_proto::metastore::{CreateIndexRequest, MetastoreService};

    use super::*;

    #[test]
    fn test_index_config_is_valid() {
        let index_config =
            OtlpGrpcLogsService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        assert_eq!(index_config.index_id, OTEL_LOGS_INDEX_ID);
    }

    #[tokio::test]
    async fn test_create_index() {
        let metastore = metastore_for_test();
        let index_config =
            OtlpGrpcLogsService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        let create_index_request =
            CreateIndexRequest::try_from_index_config(&index_config).unwrap();
        metastore.create_index(create_index_request).await.unwrap();
    }
}
