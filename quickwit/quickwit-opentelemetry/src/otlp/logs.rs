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
use quickwit_actors::Mailbox;
use quickwit_ingest_api::IngestApiService;
use quickwit_proto::ingest_api::{DocBatch, IngestRequest};
use quickwit_proto::opentelemetry::proto::collector::logs::v1::logs_service_server::LogsService;
use quickwit_proto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::otlp::extract_attributes;

const OTEL_LOG_INDEX_ID: &str = "otel-log-v0";

#[derive(Clone)]
pub struct OtlpGrpcLogsService {
    ingest_api_service: Mailbox<IngestApiService>,
}

impl OtlpGrpcLogsService {
    // TODO: remove and use registry
    pub fn new(ingest_api_service: Mailbox<IngestApiService>) -> Self {
        Self { ingest_api_service }
    }
}

#[derive(Debug, Serialize)]
struct LogEvent {
    timestamp: i64,
    attributes: HashMap<String, JsonValue>,
    resource: HashMap<String, JsonValue>,
    body: Option<String>,
    severity: String,
}

#[async_trait]
impl LogsService for OtlpGrpcLogsService {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        let request = request.into_inner();
        let mut doc_batch = DocBatch {
            index_id: OTEL_LOG_INDEX_ID.to_string(),
            ..Default::default()
        };

        for resource_log in request.resource_logs {
            let resource: HashMap<String, JsonValue> = resource_log
                .resource
                .map(|resource| extract_attributes(resource.attributes))
                .unwrap_or_default();
            for scope_log in resource_log.scope_logs {
                for log_record in scope_log.log_records {
                    let body = log_record.body.and_then(|body| match body.value {
                        Some(Value::StringValue(inner)) => Some(inner),
                        _ => None,
                    });
                    let severity_text = log_record.severity_text;
                    let timestamp = if log_record.time_unix_nano != 0 {
                        // Quickwit supports only microseconds.
                        (log_record.time_unix_nano / 1000) as i64
                    } else {
                        (log_record.observed_time_unix_nano / 1000) as i64
                    };
                    let attributes = extract_attributes(log_record.attributes);
                    let log_event = LogEvent {
                        timestamp,
                        attributes,
                        body,
                        severity: severity_text,
                        resource: resource.clone(),
                    };
                    let log_event_json = serde_json::to_vec(&log_event)
                        .expect("`LogEvent` are serializable, this should never happened.");
                    let log_event_json_len = log_event_json.len() as u64;
                    doc_batch.concat_docs.extend_from_slice(&log_event_json);
                    doc_batch.doc_lens.push(log_event_json_len);
                }
            }
        }

        let ingest_request = IngestRequest {
            doc_batches: vec![doc_batch],
        };

        self.ingest_api_service
            .ask_for_res(ingest_request)
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;

        Ok(tonic::Response::new(ExportLogsServiceResponse::default()))
    }
}
