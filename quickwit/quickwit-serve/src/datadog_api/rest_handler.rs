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
use std::time::{SystemTime, UNIX_EPOCH};

use quickwit_common::rate_limited_error;
use quickwit_config::INGEST_V2_SOURCE_ID;
use quickwit_datetime::{parse_date_time_str, parse_timestamp, DateTimeInputFormat};
use quickwit_ingest::DocBatchV2Builder;
use quickwit_proto::ingest::router::{
    IngestRequestV2, IngestRouterService, IngestRouterServiceClient, IngestSubrequest,
};
use quickwit_proto::ingest::CommitTypeV2;
use quickwit_proto::types::{DocUidGenerator, IndexId};
use quickwit_proto::{ServiceError, ServiceErrorCode};
use serde::{self, Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;
use tracing::{debug, error, warn};
use uuid::Uuid;
use warp::{Filter, Rejection};

use super::{convert_tags, normalize_fields, NormalizeField, StringOrVec};
use crate::decompression::get_body_bytes;
use crate::rest_api_response::into_rest_api_response;
use crate::simple_list::from_simple_list;
use crate::{with_arg, Body, BodyFormat};

const DATADOG_INDEX_ID: &str = "datadog";

#[derive(utoipa::OpenApi)]
#[openapi(paths(datadog_logs,))]
pub struct DatadogApi;

pub(crate) fn datadog_ingest_api_handlers(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_logs(ingest_router.clone()).boxed()
}

/// Based on vector agent logs endpoint:
/// https://github.com/vectordotdev/vector/blob/450de36904f3d1524057e8cdb736941194da8d22/src/sources/datadog_agent/mod.rs#L499
pub(crate) fn datadog_filter() -> impl Filter<Extract = (Body,), Error = Rejection> + Clone {
    let path_filter = warp::path!("api" / "v1" / "input")
        .or(warp::path!("api" / "v2" / "logs"))
        .unify();
    path_filter
        .and(warp::post())
        .and(warp::header::exact_ignore_case(
            "content-type",
            "application/json",
        ))
        .and(get_body_bytes())
}

#[utoipa::path(
    post,
    tag = "Datadog Logs",
    path = "/api/v2/logs",
    request_body(content = String, description = "Datadog Log JSON message", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully exported logs.", body = bool),
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add docs to."),
    )
)]
pub(crate) fn datadog_logs(
    ingest_router: IngestRouterServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    datadog_filter()
        .and(with_arg(ingest_router))
        .and(warp::post())
        .then(|body, ingest_router| async move {
            datadog_ingest_logs(ingest_router, DATADOG_INDEX_ID.to_string(), body).await
        })
        .and(with_arg(BodyFormat::default()))
        .map(into_rest_api_response)
        .boxed()
}

#[derive(Debug, Clone, thiserror::Error, Serialize)]
pub enum DatadogApiError {
    #[error("invalid datadog log request: {0}")]
    InvalidPayload(String),
    #[error("error when ingesting payload: {0}")]
    Ingest(String),
    #[error("Datadog Log Preprocessing Panicked: {0}")]
    Panicked(String),
}

impl ServiceError for DatadogApiError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            DatadogApiError::Panicked(_) => ServiceErrorCode::Internal,
            DatadogApiError::InvalidPayload(_) => ServiceErrorCode::BadRequest,
            DatadogApiError::Ingest(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "datadog internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
        }
    }
}

fn deserialize_datadog_log(data: &[u8]) -> Result<Vec<DatadogLogMsg>, DatadogApiError> {
    serde_json::from_slice(data).map_err(|error| {
        error!(
            message = "Failed to parse datadog logs.",
            internal_log_rate_limit = true,
            error = ?error
        );
        DatadogApiError::InvalidPayload(format!("Error parsing JSON: {:?}", error))
    })
}

async fn datadog_ingest_logs(
    ingest_router: IngestRouterServiceClient,
    index_id: IndexId,
    body: Body,
) -> Result<(), DatadogApiError> {
    if body.content.is_empty() || body.content.as_ref() == b"{}" {
        // The datadog agent may send an empty payload as a keep alive
        // https://github.com/DataDog/datadog-agent/blob/5a6c5dd75a2233fbf954e38ddcc1484df4c21a35/pkg/logs/client/http/destination.go#L52
        debug!(
            message = "Empty payload ignored.",
            internal_log_rate_limit = true
        );
        return Ok(());
    }

    let handle = quickwit_common::thread_pool::run_cpu_intensive(move || {
        // TODO: We could just validate + get the byte bounds of each object instead of the more
        // expensive serde_json rountrip.
        // e.g. Vec<RawValue> + validation
        let messages: Vec<DatadogLogMsg> = deserialize_datadog_log(&body.content)?;

        let mut doc_batch_builder = DocBatchV2Builder::default();
        let mut doc_uid_generator = DocUidGenerator::default();

        for doc in messages {
            let processed_log = ProcessedLog::from_datadog_log_msg(doc);
            doc_batch_builder.add_doc(
                doc_uid_generator.next_doc_uid(),
                serde_json::to_string(&processed_log).unwrap().as_bytes(),
            );
        }
        Ok(doc_batch_builder.build())
    });
    let doc_batch = handle
        .await
        .map_err(|err| DatadogApiError::Panicked(err.to_string()))??;

    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id,
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch,
    };
    let request = IngestRequestV2 {
        commit_type: CommitTypeV2::Auto as i32,
        subrequests: vec![subrequest],
    };
    let response = ingest_router
        .ingest(request)
        .await
        .map_err(|err| DatadogApiError::Ingest(err.to_string()))?;
    for failure in response.failures.iter() {
        error!(
            message = "Failed to ingest logs.",
            internal_log_rate_limit = true,
            error = ?failure
        );
    }
    if !response.failures.is_empty() {
        return Err(DatadogApiError::Ingest(format!(
            "Failed to ingest logs {:?}.",
            response.failures
        )));
    }
    Ok(())
}

// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatadogLogMsg {
    pub message: String,
    pub status: Option<String>,
    #[serde(with = "time::serde::timestamp::milliseconds")]
    pub timestamp: OffsetDateTime,
    pub hostname: String,
    pub service: String,
    pub ddsource: String,
    #[serde(deserialize_with = "from_simple_list")]
    pub ddtags: Option<Vec<String>>,
}

/// TODO: Move to pipeline later on
/// The final enriched struct we want to produce.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProcessedLog {
    pub message: String,
    pub status: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub host: String,
    pub service: String,
    pub source: String,
    pub tags: Vec<String>,
    /// E.g.
    /// tags:["env:dev", "region:us-east", "region:east"] =>
    /// tag: { "env": "dev", "region": ["us-east", "east"] }
    pub tag: HashMap<String, StringOrVec>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub custom: serde_json::Map<String, serde_json::Value>,

    pub id: String,
    pub discovery_timestamp: i64,
    pub ingest_size_in_bytes: usize,
}

impl ProcessedLog {
    #[allow(dead_code)]
    pub fn get_core_string_field_by_name(&self, field: &str) -> Option<&String> {
        match field {
            "message" => Some(&self.message),
            "status" => Some(&self.status),
            "host" => Some(&self.host),
            "service" => Some(&self.service),
            "source" => Some(&self.source),
            _ => None,
        }
    }

    pub fn from_datadog_log_msg(msg: DatadogLogMsg) -> Self {
        let ingest_size_in_bytes = serde_json::to_string(&msg)
            .map(|s| s.len())
            .unwrap_or_default();
        let tags = msg.ddtags.unwrap_or_default();
        let mut processed = ProcessedLog {
            message: msg.message,
            ingest_size_in_bytes,
            status: msg.status.unwrap_or("info".to_string()).to_lowercase(),
            timestamp: msg.timestamp,
            host: msg.hostname,
            service: msg.service,
            source: msg.ddsource,
            tag: convert_tags(&tags),
            tags,
            id: Uuid::new_v4().to_string(),
            discovery_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            trace_id: None,
            span_id: None,
            custom: Default::default(),
        };

        let fields = vec![
            NormalizeField::from_comma_sep(
                "@timestamp, timestamp, _timestamp, Timestamp, eventTime, date, published_date, \
                 syslog.timestamp, time",
                "timestamp",
                false,
            ),
            NormalizeField::from_comma_sep("host, syslog.hostname, hostname", "host", false),
            NormalizeField::from_comma_sep("service, syslog.appname, dd.service", "service", false),
            NormalizeField::from_comma_sep(
                "dd.trace_id, contextMap.dd.trace_id, named_tags.dd.trace_id, trace_id, traceID, \
                 traceId",
                "trace_id",
                false,
            ),
            NormalizeField::from_comma_sep(
                "span_id, dd.span_id, contextMap.dd.span_id, named_tags.dd.span_id",
                "span_id",
                false,
            ),
            NormalizeField::from_comma_sep("message, msg, log", "message", false),
        ];

        // Apply normalization
        // TODO: Do this after the JSON parsing, so we can move all these fields to core.
        normalize_fields(
            &mut processed,
            &fields,
            |processed: &mut ProcessedLog, alias, val| {
                match alias {
                    "timestamp" => {
                        try_parse_and_update_timestamp(processed, Some(&val));
                    }
                    "host" => {
                        if let Some(s) = val.as_str() {
                            processed.host = s.to_owned();
                        }
                    }
                    "service" => {
                        if let Some(s) = val.as_str() {
                            processed.service = s.to_owned();
                        }
                    }
                    "trace_id" => {
                        if let Some(s) = val.as_str() {
                            processed.trace_id = Some(s.to_owned());
                        }
                    }
                    "span_id" => {
                        if let Some(s) = val.as_str() {
                            processed.span_id = Some(s.to_owned());
                        }
                    }
                    "message" => {
                        if let Some(s) = val.as_str() {
                            processed.message = s.to_owned();
                        }
                    }
                    _ => {
                        warn!("unhandled alias: {alias}");
                        // Any other field, just copy it over
                        processed.custom.insert(alias.to_owned(), val);
                    }
                }
            },
        );

        // Try to parse `processed.message` as JSON
        //    If it's valid JSON object, move some attributes to core.
        if let Ok(mut parsed_map) =
            serde_json::from_str::<serde_json::Map<String, Value>>(&processed.message)
        {
            // Move known fields out of the parsed JSON into `processed`
            // e.g. if the nested JSON has "message", "status", "timestamp", override them:
            if let Some(Value::String(m)) = parsed_map.remove("message") {
                processed.message = m;
            }
            // TODO: Check that status contains valid values
            if let Some(Value::String(s)) = parsed_map.remove("status") {
                processed.status = s.to_lowercase();
            }
            try_parse_and_update_timestamp(&mut processed, parsed_map.get("timestamp"));
            if let Some(Value::String(h)) = parsed_map.remove("hostname") {
                processed.host = h;
            }
            if let Some(Value::String(svc)) = parsed_map.remove("service") {
                processed.service = svc;
            }

            // Rest goes to `processed.custom`
            processed.custom = parsed_map;
        }

        processed
    }
}

/// Attempt to parse `ts_val` as one of the following:
/// - Integer (Unix epoch)
/// - String (RFC3339)
/// - String (ISO-8601)
/// - String (RFC3164) -> unsupported currently
///
/// If we succeed, we update `processed.timestamp`. Otherwise, we do nothing.
pub fn try_parse_and_update_timestamp(processed: &mut ProcessedLog, ts_val: Option<&Value>) {
    match ts_val {
        Some(Value::Number(num)) => {
            if let Some(epoch_i64) = num.as_i64() {
                if let Ok(dt) = parse_timestamp(epoch_i64) {
                    processed.timestamp = dt.into_utc();
                }
            }
        }
        Some(Value::String(s)) => {
            if let Ok(dt) = parse_date_time_str(
                s,
                &[DateTimeInputFormat::Rfc3339, DateTimeInputFormat::Iso8601],
            ) {
                processed.timestamp = dt.into_utc();
            }
        }

        _ => {}
    }
}

#[cfg(test)]
pub(crate) mod tests {

    use serde_json::Value;

    use super::*;

    /// Helper to build an `DatadogLogMsg`.
    pub fn make_datadog_log_msg() -> DatadogLogMsg {
        DatadogLogMsg {
            message: "Test log message".to_string(),
            status: Some("INFO".to_string()),
            timestamp: OffsetDateTime::now_utc(),
            hostname: "test-host".to_string(),
            service: "test-service".to_string(),
            ddsource: "rust".to_string(),
            ddtags: Some(vec!["env:dev".into(), "region:us-east".into()]),
        }
    }

    pub fn make_processed_log() -> ProcessedLog {
        ProcessedLog::from_datadog_log_msg(make_datadog_log_msg())
    }

    #[test]
    fn deserialize_datadog_log_test() {
        let data = r#"[
            {
              "hostname": "COMP-DMXWPJQKQY",
              "message": "[2025-02-11T16:41:16.703Z] Info : Modified 9 routes in 0ms",
              "service": "service123",
              "ddsource": "appgate_client",
              "status": "info",
              "ddtags": "filename:driver.log,dirname:/var/log/appgate,ingest:pomchi",
              "timestamp": 1739872513
            }
        ]"#;
        deserialize_datadog_log(data.as_bytes()).unwrap();
    }

    #[test]
    fn test_processed_log_basic() {
        let mut msg = make_datadog_log_msg();
        // Possibly override some fields for the test
        msg.message =
            r#"{ "message": "Overridden message", "status": "ERROR", "extra": 123 }"#.to_string();

        // Convert to ProcessedLog
        let processed = ProcessedLog::from_datadog_log_msg(msg.clone());

        // Verify core fields
        assert_eq!(processed.message, "Overridden message");
        assert_eq!(processed.status, "error"); // lowercased from "ERROR"
        assert_eq!(processed.host, "test-host");
        assert_eq!(processed.service, "test-service");
        assert_eq!(processed.source, "rust");

        // The ID, ingest_size_in_bytes, and discovery_timestamp won't be a fixed value,
        // so we just check they're non-empty or > 0.
        assert!(!processed.id.is_empty());
        assert!(processed.ingest_size_in_bytes > 0);
        assert!(processed.discovery_timestamp > 0);

        // "extra":123 from the nested JSON in the message body should end up in `custom`.
        // because the code doesn't specifically handle "extra".
        let custom_extra = processed.custom.get("extra").unwrap();
        assert_eq!(custom_extra, &Value::Number(123.into()));

        // Check that `tag` was created properly from `ddtags`.
        // e.g. "env:dev" =>  tag["env"] = "dev"
        //      "region:us-east" => tag["region"] = "us-east"
        let tag_env = processed.tag.get("env").unwrap();
        assert_eq!(
            tag_env,
            &StringOrVec::String("dev".to_string()),
            "Expected env:dev"
        );
        let tag_region = processed.tag.get("region").unwrap();
        assert_eq!(tag_region, &StringOrVec::String("us-east".to_string()));
    }

    /// Test that integer timestamps are interpreted as seconds or milliseconds
    #[test]
    fn test_epoch_timestamps() {
        let mut p = make_processed_log();
        // Suppose we pass Some(&Value::Number(123456789.into()))
        //  => That is <2_000_000_000, so treat as seconds
        try_parse_and_update_timestamp(&mut p, Some(&Value::Number(123456789.into())));
        // That is 1973-11-29T21:33:09Z if seconds
        assert_eq!(
            p.timestamp,
            OffsetDateTime::from_unix_timestamp(123456789).unwrap()
        );

        // Suppose we pass a bigger number => treat as milliseconds
        let mut p2 = make_processed_log();
        try_parse_and_update_timestamp(&mut p2, Some(&Value::Number(1_694_449_000_000i64.into())));
        // That is around 2023-09-10T14:50:00Z if milliseconds
        // We'll just check it's not the default or zero
        assert_ne!(p2.timestamp, p.timestamp);
    }
}
