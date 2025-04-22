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

use quickwit_datetime::{DateTimeInputFormat, parse_date_time_str, parse_timestamp};
use serde::{self, Deserialize, Serialize};
use serde_json::Value;
use serde_with::formats::CommaSeparator;
use serde_with::{StringWithSeparator, serde_as};
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;

use crate::normalize_field::{NormalizeField, normalize_fields};
use crate::path_access::ParsedPath;
use crate::transformers::StatusRemapStep;
use crate::{PipelineStep, StringOrVec, convert_tags};

// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
#[serde_as]
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
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    #[serde(default)]
    pub ddtags: Vec<String>,
}

/// The final enriched struct we want to produce.
///  TODO fix the confusing name (ProcessedDoc)
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
    pub tiebreaker: i64,
    pub ingest_size_in_bytes: usize,
}

impl ProcessedLog {
    pub fn get_core_string_field_by_name(&self, field: &str) -> Option<&str> {
        match field {
            "message" => Some(&self.message),
            "status" => Some(&self.status),
            "host" => Some(&self.host),
            "service" => Some(&self.service),
            "source" => Some(&self.source),
            "trace_id" => self.trace_id.as_deref(),
            _ => None,
        }
    }

    pub fn from_datadog_log_msg(msg: DatadogLogMsg) -> Self {
        let ingest_size_in_bytes = serde_json::to_string(&msg)
            .map(|s| s.len())
            .unwrap_or_default();
        let tags = msg.ddtags;
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
            tiebreaker: rand::random(),
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
                        try_parse_and_update_timestamp(processed, &val);
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
            if let Some(val) = parsed_map.get("timestamp") {
                try_parse_and_update_timestamp(&mut processed, val);
            }
            if let Some(Value::String(h)) = parsed_map.remove("hostname") {
                processed.host = h;
            }
            if let Some(Value::String(svc)) = parsed_map.remove("service") {
                processed.service = svc;
            }

            // Rest goes to `processed.custom`
            processed.custom = parsed_map;
        }

        // TODO:: We don't need to recreate this StatusRemapStep for every log.
        let sources: Vec<ParsedPath> = ["status", "severity", "level", "syslog.severity"]
            .iter()
            .map(|field| ParsedPath::from(*field))
            .collect();
        StatusRemapStep { sources }.apply(&mut processed).unwrap();

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
pub fn try_parse_and_update_timestamp(processed: &mut ProcessedLog, ts_val: &Value) {
    match ts_val {
        Value::Number(num) => {
            if let Some(epoch_i64) = num.as_i64() {
                if let Ok(dt) = parse_timestamp(epoch_i64) {
                    processed.timestamp = dt.into_utc();
                }
            }
        }
        Value::String(s) => {
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
    use time::OffsetDateTime;

    use crate::processed_log::{DatadogLogMsg, try_parse_and_update_timestamp};
    use crate::{ProcessedLog, StringOrVec};

    /// Helper to build an `DatadogLogMsg`.
    pub fn make_datadog_log_msg() -> DatadogLogMsg {
        DatadogLogMsg {
            message: "Test log message".to_string(),
            status: Some("INFO".to_string()),
            timestamp: OffsetDateTime::now_utc(),
            hostname: "test-host".to_string(),
            service: "test-service".to_string(),
            ddsource: "rust".to_string(),
            ddtags: vec!["env:dev".into(), "region:us-east".into()],
        }
    }

    pub fn make_processed_log() -> ProcessedLog {
        ProcessedLog::from_datadog_log_msg(make_datadog_log_msg())
    }

    #[test]
    fn test_deserialize_datadog_log_msg() {
        let json = r#"{
                "message": "Overridden message",
                "status": "info",
                "ddtags": "env:dev,region:us-east",
                "timestamp": 1620000000000,
                "hostname": "test-host",
                "service": "test-service",
                "ddsource": "rust"
            }"#
        .to_string();
        let msg: DatadogLogMsg = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.message, "Overridden message");
        assert_eq!(msg.status.unwrap(), "info");
        assert_eq!(msg.hostname, "test-host");
        assert_eq!(msg.service, "test-service");
        assert_eq!(msg.ddsource, "rust");
        assert_eq!(
            msg.ddtags,
            vec!["env:dev".to_string(), "region:us-east".to_string()]
        );
    }

    #[test]
    fn test_deserialize_datadog_log_msg_with_no_tags() {
        // Test with no tags
        // unclear if tags is optional or not
        let json = r#"{
                "message": "Overridden message",
                "status": "info",
                "timestamp": 1620000000000,
                "hostname": "test-host",
                "service": "test-service",
                "ddsource": "rust"
            }"#
        .to_string();
        let msg: DatadogLogMsg = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.message, "Overridden message");
        assert_eq!(msg.status.unwrap(), "info");
        assert_eq!(msg.hostname, "test-host");
        assert_eq!(msg.service, "test-service");
        assert_eq!(msg.ddsource, "rust");
        assert_eq!(msg.ddtags, Vec::<String>::new());
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
        try_parse_and_update_timestamp(&mut p, &Value::Number(123456789.into()));
        // That is 1973-11-29T21:33:09Z if seconds
        assert_eq!(
            p.timestamp,
            OffsetDateTime::from_unix_timestamp(123456789).unwrap()
        );

        // Suppose we pass a bigger number => treat as milliseconds
        let mut p2 = make_processed_log();
        try_parse_and_update_timestamp(&mut p2, &Value::Number(1_694_449_000_000i64.into()));
        // That is around 2023-09-10T14:50:00Z if milliseconds
        // We'll just check it's not the default or zero
        assert_ne!(p2.timestamp, p.timestamp);
    }

    #[test]
    fn test_override_status_from_custom() {
        let json = r#"{ 
                "message": "{ \"message\": \"Overridden message\", \"severity\": \"INFO\" }",
                "status": "error", 
                "timestamp": 1620000000000,
                "hostname": "test-host",
                "service": "test-service",
                "ddsource": "rust"
            }"#
        .to_string();
        let msg: DatadogLogMsg = serde_json::from_str(&json).unwrap();
        let msg: ProcessedLog = ProcessedLog::from_datadog_log_msg(msg);
        assert_eq!(msg.message, "Overridden message");
        assert_eq!(msg.status, "info");
    }
}
