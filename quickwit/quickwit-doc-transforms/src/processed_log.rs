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
use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use quickwit_datetime::{DateTimeInputFormat, parse_date_time_str, parse_timestamp};
use serde::ser::SerializeStruct;
use serde::{self, Deserialize, Serialize};
use serde_json::Value;
use serde_with::formats::CommaSeparator;
use serde_with::{StringWithSeparator, serde_as};
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;

use crate::path_access::ParsedPath;
use crate::string_or_vec::StringOrVec;
use crate::transformers::{
    CoreStringAttr, CoreStringAttrRemapStep, DateRemapStep, StatusRemapStep,
};
use crate::{Pipeline, PipelineStep, convert_tags};

// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatadogLogMsg {
    pub message: String,
    pub status: Option<String>,
    #[serde(with = "time::serde::timestamp::milliseconds")]
    pub timestamp: OffsetDateTime,
    #[serde(alias = "host")]
    pub hostname: String,
    pub service: String,
    #[serde(alias = "source")]
    pub ddsource: String,
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    #[serde(default)]
    #[serde(alias = "tags")]
    pub ddtags: Vec<String>,
}

/// The final enriched struct we want to produce.
///  TODO fix the confusing name (ProcessedDoc)
#[derive(Clone, Debug, Serialize)]
pub struct ProcessedLog {
    pub message: String,
    pub status: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub host: String,
    pub service: String,
    pub source: String,

    /// E.g.
    /// tags:["env:dev", "region:us-east", "region:east"] =>
    /// tag: { "env": "dev", "region": ["us-east", "east"] }
    #[serde(flatten)]
    pub tag: TagField,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub custom: serde_json::Map<String, serde_json::Value>,

    pub id: String,
    pub discovery_timestamp: i64,
    pub tiebreaker: i64,
    pub ingest_size_in_bytes: usize,
}

/// Special struct that serializes two different ways:
/// tags:["env:dev", "region:us-east", "region:east"] =>
/// tag: { "env": "dev", "region": ["us-east", "east"] }
#[derive(Clone, Debug)]
pub struct TagField {
    pub tag: HashMap<String, StringOrVec>,
}
impl From<HashMap<String, StringOrVec>> for TagField {
    fn from(tag: HashMap<String, StringOrVec>) -> Self {
        TagField { tag }
    }
}
impl TagField {
    fn tags_vec(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (k, v) in &self.tag {
            match v {
                StringOrVec::String(s) => out.push(format!("{k}:{s}")),
                StringOrVec::Vec(list) => {
                    out.extend(list.iter().map(|s| format!("{k}:{s}")));
                }
            }
        }
        out.sort();
        out
    }
}

impl Serialize for TagField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut st = serializer.serialize_struct("TagField", 2)?;
        st.serialize_field("tag", &self.tag)?;
        st.serialize_field("tags", &self.tags_vec())?;
        st.end()
    }
}

impl DerefMut for TagField {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tag
    }
}
impl Deref for TagField {
    type Target = HashMap<String, StringOrVec>;

    fn deref(&self) -> &Self::Target {
        &self.tag
    }
}

static PREPROCESSING_PIPELINE: OnceLock<Pipeline> = OnceLock::new();
fn get_preprocessing_pipeline() -> &'static Pipeline {
    PREPROCESSING_PIPELINE.get_or_init(create_preprocessing_pipeline)
}

// The preprocessing pipeline is used to remap fields from custom to core attributes.
fn create_preprocessing_pipeline() -> Pipeline {
    let string_remap = |path: &[&str], core_attr| {
        let sources: Vec<ParsedPath> = path.iter().map(|field| ParsedPath::from(*field)).collect();
        Box::new(CoreStringAttrRemapStep { sources, core_attr })
    };

    let steps: Vec<Box<dyn PipelineStep>> = vec![
        Box::new(DateRemapStep {
            sources: [
                "@timestamp",
                "timestamp",
                "_timestamp",
                "Timestamp",
                "eventTime",
                "date",
                "published_date",
                "syslog.timestamp",
                "time",
            ]
            .iter()
            .map(|field| ParsedPath::from(*field))
            .collect(),
        }),
        Box::new(StatusRemapStep {
            sources: ["status", "severity", "level", "syslog.severity"]
                .iter()
                .map(|field| ParsedPath::from(*field))
                .collect(),
        }),
        string_remap(
            &["dd.service", "service", "syslog.appname"],
            CoreStringAttr::Service,
        ),
        string_remap(
            &[
                "span_id",
                "dd.span_id",
                "contextmap.dd.span_id",
                "named_tags.dd.span_id",
                "syslog.span_id",
            ],
            CoreStringAttr::SpanId,
        ),
        string_remap(
            &["dd.trace_id", "trace_id", "syslog.trace_id"],
            CoreStringAttr::TraceId,
        ),
        string_remap(
            &["message", "dd.message", "syslog.message"],
            CoreStringAttr::Message,
        ),
        string_remap(
            &["host", "hostname", "syslog.hostname"],
            CoreStringAttr::Host,
        ),
    ];

    Pipeline::from_steps(steps)
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
        let mut tags = msg.ddtags;
        tags.push(format!("source:{}", msg.ddsource));
        tags.push(format!("service:{}", msg.service));
        let mut processed = ProcessedLog {
            message: msg.message,
            ingest_size_in_bytes,
            status: msg.status.unwrap_or("info".to_string()).to_lowercase(),
            timestamp: msg.timestamp,
            host: msg.hostname,
            service: msg.service,
            source: msg.ddsource,
            tag: convert_tags(&tags).into(),
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

        // Try to parse `processed.message` as JSON
        //    If it's a JSON object, move some attributes to core via the preprocessing pipeline.
        if let Ok(parsed_map) =
            serde_json::from_str::<serde_json::Map<String, Value>>(&processed.message)
        {
            processed.custom = parsed_map;
            match get_preprocessing_pipeline().apply(&mut processed) {
                Ok(_) => {}
                Err(err) => {
                    // This should not happen, but if it does, we log the error.
                    warn!("Failed to apply preprocessing pipeline: {}", err);
                }
            }
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

    use serde_json::{Value, json};
    use time::OffsetDateTime;

    use crate::ProcessedLog;
    use crate::processed_log::{DatadogLogMsg, try_parse_and_update_timestamp};
    use crate::string_or_vec::StringOrVec;

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

        let source_tag = processed.tag.get("source").unwrap();
        assert_eq!(source_tag, &StringOrVec::String("rust".to_string()),);
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
        let json_msg = json!({
            "message": "Overridden message",
            "status": "error",
            "hostname": "overwrite-host",
            "Timestamp": "2021-01-01T00:00:00Z",
            "dd": {
                "span_id": "99999",
                "trace_id": "12345",
                "service": "overwrite-service",
            }
        });
        let json = json!({
            "message": serde_json::to_string(&json_msg).unwrap(),
            "status": "info",
            "timestamp": 1620000000000i64,
            "hostname": "test-host",
            "service": "test-service",
            "ddsource": "rust"
        });

        let msg: DatadogLogMsg =
            serde_json::from_str(&serde_json::to_string(&json).unwrap()).unwrap();
        let msg: ProcessedLog = ProcessedLog::from_datadog_log_msg(msg);
        assert_eq!(msg.message, "Overridden message");
        assert_eq!(msg.status, "error");
        assert_eq!(msg.trace_id, Some("12345".to_string()));
        assert_eq!(msg.span_id, Some("99999".to_string()));
        assert_eq!(msg.service, "overwrite-service");
        assert_eq!(msg.host, "overwrite-host");
        assert_eq!(
            msg.timestamp,
            OffsetDateTime::from_unix_timestamp(1609459200).unwrap()
        );
    }
    #[test]
    fn test_processed_log_tag_serialization() {
        use serde_json::Value;

        let mut msg = make_datadog_log_msg();
        msg.ddtags = vec![
            "env:dev".to_string(),
            "region:us-east".to_string(),
            "region:east".to_string(),
        ];

        let processed = ProcessedLog::from_datadog_log_msg(msg);

        // Serialize to JSON
        let json = serde_json::to_value(&processed).expect("serialize ProcessedLog");
        let obj = json
            .as_object()
            .expect("ProcessedLog JSON should be an object");

        let raw = obj.get("tag").and_then(Value::as_object).unwrap();
        assert_eq!(raw.get("env").unwrap(), "dev");
        assert_eq!(
            raw.get("region").unwrap(),
            &Value::Array(vec![
                Value::String("us-east".into()),
                Value::String("east".into())
            ])
        );

        let tags = obj.get("tags").and_then(Value::as_array).unwrap();
        let tags_str: Vec<_> = tags.iter().map(|v| v.as_str().unwrap()).collect();
        assert_eq!(
            tags_str,
            vec![
                "env:dev",
                "region:east",
                "region:us-east",
                "service:test-service",
                "source:rust",
            ]
        );
    }
}
