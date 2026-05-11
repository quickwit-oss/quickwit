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

use serde::ser::SerializeStruct;
use serde::{self, Deserialize, Serialize};
use serde_json::Value;
use serde_with::formats::CommaSeparator;
use serde_with::{StringWithSeparator, serde_as};
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;

use crate::date_time_parsing::{DateTimeInputFormat, parse_date_time_str, parse_timestamp};
use crate::path_access::ParsedPath;
use crate::string_or_vec::StringOrVec;
use crate::transformers::{
    CoreStringAttr, CoreStringAttrRemapStep, DateRemapStep, StatusRemapStep, SyslogProcessor,
};
use crate::{Pipeline, PipelineStep, convert_tags};

// https://github.com/DataDog/datadog-agent/blob/a33248c2bc125920a9577af1e16f12298875a4ad/pkg/logs/processor/json.go#L23-L49
#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DatadogLogMsg {
    pub message: MessageValue,
    pub status: Option<String>,
    #[serde(
        default,
        with = "time::serde::timestamp::milliseconds::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub timestamp: Option<OffsetDateTime>,
    #[serde(alias = "host")]
    pub hostname: Option<String>,
    pub service: Option<String>,
    #[serde(alias = "source")]
    pub ddsource: Option<String>,
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    #[serde(default)]
    #[serde(alias = "tags")]
    pub ddtags: Vec<String>,
}

impl Default for DatadogLogMsg {
    fn default() -> Self {
        DatadogLogMsg {
            message: MessageValue::Str("".to_string()),
            status: None,
            timestamp: None,
            hostname: None,
            service: None,
            ddsource: None,
            ddtags: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
/// Message can be either a string or an object.
pub enum MessageValue {
    Str(String),
    Obj(serde_json::Map<String, Value>),
}

impl From<String> for MessageValue {
    fn from(s: String) -> Self {
        MessageValue::Str(s)
    }
}
impl From<&str> for MessageValue {
    fn from(s: &str) -> Self {
        MessageValue::Str(s.to_string())
    }
}

/// The final enriched struct we want to produce.
#[derive(Clone, Debug, Serialize)]
pub struct ProcessedLog {
    pub message: String,
    pub status: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub host: Option<String>,
    pub service: Option<String>,
    pub source: Option<String>,

    /// E.g.
    /// tags:["env:dev", "region:us-east", "region:east"] =>
    /// tag: { "env": "dev", "region": ["us-east", "east"] }
    #[serde(flatten)]
    pub tag: TagField,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub custom: serde_json::Map<String, serde_json::Value>,

    /// Top-level fields duplicated from `custom` for tokenized FTS.
    /// `error` (with `message` and `stack` sub-fields) and `title`.
    /// Pomsky defines an `extra_fts` concatenate field over these for combined FTS.
    #[serde(flatten, skip_serializing_if = "ExtraFts::is_empty")]
    pub extra_fts: ExtraFts,

    pub id: String,
    pub discovery_timestamp: i64,
    pub tiebreaker: i32,
    pub ingest_size_in_bytes: usize,
}

/// Top-level fields duplicated from `custom` for tokenized FTS.
/// Uses `#[serde(flatten)]` so `error` serializes as a nested object and
/// `title` as a top-level string — matching the pomsky schema fields.
/// Pomsky's `extra_fts` concatenate field merges `error.message`,
/// `error.stack`, and `title` for combined FTS queries.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ExtraFts {
    #[serde(skip_serializing_if = "ErrorObject::is_empty")]
    pub error: ErrorObject,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ErrorObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

impl ErrorObject {
    pub fn is_empty(&self) -> bool {
        self.message.is_none() && self.stack.is_none()
    }
}

impl ExtraFts {
    /// Build from the `custom` map by extracting the relevant sub-fields.
    // Note: We clone strings from `custom` rather than taking ownership because
    // the values must remain in `custom` for stored field retrieval and exact-match
    // queries via the raw tokenizer. Revisit if profiling shows this is a bottleneck.
    pub fn from_custom(custom: &serde_json::Map<String, Value>) -> Self {
        let mut fts = ExtraFts::default();

        if let Some(Value::String(title)) = custom.get("title") {
            fts.title = Some(title.clone());
        }

        if let Some(Value::Object(error_obj)) = custom.get("error") {
            if let Some(Value::String(msg)) = error_obj.get("message") {
                fts.error.message = Some(msg.clone());
            }
            if let Some(Value::String(stack)) = error_obj.get("stack") {
                fts.error.stack = Some(stack.clone());
            }
        }

        fts
    }

    pub fn is_empty(&self) -> bool {
        self.error.is_empty() && self.title.is_none()
    }
}

/// Special struct that serializes two different ways:
/// tags:["env:dev", "region:us-east", "region:east"] =>
/// tag: { "env": "dev", "region": ["us-east", "east"] }
#[derive(Clone, Debug, Default)]
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
pub fn get_preprocessing_pipeline() -> &'static Pipeline {
    PREPROCESSING_PIPELINE.get_or_init(create_preprocessing_pipeline)
}

/// Flattens nested message JSON objects.
/// If the object contains nested `message` objects, we:
/// - accumulate all sibling keys into a flat map
/// - follow the `message` key down until it is no longer an object
fn flatten_nested_message_object(
    map: serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    let mut custom = serde_json::Map::new();
    let mut current_map = Some(map);

    while let Some(mut map) = current_map.take() {
        let next_message = map.remove("message");
        custom.extend(map);

        if let Some(msg_val) = next_message {
            match msg_val {
                Value::Object(nested) => {
                    // Set current_map to nested and continue
                    current_map = Some(nested);
                }
                Value::String(s) => {
                    // If the message is a JSON-serialized object, try parse and continue
                    // flattening.
                    if let Ok(nested) = serde_json::from_str::<serde_json::Map<String, Value>>(&s) {
                        current_map = Some(nested);
                    } else {
                        custom.insert("message".to_string(), Value::String(s));
                    }
                }
                other => {
                    custom.insert("message".to_string(), other);
                }
            }
        }
        if current_map.is_none() {
            break;
        }
    }

    custom
}

// The preprocessing pipeline is used to remap fields from custom to core attributes.
fn create_preprocessing_pipeline() -> Pipeline {
    let string_remap = |path: &[&str], core_attr| {
        let sources: Vec<ParsedPath> = path.iter().map(|field| ParsedPath::from(*field)).collect();
        Box::new(CoreStringAttrRemapStep { sources, core_attr })
    };

    let steps: Vec<Box<dyn PipelineStep>> = vec![
        // Add SyslogProcessor as the first step to parse syslog messages,
        // as done in Datadog intake, with the difference that in pomsky we only try to parse
        // logs from a syslog source.
        // https://datadoghq.atlassian.net/wiki/spaces/AL/pages/2727873101/Interesting+Logs+Intake+Behaviors#Syslog
        Box::new(SyslogProcessor),
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
        string_remap(
            &["host", "syslog.hostname", "hostname"],
            CoreStringAttr::Host,
        ),
        string_remap(&["ddsource"], CoreStringAttr::Source),
        string_remap(
            &["dd.service", "service", "syslog.appname"],
            CoreStringAttr::Service,
        ),
        Box::new(StatusRemapStep {
            sources: ["status", "severity", "level", "syslog.severity"]
                .iter()
                .map(|field| ParsedPath::from(*field))
                .collect(),
        }),
        string_remap(
            &[
                "dd.trace_id",
                "contextMap.dd.trace_id",
                "named_tags.dd.trace_id",
                "trace_id",
                "traceID",
                "traceId",
                "syslog.trace_id",
            ],
            CoreStringAttr::TraceId,
        ),
        string_remap(
            &[
                "span_id",
                "dd.span_id",
                "contextMap.dd.span_id",
                "named_tags.dd.span_id",
                "syslog.span_id",
            ],
            CoreStringAttr::SpanId,
        ),
        string_remap(
            &["message", "dd.message", "syslog.message", "msg", "log"],
            CoreStringAttr::Message,
        ),
    ];

    Pipeline::from_steps(steps)
}

impl ProcessedLog {
    pub fn get_core_string_field_by_name(&self, field: &str) -> Option<&str> {
        match field {
            "message" => Some(&self.message),
            "status" => Some(&self.status),
            "host" => self.host.as_deref(),
            "service" => self.service.as_deref(),
            "source" => self.source.as_deref(),
            "trace_id" => self.trace_id.as_deref(),
            _ => None,
        }
    }

    pub fn from_datadog_log_msg(msg: DatadogLogMsg) -> Self {
        let ingest_size_in_bytes = serde_json::to_string(&msg)
            .map(|s| s.len())
            .unwrap_or_default();
        let mut custom = serde_json::Map::new();

        // Helper to normalize the message value into a MessageValue::Obj if possible.
        let get_normalized_message_value = |msg: MessageValue| -> MessageValue {
            match msg {
                MessageValue::Obj(map) => MessageValue::Obj(map.clone()),
                MessageValue::Str(s) => {
                    if let Ok(parsed_map) =
                        serde_json::from_str::<serde_json::Map<String, Value>>(&s)
                    {
                        MessageValue::Obj(parsed_map)
                    } else {
                        MessageValue::Str(s.clone())
                    }
                }
            }
        };

        let message = match get_normalized_message_value(msg.message) {
            MessageValue::Str(m) => m,
            MessageValue::Obj(map) => {
                let nested_custom = flatten_nested_message_object(map);
                custom.extend(nested_custom);
                "".to_string()
            }
        };

        let mut tags = msg.ddtags;
        // Overwrite tags from custom if any
        if let Some(Value::Array(tag_values)) = custom.remove("ddtags") {
            let new_tags: Vec<String> = tag_values
                .into_iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .collect();
            if !new_tags.is_empty() {
                tags = new_tags;
            }
        }

        let mut processed = ProcessedLog {
            message,
            ingest_size_in_bytes,
            status: msg.status.unwrap_or("info".to_string()).to_lowercase(),
            timestamp: msg.timestamp.unwrap_or_else(OffsetDateTime::now_utc),
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
            custom,
            extra_fts: ExtraFts::default(),
        };

        match get_preprocessing_pipeline().apply(&mut processed) {
            Ok(_) => {}
            Err(error) => {
                // This should not happen, but if it does, we log the error.
                warn!(%error, "failed to apply preprocessing pipeline");
            }
        }

        processed.extra_fts = ExtraFts::from_custom(&processed.custom);

        // We do this after preprocessing so that we can add tags for the final service and source.
        if let Some(source) = &processed.source {
            processed
                .tag
                .insert("source".to_string(), source.as_str().into());
        }
        if let Some(service) = &processed.service {
            processed
                .tag
                .insert("service".to_string(), service.as_str().into());
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
            if let Some(epoch_i64) = num.as_i64()
                && let Ok(dt) = parse_timestamp(epoch_i64)
            {
                processed.timestamp = dt;
            }
        }
        Value::String(s) => {
            if let Ok(dt) = parse_date_time_str(
                s,
                &[DateTimeInputFormat::Rfc3339, DateTimeInputFormat::Iso8601],
            ) {
                processed.timestamp = dt;
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
            message: "Test log message".to_string().into(),
            status: Some("INFO".to_string()),
            timestamp: Some(OffsetDateTime::now_utc()),
            hostname: Some("test-host".to_string()),
            service: Some("test-service".to_string()),
            ddsource: Some("rust".to_string()),
            ddtags: vec!["env:dev".into(), "region:us-east".into()],
        }
    }

    pub fn make_processed_log() -> ProcessedLog {
        ProcessedLog::from_datadog_log_msg(make_datadog_log_msg())
    }

    #[test]
    fn test_deserialize_datadog_log_msg_minimal() {
        let json = r#"{
                "message": "Overridden message"
            }"#
        .to_string();
        let _msg: DatadogLogMsg = serde_json::from_str(&json).unwrap();
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
        assert_eq!(msg.message, "Overridden message".into());
        assert_eq!(msg.status.unwrap(), "info");
        assert_eq!(msg.hostname.unwrap(), "test-host");
        assert_eq!(msg.service.unwrap(), "test-service");
        assert_eq!(msg.ddsource.unwrap(), "rust");
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
        assert_eq!(msg.message, "Overridden message".into());
        assert_eq!(msg.status.unwrap(), "info");
        assert_eq!(msg.hostname.unwrap(), "test-host");
        assert_eq!(msg.service.unwrap(), "test-service");
        assert_eq!(msg.ddsource.unwrap(), "rust");
        assert_eq!(msg.ddtags, Vec::<String>::new());
    }

    #[test]
    fn test_processed_log_basic() {
        let mut msg = make_datadog_log_msg();
        // Possibly override some fields for the test
        msg.message = r#"{ "message": "Overridden message", "status": "ERROR", "extra": 123 }"#
            .to_string()
            .into();

        // Convert to ProcessedLog
        let processed = ProcessedLog::from_datadog_log_msg(msg.clone());

        // Verify core fields
        assert_eq!(processed.message, "Overridden message");
        assert_eq!(processed.status, "error"); // lowercased from "ERROR"
        assert_eq!(processed.host.unwrap(), "test-host");
        assert_eq!(processed.service.unwrap(), "test-service");
        assert_eq!(processed.source.unwrap(), "rust");

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
    fn test_override_status_from_custom_nested() {
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
        assert_eq!(msg.service.unwrap(), "overwrite-service");
        assert_eq!(msg.host.unwrap(), "overwrite-host");
        assert_eq!(
            msg.timestamp,
            OffsetDateTime::from_unix_timestamp(1609459200).unwrap()
        );
    }

    #[test]
    fn test_override_status_from_custom_flat() {
        let json_msg = json!({
            "message": "Overridden message",
            "status": "error",
            "hostname": "overwrite-host",
            "Timestamp": "2021-01-01T00:00:00Z",
            "dd.span_id": "99999",
            "dd.trace_id": "12345",
            "dd.service": "overwrite-service",
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
        assert_eq!(msg.service.unwrap(), "overwrite-service");
        assert_eq!(msg.host.unwrap(), "overwrite-host");
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

    #[test]
    fn test_process_syslog_message() {
        let mut msg = make_datadog_log_msg();
        msg.message = r#"<187>1 2025-10-14T07:10:44+00:00 aaabf280-c060-4637-b4e6-68f3d3c44872 vcap_nginx_access - - -  localhost - [14/Oct/2025:07:10:44 +0000] "GET /healthz HTTP/1.1" 200 214 "-" "curl/7.81.0" 127.0.0.1 vcap_request_id:064249e3-9311-4bf8-9dd7-a0068fa73016 response_time:0.002"#.to_string().into();
        msg.ddsource = Some("syslog".to_string()); // Set source to syslog so processor runs

        let processed = ProcessedLog::from_datadog_log_msg(msg);

        assert_eq!(
            processed.message,
            " localhost - [14/Oct/2025:07:10:44 +0000] \"GET /healthz HTTP/1.1\" 200 214 \"-\" \
             \"curl/7.81.0\" 127.0.0.1 vcap_request_id:064249e3-9311-4bf8-9dd7-a0068fa73016 \
             response_time:0.002"
        );
        assert_eq!(processed.status, "error");
        assert_eq!(
            processed.host,
            Some("aaabf280-c060-4637-b4e6-68f3d3c44872".to_string())
        );
        assert_eq!(
            processed.timestamp,
            OffsetDateTime::from_unix_timestamp(1760425844).unwrap()
        );
        assert_eq!(processed.custom["syslog"]["appname"], "vcap_nginx_access");
    }

    #[test]
    fn test_json_message_in_datadog_log_msg() {
        let mut msg = make_datadog_log_msg();
        msg.message = r#"{ "event": "user_login", "user": "alice", "status": "ERROR", "message": "User login failed" }"#.to_string().into();
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        assert_eq!(processed.message, "User login failed");
        assert_eq!(processed.status, "error");
        assert_eq!(processed.custom["event"], "user_login");
        assert_eq!(processed.custom["user"], "alice");
    }

    #[test]
    fn test_json_message_nested_json_msg() {
        let json = json!({"message": {
            "some_attr": "foobar",
            "message": {
                "some_nest": "blah",
                "message": {
                    "some_nest_2": "blah",
                    "message": "hello world"
                }
            }
        }});
        let msg: DatadogLogMsg =
            serde_json::from_str(&serde_json::to_string(&json).unwrap()).unwrap();
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        // The innermost "message" should be extracted
        // The other fields should be in custom (flattened)
        assert_eq!(processed.message, "hello world");
        assert_eq!(processed.custom["some_attr"], "foobar");
        assert_eq!(processed.custom["some_nest"], "blah");
        assert_eq!(processed.custom["some_nest_2"], "blah");
    }

    #[test]
    fn test_json_message_nested_json_msg_serialized() {
        let mut msg = make_datadog_log_msg();
        let json = json!({"message": {
            "some_attr": "foobar",
            "message": {
                "some_nest": "blah",
                "message": {
                    "some_nest_2": "blah",
                    "message": "hello world"
                }
            }
        }});
        msg.message = serde_json::to_string(&json).unwrap().into();
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        // The innermost "message" should be extracted
        // The other fields should be in custom (flattened)
        assert_eq!(processed.message, "hello world");
        assert_eq!(processed.custom["some_attr"], "foobar");
        assert_eq!(processed.custom["some_nest"], "blah");
        assert_eq!(processed.custom["some_nest_2"], "blah");
    }

    #[test]
    fn test_json_message_nested_serialized_inner_message() {
        let mut msg = make_datadog_log_msg();
        // Here the outer message is JSON, and an inner `message` field
        // is itself a serialized JSON object that contains another `message`.
        let json = json!({"message": {
            "some_attr": "foobar",
            "message": serde_json::to_string(&json!({
                "some_nest": "blah",
                "message": serde_json::to_string(&json!({
                    "some_nest_2": "blah",
                    "message": "hello world"
                })).unwrap()
            })).unwrap()
        }});
        msg.message = serde_json::to_string(&json).unwrap().into();

        let processed = ProcessedLog::from_datadog_log_msg(msg);
        // The innermost "message" should be extracted
        // The other fields should be in custom (flattened), including those
        // coming from the serialized inner JSON message.
        assert_eq!(processed.message, "hello world");
        assert_eq!(processed.custom["some_attr"], "foobar");
        assert_eq!(processed.custom["some_nest"], "blah");
        assert_eq!(processed.custom["some_nest_2"], "blah");
    }

    #[test]
    fn test_json_message_nested_with_override() {
        // Override source and add tags at different levels
        let mut msg = make_datadog_log_msg();
        msg.ddsource = Some("blub".to_string());
        msg.ddtags = vec!["outer:notnested".to_string()];
        msg.ddsource = None;
        let json = json!({"message": {
            "some_attr": "foobar",
            "some_nest": "blah",
            "ddsource":"ruby",
            "ddtags":["inner:nested"]
        }});
        msg.message = serde_json::to_string(&json).unwrap().into();
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        // The innermost "message" should be empty
        // The other fields should be in custom.
        assert_eq!(processed.message, "");
        assert_eq!(processed.custom["some_attr"], "foobar");
        assert_eq!(processed.custom["some_nest"], "blah");
        assert_eq!(processed.source.unwrap(), "ruby");

        // Tags are overwritten from inner message and not merged
        assert_eq!(processed.tag.get("outer"), None);
        assert_eq!(processed.tag.get("inner").unwrap(), &"nested".into());
    }

    #[test]
    fn test_json_message_nested_with_override_multiple_levels() {
        // Override source and add tags. We only consider the innermost ddsource and ddtags.
        let mut msg = make_datadog_log_msg();
        msg.ddsource = Some("blub".to_string());
        msg.ddtags = vec!["outer:notnested".to_string()];
        msg.ddsource = None;
        let json = json!({"message": {
            "some_attr": "foobar",
            "some_nest": "blah",
            "ddsource":"ruby",
            "ddtags":["inner:nested"],
            "message": {
                "ddsource":"megainner",
                "ddtags":["megainner:nested"]
            }
        }});
        msg.message = serde_json::to_string(&json).unwrap().into();
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        // The innermost "message" should be empty
        // The other fields should be in custom.
        assert_eq!(processed.message, "");
        assert_eq!(processed.custom["some_attr"], "foobar");
        assert_eq!(processed.custom["some_nest"], "blah");
        assert_eq!(processed.source.unwrap(), "megainner");

        // Tags are overwritten by all levels below
        assert_eq!(processed.tag.get("outer"), None);
        assert_eq!(processed.tag.get("inner"), None);
        assert_eq!(processed.tag.get("megainner").unwrap(), &"nested".into());
    }

    #[test]
    fn test_extra_fts_populated_from_error_and_title() {
        let json_msg = json!({
            "message": "something went wrong",
            "error": {
                "message": "java.lang.NullPointerException",
                "stack": "at com.example.Main.run(Main.java:42)"
            },
            "title": "Critical failure in payment service"
        });
        let msg = DatadogLogMsg {
            message: serde_json::to_string(&json_msg).unwrap().into(),
            ..DatadogLogMsg::default()
        };
        let processed = ProcessedLog::from_datadog_log_msg(msg);

        assert_eq!(
            processed.extra_fts.error.message.as_deref(),
            Some("java.lang.NullPointerException")
        );
        assert!(
            processed
                .extra_fts
                .error
                .stack
                .as_deref()
                .unwrap()
                .contains("Main.java:42")
        );
        assert_eq!(
            processed.extra_fts.title.as_deref(),
            Some("Critical failure in payment service")
        );
    }

    #[test]
    fn test_extra_fts_empty_when_no_matching_fields() {
        let msg = DatadogLogMsg {
            message: "plain text log with no JSON".into(),
            ..DatadogLogMsg::default()
        };
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        assert!(processed.extra_fts.is_empty());
    }

    #[test]
    fn test_extra_fts_partial_fields() {
        let json_msg = json!({
            "error": { "message": "connection timeout" }
        });
        let msg = DatadogLogMsg {
            message: serde_json::to_string(&json_msg).unwrap().into(),
            ..DatadogLogMsg::default()
        };
        let processed = ProcessedLog::from_datadog_log_msg(msg);

        assert_eq!(
            processed.extra_fts.error.message.as_deref(),
            Some("connection timeout")
        );
        assert!(processed.extra_fts.error.stack.is_none());
        assert!(processed.extra_fts.title.is_none());
    }

    #[test]
    fn test_extra_fts_not_serialized_when_empty() {
        let msg = DatadogLogMsg {
            message: "plain log".into(),
            ..DatadogLogMsg::default()
        };
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        let json_str = serde_json::to_string(&processed).unwrap();
        assert!(
            !json_str.contains("\"error\""),
            "error should not appear when empty"
        );
        assert!(
            !json_str.contains("\"title\""),
            "title should not appear when empty"
        );
    }

    #[test]
    fn test_extra_fts_serialized_when_populated() {
        let json_msg = json!({
            "error": { "message": "something broke" },
            "title": "Alert: disk full"
        });
        let msg = DatadogLogMsg {
            message: serde_json::to_string(&json_msg).unwrap().into(),
            ..DatadogLogMsg::default()
        };
        let processed = ProcessedLog::from_datadog_log_msg(msg);
        let json_str = serde_json::to_string(&processed).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["error"]["message"], "something broke");
        assert_eq!(parsed["title"], "Alert: disk full");
    }
}
