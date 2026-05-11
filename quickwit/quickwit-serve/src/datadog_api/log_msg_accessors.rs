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

use quickwit_processing::{DatadogLogMsg, MessageValue};

/// Returns a closure that extracts custom field values from a DatadogLogMsg using a path.
///
/// Custom fields are nested values found in the message JSON object.
/// Example: path `["metadata", "ip"]` looks up `message.metadata.ip`
///
///  Notice that to match the preprocessing, we need to look inside nested messsage as well
/// `{"message": {"message": {"metadata": {"ip": "1.1.1.1"}}}}`
pub fn custom_field_accessor<'a>(
    msg: &'a DatadogLogMsg,
) -> impl Fn(&[String]) -> Option<&'a str> + 'a {
    move |path| {
        walk_message_chain(&msg.message, |map| {
            let [components @ .., tail] = path else {
                return None;
            };
            let mut current = map;
            for component in components {
                match current.get(component.as_str())? {
                    serde_json::Value::Object(next) => current = next,
                    _ => return None,
                }
            }
            match current.get(tail.as_str())? {
                serde_json::Value::String(s) => Some(s.as_str()),
                _ => None,
            }
        })
    }
}

/// Returns a closure that extracts tag values from a DatadogLogMsg.
///
/// Tags include:
/// - Known fields: `service`, `host`/`hostname`, `source`/`ddsource`, `status`
/// - Custom tags in `ddtags` as `key:value` pairs (e.g., `"env:prod"` matches key `"env"`)
/// - Attributes found in a somehow structured object that looks like something that is going to be
///   tag after pre-processing (quickwit-processing `create_preprocessing_pipeline` in
///   `processed_log.rs`).
///
/// The last case is a best effort. I don't think it captures the most relevant use case of the
/// preprocessing, which might be a structure containing a nested stringified structure. We might
/// want to re-evaluate at some point.
pub fn tag_accessor<'a>(msg: &'a DatadogLogMsg) -> impl Fn(&str) -> Option<&'a str> + 'a {
    move |key| match key {
        "service" => msg.service.as_deref().or_else(|| {
            msg_obj_str(
                &msg.message,
                &[
                    &["service"],
                    &["dd", "service"],
                    &["dd.service"],
                    &["syslog", "appname"],
                    &["syslog.appname"],
                ],
            )
        }),
        "host" | "hostname" => msg.hostname.as_deref().or_else(|| {
            msg_obj_str(
                &msg.message,
                &[
                    &["host"],
                    &["hostname"],
                    &["syslog", "hostname"],
                    &["syslog.hostname"],
                ],
            )
        }),
        "source" | "ddsource" => msg
            .ddsource
            .as_deref()
            .or_else(|| msg_obj_str(&msg.message, &[&["ddsource"], &["source"]])),
        "status" => msg.status.as_deref().or_else(|| {
            msg_obj_str(
                &msg.message,
                &[
                    &["status"],
                    &["severity"],
                    &["level"],
                    &["syslog", "severity"],
                    &["syslog.severity"],
                ],
            )
        }),
        "message" => match &msg.message {
            MessageValue::Str(s) => Some(s),
            _ => None,
        },
        _ => msg
            .ddtags
            .iter()
            .find_map(|tag| tag.strip_prefix(key)?.strip_prefix(':'))
            .or_else(|| msg_obj_ddtag(&msg.message, key)),
    }
}

/// Walks `"message"` → `"message"` object chains inside a `MessageValue::Obj`,
/// applying `f` at each level until it returns `Some`. Mirrors the flattening
/// done by quickwit-processing's `flatten_nested_message_object`.
///
/// For example, given `{"message": {"message": {"host": "my-host"}}}`, the closure
/// `f` is called first with `{"message": {"host": "my-host"}}`, then with
/// `{"host": "my-host"}`.
///
/// Does NOT parse `Value::String` messages as JSON — that would require allocation.
fn walk_message_chain<'a, F, T>(message: &'a MessageValue, f: F) -> Option<T>
where F: Fn(&'a serde_json::Map<String, serde_json::Value>) -> Option<T> {
    let MessageValue::Obj(first) = message else {
        return None;
    };
    let mut current = first;
    loop {
        if let Some(result) = f(current) {
            return Some(result);
        }
        match current.get("message") {
            Some(serde_json::Value::Object(nested)) => current = nested,
            _ => return None,
        }
    }
}

/// Looks up the first matching string value from `paths` inside a `MessageValue::Obj`.
///
/// Each path is a slice of keys to traverse, e.g. `&["dd", "service"]` matches
/// both `{"dd": {"service": "..."}}` and walks through nested objects.
fn msg_obj_str<'a>(message: &'a MessageValue, paths: &[&[&str]]) -> Option<&'a str> {
    walk_message_chain(message, |map| {
        paths.iter().find_map(|path| {
            let [components @ .., tail] = *path else {
                return None;
            };
            let mut current = map;
            for component in components {
                match current.get(*component)? {
                    serde_json::Value::Object(next) => current = next,
                    _ => return None,
                }
            }
            match current.get(*tail)? {
                serde_json::Value::String(s) => Some(s.as_str()),
                _ => None,
            }
        })
    })
}

/// Looks up a tag `key:value` pair inside a `"ddtags"` JSON array in a `MessageValue::Obj`.
fn msg_obj_ddtag<'a>(message: &'a MessageValue, tag_key: &str) -> Option<&'a str> {
    walk_message_chain(message, |map| {
        let serde_json::Value::Array(tags) = map.get("ddtags")? else {
            return None;
        };
        tags.iter().find_map(|v| {
            let serde_json::Value::String(s) = v else {
                return None;
            };
            s.strip_prefix(tag_key)?.strip_prefix(':')
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirrors `try_parse_datadog_log_messages` from rest_handler.rs:
    /// try strategy 1 (DatadogLogMsg serde) then strategy 2 (raw Map as Obj).
    fn parse_msg(json: &str) -> DatadogLogMsg {
        if let Ok(msg) = serde_json::from_str::<DatadogLogMsg>(json) {
            return msg;
        }
        let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(json).unwrap();
        DatadogLogMsg {
            message: MessageValue::Obj(obj),
            status: None,
            timestamp: None,
            hostname: None,
            service: None,
            ddsource: None,
            ddtags: Vec::new(),
        }
    }

    #[test]
    fn test_tag_accessor_known_fields() {
        // Strategy 1: JSON has "message" → parsed as DatadogLogMsg with struct fields filled
        let msg = parse_msg(
            r#"{"message": "test log", "service": "my-service", "hostname": "my-host", "ddsource": "nginx", "status": "info"}"#,
        );
        let getter = tag_accessor(&msg);

        assert_eq!(getter("service"), Some("my-service"));
        assert_eq!(getter("hostname"), Some("my-host"));
        assert_eq!(getter("ddsource"), Some("nginx"));
        assert_eq!(getter("status"), Some("info"));
        assert_eq!(getter("unknown"), None);
    }

    #[test]
    fn test_tag_accessor_field_name_aliases() {
        // Serde aliases: "host"→hostname, "source"→ddsource on the struct
        let msg = parse_msg(r#"{"message": "test log", "host": "my-host", "source": "nginx"}"#);
        let getter = tag_accessor(&msg);
        assert_eq!(getter("host"), Some("my-host"));
        assert_eq!(getter("hostname"), Some("my-host"));
        assert_eq!(getter("source"), Some("nginx"));
        assert_eq!(getter("ddsource"), Some("nginx"));

        // Alternate names from create_preprocessing_pipeline inside message obj
        let msg = parse_msg(r#"{"syslog.appname": "audit-svc"}"#);
        assert_eq!(tag_accessor(&msg)("service"), Some("audit-svc"));

        let msg = parse_msg(r#"{"hostname": "web-99"}"#);
        assert_eq!(tag_accessor(&msg)("host"), Some("web-99"));

        let msg = parse_msg(r#"{"source": "apache"}"#);
        assert_eq!(tag_accessor(&msg)("source"), Some("apache"));

        let msg = parse_msg(r#"{"level": "error"}"#);
        assert_eq!(tag_accessor(&msg)("status"), Some("error"));

        let msg = parse_msg(r#"{"syslog.severity": "crit"}"#);
        assert_eq!(tag_accessor(&msg)("status"), Some("crit"));

        // Nested object form of dotted keys (e.g. {"dd": {"service": ...}} for "dd.service")
        let msg = parse_msg(r#"{"message": {"dd": {"service": "audit-B1"}}}"#);
        assert_eq!(tag_accessor(&msg)("service"), Some("audit-B1"));

        let msg = parse_msg(r#"{"syslog": {"severity": "crit"}}"#);
        assert_eq!(tag_accessor(&msg)("status"), Some("crit"));

        let msg = parse_msg(r#"{"syslog": {"hostname": "web-99"}}"#);
        assert_eq!(tag_accessor(&msg)("host"), Some("web-99"));

        let msg = parse_msg(r#"{"syslog": {"appname": "audit-svc"}}"#);
        assert_eq!(tag_accessor(&msg)("service"), Some("audit-svc"));
    }

    #[test]
    fn test_tag_accessor_ddtags() {
        // Strategy 1: ddtags is a comma-separated string, deserialized into Vec<String>
        let msg = parse_msg(
            r#"{"message": "log with tags", "ddtags": "env:prod,team:backend,region:us-east-1"}"#,
        );
        let getter = tag_accessor(&msg);
        assert_eq!(getter("env"), Some("prod"));
        assert_eq!(getter("team"), Some("backend"));
        assert_eq!(getter("region"), Some("us-east-1"));
        assert_eq!(getter("missing_tag"), None);

        // Strategy 2: ddtags as JSON array in object
        let msg = parse_msg(r#"{"ddtags": ["my-tag:critical", "env:prod"]}"#);
        let getter = tag_accessor(&msg);
        assert_eq!(getter("my-tag"), Some("critical"));
        assert_eq!(getter("env"), Some("prod"));
        assert_eq!(getter("missing"), None);

        // Strategy 1: ddtags array inside message object
        let msg = parse_msg(r#"{"message": {"ddtags": ["team:backend"]}}"#);
        assert_eq!(tag_accessor(&msg)("team"), Some("backend"));

        // Nested message chain
        let msg = parse_msg(r#"{"message": {"message": {"ddtags": ["env:staging"]}}}"#);
        assert_eq!(tag_accessor(&msg)("env"), Some("staging"));
    }

    #[test]
    fn test_custom_field_accessor_nested_paths() {
        let msg = parse_msg(
            r#"{
                "user_id": "12345",
                "metadata": {
                    "ip": "192.168.1.1",
                    "browser": "chrome"
                }
            }"#,
        );
        let getter = custom_field_accessor(&msg);

        assert_eq!(getter(&["user_id".to_string()]), Some("12345"));
        assert_eq!(
            getter(&["metadata".to_string(), "ip".to_string()]),
            Some("192.168.1.1")
        );
        assert_eq!(
            getter(&["metadata".to_string(), "browser".to_string()]),
            Some("chrome")
        );
        assert_eq!(getter(&["missing".to_string()]), None);
        assert_eq!(
            getter(&["metadata".to_string(), "missing".to_string()]),
            None
        );

        // Custom field inside a nested message chain
        let msg =
            parse_msg(r#"{"message": {"message": {"event": {"result": {"status": "failed"}}}}}"#);
        let getter = custom_field_accessor(&msg);
        assert_eq!(
            getter(&[
                "event".to_string(),
                "result".to_string(),
                "status".to_string()
            ]),
            Some("failed")
        );
    }

    #[test]
    fn test_tag_accessor_fallback_to_message_obj() {
        // Strategy 2: fields in Obj, struct fields all None
        let msg = parse_msg(r#"{"service": "audit-pay", "host": "web-01", "status": "error"}"#);
        let getter = tag_accessor(&msg);
        assert_eq!(getter("service"), Some("audit-pay"));
        assert_eq!(getter("host"), Some("web-01"));
        assert_eq!(getter("status"), Some("error"));

        // Message→message chain
        let msg = parse_msg(r#"{"message": {"message": {"service": "audit-deep"}}}"#);
        assert_eq!(tag_accessor(&msg)("service"), Some("audit-deep"));

        // Sibling fields at different levels of the chain
        let msg =
            parse_msg(r#"{"message": {"host": "web-outer", "message": {"status": "error"}}}"#);
        let getter = tag_accessor(&msg);
        assert_eq!(getter("host"), Some("web-outer"));
        assert_eq!(getter("status"), Some("error"));

        // String message inside chain stops traversal (can't parse without allocating)
        let msg = parse_msg(r#"{"message": {"message": "{\"service\": \"audit-hidden\"}"}}"#);
        assert_eq!(tag_accessor(&msg)("service"), None);
    }

    #[test]
    fn test_failing_to_parse_stringified_json_message() {
        let msg = parse_msg(r#"{"message": {"message": "{\"service\": \"audit\"}"}}"#);
        let getter = tag_accessor(&msg);
        assert_eq!(getter("service"), None);

        // according to the preprocessor, it should assert
        // assert_eq!(getter("service"), Some("audit"));
    }
}
