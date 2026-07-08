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

use std::fmt;

use quickwit_common::get_from_env_opt;
use serde_json::{Map, Value};
use time::format_description::BorrowedFormatItem;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::format::{
    DefaultFields, Format, FormatEvent, FormatFields, Full, Json, JsonFields, Writer,
};
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::registry::LookupSpan;

/// We do not rely on the RFC3339 implementation, because it has a nanosecond precision.
/// See discussion here: https://github.com/time-rs/time/discussions/418
pub(crate) fn time_formatter() -> UtcTime<Vec<BorrowedFormatItem<'static>>> {
    let time_format = time::format_description::parse(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
    )
    .expect("time format description should be valid");
    UtcTime::new(time_format)
}

pub(crate) enum EventFormat<'a> {
    Full(Format<Full, UtcTime<Vec<BorrowedFormatItem<'a>>>>),
    Json(Format<Json>),
    Ddg(DdgFormat),
}

impl EventFormat<'_> {
    /// Gets the log format from the environment variable `QW_LOG_FORMAT`.
    pub(crate) fn get_from_env() -> Self {
        match get_from_env_opt::<String>("QW_LOG_FORMAT", false)
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("json") => EventFormat::Json(tracing_subscriber::fmt::format().json()),
            Some("ddg") => EventFormat::Ddg(DdgFormat::new()),
            _ => {
                let full_format = tracing_subscriber::fmt::format()
                    .with_target(true)
                    .with_timer(time_formatter());
                EventFormat::Full(full_format)
            }
        }
    }

    pub(crate) fn format_fields(&self) -> FieldFormat {
        match self {
            EventFormat::Full(_) | EventFormat::Ddg(_) => {
                FieldFormat::Default(DefaultFields::new())
            }
            EventFormat::Json(_) => FieldFormat::Json(JsonFields::new()),
        }
    }
}

impl<S, N> FormatEvent<S, N> for EventFormat<'_>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        match self {
            EventFormat::Full(format) => format.format_event(ctx, writer, event),
            EventFormat::Json(format) => format.format_event(ctx, writer, event),
            EventFormat::Ddg(format) => format.format_event(ctx, writer, event),
        }
    }
}

/// Reserved top-level keys in the DDG output. Event fields whose name collides
/// with one of these are dropped from the structured attributes (their value is
/// still rendered inside `message`) to avoid overwriting the reserved value.
const RESERVED_KEYS: [&str; 5] = ["timestamp", "level", "service", "ddsource", "message"];

/// Records an event's structured fields directly into the output JSON object so
/// they are emitted as top-level attributes (faceted in Datadog), mirroring the
/// structured fields the JSON formatter exposes.
///
/// A visitor is required because `tracing` exposes field *values* only through
/// the [`Visit`] callbacks (`Event::fields` yields names, not values). The
/// implicit `message` field is excluded (it is reserved and already rendered
/// into the human-readable `message`), as is any field colliding with a reserved
/// key. Typed values (numbers, booleans, strings) preserve their JSON type;
/// everything else falls back to its `Debug` representation as a string.
struct FieldCollector<'a> {
    output: &'a mut Map<String, Value>,
}

impl FieldCollector<'_> {
    fn insert(&mut self, field: &Field, value: Value) {
        let name = field.name();
        if RESERVED_KEYS.contains(&name) {
            return;
        }
        self.output.insert(name.to_string(), value);
    }
}

/// Returns the parsed JSON when `value` is a serialized array or object, else
/// `None`. Only containers are promoted.
fn as_json_container(value: &str) -> Option<Value> {
    if !value.trim_start().starts_with(['[', '{']) {
        return None;
    }
    match serde_json::from_str(value) {
        Ok(json @ (Value::Array(_) | Value::Object(_))) => Some(json),
        _ => None,
    }
}

impl Visit for FieldCollector<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert(field, value.into());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, value.into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        // A genuine string field is kept verbatim — we do not reinterpret string
        // values as JSON.
        self.insert(field, value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // A `Debug`-rendered field is the only place structured data gets
        // flattened into a string (a `Vec`/struct logged via `?`, since `tracing`
        // cannot carry nested values). When that rendering happens to be a valid
        // JSON array/object (e.g. `["a", "b"]` from a `Vec<String>`), embed it as
        // nested JSON. Non-JSON Debug output (ranges, structs, ...) stays a plain
        // string, so this is never worse than the raw Debug rendering.
        let rendered = format!("{value:?}");
        match as_json_container(&rendered) {
            Some(json) => self.insert(field, json),
            None => self.insert(field, rendered.into()),
        }
    }
}

/// Outputs JSON with `timestamp`, `level`, `service`, `ddsource`, and `message`
/// fields. The `message` is formatted using the regular text formatter (level,
/// target, spans, fields).
///
/// The event's own structured fields are additionally emitted as top-level
/// attributes (the same key/values the JSON formatter exposes), excluding any
/// field whose name collides with a reserved key (see [`RESERVED_KEYS`]). Span
/// fields are not promoted; they remain rendered inside `message`.
///
/// Example output:
/// ```json
/// {"timestamp":"2025-03-23T14:30:45Z","level":"INFO","service":"quickwit","ddsource":"quickwit","message":"INFO quickwit_search: hello","key":"value","count":42}
/// ```
pub(crate) struct DdgFormat {
    text_format: Format<Full, ()>,
}

impl DdgFormat {
    fn new() -> Self {
        Self {
            text_format: tracing_subscriber::fmt::format()
                .with_target(true)
                .without_time(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for DdgFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // Render the event as text using the Full formatter (without timestamp)
        let mut message = String::with_capacity(256);
        self.text_format
            .format_event(ctx, Writer::new(&mut message), event)?;
        let message = message.trim();

        // Timestamp (RFC 3339)
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| fmt::Error)?;

        let level = event.metadata().level().as_str();

        // Build the output object. Reserved keys are inserted first; the
        // collector then records the event's structured fields directly, skipping
        // any field that would collide with a reserved key.
        let mut output = Map::with_capacity(RESERVED_KEYS.len());
        output.insert("timestamp".to_string(), timestamp.into());
        output.insert("level".to_string(), level.into());
        output.insert("service".to_string(), "quickwit".into());
        output.insert("ddsource".to_string(), "quickwit".into());
        output.insert("message".to_string(), message.into());
        event.record(&mut FieldCollector {
            output: &mut output,
        });

        let line = serde_json::to_string(&output).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

pub(crate) enum FieldFormat {
    Default(DefaultFields),
    Json(JsonFields),
}

impl FormatFields<'_> for FieldFormat {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'_>, fields: R) -> fmt::Result {
        match self {
            FieldFormat::Default(default_fields) => default_fields.format_fields(writer, fields),
            FieldFormat::Json(json_fields) => json_fields.format_fields(writer, fields),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    /// A shared buffer writer for capturing log output in tests.
    #[derive(Clone, Default)]
    struct TestMakeWriter(Arc<Mutex<Vec<u8>>>);

    impl TestMakeWriter {
        fn get_string(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TestMakeWriter {
        type Writer = TestWriter;

        fn make_writer(&'a self) -> Self::Writer {
            TestWriter(self.0.clone())
        }
    }

    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write_all(buf)?;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Sets up a subscriber with `DdgFormat` and captures the output.
    fn capture_ddg_log<F: FnOnce()>(f: F) -> serde_json::Value {
        let writer = TestMakeWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(DdgFormat::new())
                .fmt_fields(FieldFormat::Default(DefaultFields::new()))
                .with_ansi(false)
                .with_writer(writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, f);
        let output = writer.get_string();
        serde_json::from_str(&output).expect("output should be valid JSON")
    }

    const TARGET: &str = module_path!();

    #[test]
    fn test_ddg_format_has_expected_fields() {
        let json = capture_ddg_log(|| tracing::info!("hello"));
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 5, "{obj:?}");
        assert!(obj.contains_key("timestamp"));
        assert!(obj.contains_key("level"));
        assert!(obj.contains_key("service"));
        assert!(obj.contains_key("ddsource"));
        assert!(obj.contains_key("message"));
    }

    #[test]
    fn test_ddg_format_basic_message() {
        let json = capture_ddg_log(|| tracing::info!("hello world"));
        assert_eq!(json["level"], "INFO");
        assert_eq!(json["service"], "quickwit");
        assert_eq!(json["ddsource"], "quickwit");
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: hello world")
        );
    }

    #[test]
    fn test_ddg_format_with_fields() {
        let json = capture_ddg_log(|| {
            tracing::info!(key = "value", count = 42, "processing request");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: processing request key=\"value\" count=42")
        );
        // Structured fields are also emitted as top-level attributes.
        assert_eq!(json["key"], "value");
        assert_eq!(json["count"], 42);
    }

    #[test]
    fn test_ddg_format_promotes_typed_fields() {
        let json = capture_ddg_log(|| {
            tracing::info!(
                text = "hi",
                count = 42_i64,
                ratio = 0.5_f64,
                enabled = true,
                "typed",
            );
        });
        let obj = json.as_object().unwrap();
        // 5 reserved keys + 4 promoted fields.
        assert_eq!(obj.len(), 9, "{obj:?}");
        assert_eq!(json["text"], Value::from("hi"));
        assert_eq!(json["count"], Value::from(42));
        assert!(json["count"].is_i64());
        assert_eq!(json["ratio"], Value::from(0.5));
        assert!(json["ratio"].is_f64());
        assert_eq!(json["enabled"], Value::Bool(true));
    }

    #[test]
    fn test_ddg_format_message_field_not_duplicated() {
        // The implicit `message` field must not appear twice or overwrite the
        // rendered message.
        let json = capture_ddg_log(|| tracing::info!("hello"));
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 5, "{obj:?}");
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: hello")
        );
    }

    #[test]
    fn test_ddg_format_reserved_key_collision_is_protected() {
        // A user field named like a reserved key must not overwrite the reserved
        // value; it is still visible inside the rendered message.
        let json = capture_ddg_log(|| {
            tracing::info!(level = "custom", service = "other", "collision");
        });
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 5, "{obj:?}");
        assert_eq!(json["level"], "INFO");
        assert_eq!(json["service"], "quickwit");
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: collision level=\"custom\" service=\"other\"")
        );
    }

    #[test]
    fn test_ddg_format_span_fields_not_promoted() {
        // Span fields stay inside the rendered message; only the event's own
        // fields become top-level attributes.
        let json = capture_ddg_log(|| {
            let span = tracing::info_span!("my_span", span_field = 123);
            let _guard = span.enter();
            tracing::info!(event_field = "value", "inside span");
        });
        let obj = json.as_object().unwrap();
        // 5 reserved keys + 1 event field. The span field is not promoted.
        assert_eq!(obj.len(), 6, "{obj:?}");
        assert_eq!(json["event_field"], "value");
        assert!(!obj.contains_key("span_field"));
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO my_span{{span_field=123}}: {TARGET}: inside span event_field=\"value\"")
        );
    }

    #[test]
    fn test_ddg_format_with_span() {
        let json = capture_ddg_log(|| {
            let span = tracing::info_span!("my_span", id = 123);
            let _guard = span.enter();
            tracing::info!("inside span");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO my_span{{id=123}}: {TARGET}: inside span")
        );
    }

    /// Captures raw text output using the production Full formatter (with timestamp, no ANSI).
    fn capture_full_log<F: FnOnce()>(f: F) -> String {
        let writer = TestMakeWriter::default();
        let full_format = tracing_subscriber::fmt::format()
            .with_target(true)
            .with_timer(time_formatter());
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(full_format)
                .fmt_fields(DefaultFields::new())
                .with_ansi(false)
                .with_writer(writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, f);
        writer.get_string().trim_end().to_string()
    }

    #[test]
    fn test_ddg_format_with_nested_spans() {
        let make_event = || {
            let outer = tracing::info_span!("outer", req_id = 42);
            let _outer_guard = outer.enter();
            let inner = tracing::info_span!("inner", step = "parse");
            let _inner_guard = inner.enter();
            tracing::info!("deep inside");
        };

        // Compare DDG message against production Full formatter output.
        // The only difference is the leading timestamp.
        let full_output = capture_full_log(make_event);
        let json = capture_ddg_log(make_event);
        let ddg_message = json["message"].as_str().unwrap();

        // Full output: "2025-03-23T14:30:45.123Z  INFO outer{...}: target: deep inside"
        // DDG message:                            "INFO outer{...}: target: deep inside"
        // The timestamp adds one extra space of padding, so we trim both and compare.
        let full_without_timestamp = full_output
            .find("  ")
            .map(|pos| &full_output[pos..])
            .unwrap_or(&full_output);
        assert_eq!(
            ddg_message.trim_start(),
            full_without_timestamp.trim_start(),
        );

        assert_eq!(
            ddg_message,
            format!("INFO outer{{req_id=42}}:inner{{step=\"parse\"}}: {TARGET}: deep inside")
        );
    }

    #[test]
    fn test_ddg_format_escapes_special_chars() {
        let json = capture_ddg_log(|| {
            tracing::info!(r#"hello "world" with\backslash"#);
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!(r#"INFO {TARGET}: hello "world" with\backslash"#)
        );
    }

    #[test]
    fn test_ddg_format_escapes_newlines() {
        let json = capture_ddg_log(|| {
            tracing::info!("line1\nline2\ttab");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: line1\nline2\ttab")
        );
    }

    #[test]
    fn test_ddg_format_levels() {
        for (expected_level, log_fn) in [
            (
                "WARN",
                Box::new(|| tracing::warn!("w")) as Box<dyn FnOnce()>,
            ),
            ("ERROR", Box::new(|| tracing::error!("e"))),
            ("INFO", Box::new(|| tracing::info!("i"))),
        ] {
            let json = capture_ddg_log(log_fn);
            assert_eq!(json["level"], expected_level);
        }
    }

    #[test]
    fn test_ddg_format_timestamp_is_rfc3339() {
        let json = capture_ddg_log(|| tracing::info!("hello"));
        let ts = json["timestamp"].as_str().unwrap();
        time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|err| panic!("timestamp is not valid RFC 3339: {ts}: {err}"));
    }

    #[test]
    fn test_ddg_format_with_bool_and_float_fields() {
        let json = capture_ddg_log(|| {
            tracing::info!(enabled = true, ratio = 0.75, "status check");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: status check enabled=true ratio=0.75")
        );
    }

    #[test]
    fn test_ddg_format_str_fields_kept_verbatim() {
        // Genuine string fields are never reinterpreted as JSON, even when their
        // contents would parse as a JSON array or object. Only the Debug path
        // (where a `Vec`/struct was flattened to a string) attempts promotion.
        let json = capture_ddg_log(|| {
            tracing::info!(
                members = r#"["10215528..15153955", "15779969..16133624"]"#,
                fields = r#"{"host": "quickwit", "count": 4}"#,
                merged = "10215528..18151721",
                version = "1.2",
                "compaction",
            );
        });
        assert_eq!(
            json["members"],
            Value::from(r#"["10215528..15153955", "15779969..16133624"]"#)
        );
        assert!(json["members"].is_string());
        assert_eq!(
            json["fields"],
            Value::from(r#"{"host": "quickwit", "count": 4}"#)
        );
        assert!(json["fields"].is_string());
        assert_eq!(json["merged"], Value::from("10215528..18151721"));
        assert_eq!(json["version"], Value::from("1.2"));
    }

    /// Captures output from the built-in `Json` formatter (the `QW_LOG_FORMAT=json`
    /// path), the same way production wires it up in `EventFormat`.
    fn capture_json_log<F: FnOnce()>(f: F) -> serde_json::Value {
        let writer = TestMakeWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(tracing_subscriber::fmt::format().json())
                .fmt_fields(JsonFields::new())
                .with_ansi(false)
                .with_writer(writer.clone()),
        );
        tracing::subscriber::with_default(subscriber, f);
        let output = writer.get_string();
        serde_json::from_str(&output).expect("output should be valid JSON")
    }

    #[test]
    fn test_json_format_has_same_nested_field_limitation() {
        // The built-in Json formatter is subject to the exact same constraint as
        // DdgFormat: `tracing` cannot carry a nested value, so a Vec must be
        // stringified at the call site before any formatter sees it.

        // Logged via Debug (`?`): the Json formatter emits an escaped JSON STRING,
        // not a nested array — identical to the DDG bug being reported.
        let json = capture_json_log(|| {
            let members = vec![
                "10215528..15153955".to_string(),
                "15779969..16133624".to_string(),
            ];
            tracing::info!(members = ?members, "probe");
        });
        let fields = &json["fields"];
        assert_eq!(
            fields["members"],
            Value::from(r#"["10215528..15153955", "15779969..16133624"]"#),
            "Json formatter also renders a Debug-logged Vec as an escaped string, not an array: \
             {json}"
        );
        assert!(
            fields["members"].is_string(),
            "value is a string, not a nested array: {json}"
        );

        // Logged as a serde JSON string field: the Json formatter ALSO keeps it a
        // string (it does not re-parse), so the escaping is still there.
        let json2 = capture_json_log(|| {
            let members = vec!["a".to_string(), "b".to_string()];
            let serialized = serde_json::to_string(&members).unwrap();
            tracing::info!(members = serialized.as_str(), "probe");
        });
        assert_eq!(json2["fields"]["members"], Value::from(r#"["a","b"]"#));
        assert!(json2["fields"]["members"].is_string());
    }

    #[test]
    fn test_ddg_format_promotes_debug_logged_string_vec() {
        // The reported case: a `Vec<String>` logged via `?`. Rust's Debug output
        // is valid JSON, so it is promoted to a nested array.
        let json = capture_ddg_log(|| {
            let members = vec![
                "10215528..15153955".to_string(),
                "15779969..16133624".to_string(),
            ];
            let components = vec![
                "fastfield:host".to_string(),
                "fastfield:service".to_string(),
            ];
            tracing::info!(members = ?members, components = ?components, "coalesced");
        });
        assert_eq!(
            json["members"],
            serde_json::json!(["10215528..15153955", "15779969..16133624"])
        );
        assert!(json["members"].is_array());
        assert_eq!(
            json["components"],
            serde_json::json!(["fastfield:host", "fastfield:service"])
        );
    }

    #[test]
    fn test_ddg_format_promotes_debug_logged_numeric_vec() {
        // A `Vec` of numbers also Debug-renders as valid JSON.
        let json = capture_ddg_log(|| {
            let sizes = vec![1u64, 2, 3];
            tracing::info!(sizes = ?sizes, "sizes");
        });
        assert_eq!(json["sizes"], serde_json::json!([1, 2, 3]));
        assert!(json["sizes"].is_array());
    }

    #[test]
    fn test_ddg_format_debug_non_json_stays_string() {
        // The boundary of the best-effort approach: Debug output that is NOT valid
        // JSON stays a plain string. A `Vec<Range>` Debug-renders as `[a..b]`,
        // which is not JSON, so it is left untouched (never worse than before).
        let json = capture_ddg_log(|| {
            let ranges = vec![10usize..15, 20..25];
            tracing::info!(ranges = ?ranges, "ranges");
        });
        assert_eq!(json["ranges"], Value::from("[10..15, 20..25]"));
        assert!(json["ranges"].is_string());
    }

    #[test]
    fn test_ddg_format_debug_struct_stays_string() {
        // A struct's Debug output (`Foo { .. }`) is not JSON, so it stays a string.
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Foo {
            a: u32,
        }
        let json = capture_ddg_log(|| {
            tracing::info!(foo = ?Foo { a: 1 }, "struct");
        });
        assert_eq!(json["foo"], Value::from("Foo { a: 1 }"));
        assert!(json["foo"].is_string());
    }

    #[test]
    fn test_ddg_format_fields_only() {
        let json = capture_ddg_log(|| {
            tracing::info!(action = "ping");
        });
        assert_eq!(
            json["message"].as_str().unwrap(),
            format!("INFO {TARGET}: action=\"ping\"")
        );
    }
}
