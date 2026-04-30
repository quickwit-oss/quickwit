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

mod remap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, TraceEvent, Value};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

use self::remap::remap_dd_span_to_schema;

/// Preprocesses span events (post-explode for Datadog agent, native for
/// OTLP). Dispatches to a source-specific handler based on the event's
/// `source_type` metadata.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreprocessSpanConfig;

impl vector_lib::configurable::NamedComponent for PreprocessSpanConfig {
    fn get_component_name(&self) -> &'static str {
        "preprocess_span"
    }
}

impl GenerateConfig for PreprocessSpanConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "preprocess_span")]
impl TransformConfig for PreprocessSpanConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(PreprocessSpan))
    }

    fn input(&self) -> Input {
        Input::trace()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Trace,
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PreprocessSpan;

impl FunctionTransform for PreprocessSpan {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        if let Event::Trace(trace) = &mut event {
            let source_type = trace.metadata().source_type().unwrap_or("unknown");
            match source_type {
                "datadog_agent" => preprocess_datadog_trace(trace),
                "opentelemetry" => preprocess_otlp_trace(trace),
                _ => {}
            }
        }
        output.push(event);
    }
}

/// After the `explode_trace_spans` transform, each Datadog agent TraceEvent
/// represents a single span with fields at the top level.
///
/// Emits the three timestamp fields the `datadog-spans` index expects:
/// - `start_time` — span start, full nanosecond precision (i64, unix ns)
/// - `timestamp` — span end = `floor((start + duration) / 1e6)` as rfc3339 with millisecond
///   precision and a `Z` suffix; this is the index's `timestamp_field`, so the doc is dropped
///   without it
/// - `discovery_timestamp` — when intake observed the span (i64, unix ms)
///
/// Also normalizes the IDs to unsigned 64-bit decimal strings:
/// - `trace_id`, `span_id`, `parent_id` (i64, really u64) → decimal string
/// - `trace_id_low` — copy of `trace_id` (the lower 64 bits as decimal). Kept for compatibility
///   with SaaS docs where `trace_id` may be a 128-bit hex string and `trace_id_low` carries the
///   lower 64 bits separately. From intake, both fields hold the same value.
///
/// The upper 64 bits of 128-bit Datadog trace IDs aren't reliably
/// available on the wire (the SaaS pipeline assembles them downstream),
/// so we emit only what we have — the lower 64 bits — in a single canonical
/// decimal form.
fn preprocess_datadog_trace(trace: &mut TraceEvent) {
    let start_dt_opt: Option<DateTime<Utc>> = match trace.get("start") {
        Some(Value::Timestamp(dt)) => Some(*dt),
        _ => None,
    };
    let duration_ns: i64 = match trace.get("duration") {
        Some(Value::Integer(d)) => *d,
        _ => 0,
    };

    if let Some(dt) = start_dt_opt
        && let Some(start_ns) = dt.timestamp_nanos_opt()
    {
        trace.insert("start_time", start_ns);
        let end_ns = start_ns.saturating_add(duration_ns);
        let end_ms = end_ns.div_euclid(1_000_000);
        if let Some(end_dt) = DateTime::<Utc>::from_timestamp_millis(end_ms) {
            trace.insert(
                "timestamp",
                end_dt.to_rfc3339_opts(SecondsFormat::Millis, true),
            );
        }
    }

    trace.insert("discovery_timestamp", Utc::now().timestamp_millis());

    if let Some(Value::Integer(raw)) = trace.get("trace_id") {
        let decimal = (*raw as u64).to_string();
        trace.insert("trace_id", decimal.clone());
        trace.insert("trace_id_low", decimal);
    }

    if let Some(Value::Integer(raw)) = trace.get("span_id") {
        trace.insert("span_id", (*raw as u64).to_string());
    }

    if let Some(Value::Integer(raw)) = trace.get("parent_id") {
        trace.insert("parent_id", (*raw as u64).to_string());
    }

    remap_dd_span_to_schema(trace);
}

/// OTLP spans already carry `trace_id` (32-char hex) and `span_id` (16-char
/// hex) in the right format. Only the timestamp needs normalizing:
/// `start_time_unix_nano` (DateTime) → `start_timestamp_nanos` (unix micros).
fn preprocess_otlp_trace(trace: &mut TraceEvent) {
    if let Some(Value::Timestamp(dt)) = trace.get("start_time_unix_nano") {
        let start_micros = dt.timestamp_micros();
        trace.insert("start_timestamp_nanos", start_micros);
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use vector::event::{Event, TraceEvent, Value};

    use super::*;

    fn run_transform(event: Event) -> Vec<Event> {
        let mut transform = PreprocessSpan;
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);
        output.into_events().collect()
    }

    fn make_trace_with_source_type(source_type: &str) -> Event {
        let mut trace = TraceEvent::default();
        trace
            .metadata_mut()
            .set_source_type(source_type.to_string());
        Event::Trace(trace)
    }

    fn make_dd_trace(trace_id: i64, span_id: i64, start_nanos: i64) -> Event {
        make_dd_trace_with_duration(trace_id, span_id, start_nanos, 0)
    }

    fn make_dd_trace_with_duration(
        trace_id: i64,
        span_id: i64,
        start_nanos: i64,
        duration_nanos: i64,
    ) -> Event {
        let dt = Utc.timestamp_nanos(start_nanos);
        let mut trace = TraceEvent::default();
        trace
            .metadata_mut()
            .set_source_type("datadog_agent".to_string());
        trace.insert("start", dt);
        trace.insert("duration", duration_nanos);
        trace.insert("trace_id", trace_id);
        trace.insert("span_id", span_id);
        Event::Trace(trace)
    }

    #[test]
    fn test_dd_timestamps() {
        // start = 2024-08-19T09:35:43.000Z, duration = 5 ms.
        let events = run_transform(make_dd_trace_with_duration(
            1,
            1,
            1_724_060_143_000_000_000,
            5_000_000,
        ));
        let trace = events[0].as_trace();
        assert_eq!(
            trace.get("start_time"),
            Some(&Value::Integer(1_724_060_143_000_000_000)),
        );
        assert_eq!(
            trace.get("timestamp"),
            Some(&Value::from("2024-08-19T09:35:43.005Z")),
        );
        // `discovery_timestamp` is set to "now" — just check it's present.
        assert!(matches!(
            trace.get("discovery_timestamp"),
            Some(Value::Integer(_)),
        ));
    }

    #[test]
    fn test_dd_trace_id_decimal() {
        // trace_id = 12345678 → decimal string. trace_id_low mirrors it.
        let events = run_transform(make_dd_trace(12345678, 1, 0));
        let trace = events[0].as_trace();
        assert_eq!(trace.get("trace_id"), Some(&Value::from("12345678")));
        assert_eq!(trace.get("trace_id_low"), Some(&Value::from("12345678")));
    }

    #[test]
    fn test_dd_span_id() {
        // span_id 12345678 → decimal string "12345678".
        let events = run_transform(make_dd_trace(1, 12345678, 0));
        assert_eq!(
            events[0].as_trace().get("span_id"),
            Some(&Value::from("12345678")),
        );
    }

    #[test]
    fn test_dd_parent_id() {
        // parent_id u64 → decimal string. Root spans have parent_id = 0.
        let mut event = make_dd_trace(1, 1, 0);
        let Event::Trace(trace) = &mut event else {
            unreachable!();
        };
        trace.insert("parent_id", 4242i64);

        let events = run_transform(event);
        assert_eq!(
            events[0].as_trace().get("parent_id"),
            Some(&Value::from("4242")),
        );
    }

    #[test]
    fn test_dd_large_ids_reinterpreted_as_unsigned() {
        // Vector casts u64 to i64, so large u64s become negative i64s.
        // u64::MAX = 18446744073709551615 = -1 as i64.
        let events = run_transform(make_dd_trace(-1, -1, 0));
        let trace = events[0].as_trace();
        assert_eq!(
            trace.get("trace_id"),
            Some(&Value::from("18446744073709551615")),
        );
        assert_eq!(
            trace.get("trace_id_low"),
            Some(&Value::from("18446744073709551615")),
        );
        assert_eq!(
            trace.get("span_id"),
            Some(&Value::from("18446744073709551615")),
        );
    }

    #[test]
    fn test_otlp_start_timestamp() {
        let dt = Utc.timestamp_nanos(1_724_060_143_000_000_000);
        let mut trace = TraceEvent::default();
        trace
            .metadata_mut()
            .set_source_type("opentelemetry".to_string());
        trace.insert("start_time_unix_nano", dt);
        trace.insert("trace_id", "463ac35a2141473600000000deadbeef");
        trace.insert("span_id", "00000000cafebabe");

        let events = run_transform(Event::Trace(trace));
        assert_eq!(
            events[0].as_trace().get("start_timestamp_nanos"),
            Some(&Value::Integer(1_724_060_143_000_000)),
        );
        // OTLP IDs are already hex strings — left unchanged.
        assert_eq!(
            events[0].as_trace().get("trace_id"),
            Some(&Value::from("463ac35a2141473600000000deadbeef")),
        );
        assert_eq!(
            events[0].as_trace().get("span_id"),
            Some(&Value::from("00000000cafebabe")),
        );
    }

    #[test]
    fn test_preprocess_trace_without_start_field() {
        let event = make_trace_with_source_type("datadog_agent");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        let trace = events[0].as_trace();
        assert!(trace.get("start_time").is_none());
        assert!(trace.get("timestamp").is_none());
        // `discovery_timestamp` is still set even when `start` is missing.
        assert!(matches!(
            trace.get("discovery_timestamp"),
            Some(Value::Integer(_)),
        ));
    }

    #[test]
    fn test_unknown_source_type_passes_through() {
        let event = make_trace_with_source_type("unknown");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        let trace = events[0].as_trace();
        assert!(trace.get("start_time").is_none());
        assert!(trace.get("timestamp").is_none());
        assert!(trace.get("discovery_timestamp").is_none());
    }
}
