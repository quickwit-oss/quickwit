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

use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::{Event, TraceEvent, Value};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

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
        if let Event::Trace(ref mut trace) = event {
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
/// Normalizes:
/// - `start` (DateTime) → `start_timestamp_nanos` (unix micros)
/// - `trace_id` (i64, really u64) → 32-char hex, zero-padded or using `meta._dd.p.tid` for the
///   upper 64 bits when present
/// - `span_id` (i64, really u64) → 16-char hex
fn preprocess_datadog_trace(trace: &mut TraceEvent) {
    if let Some(Value::Timestamp(dt)) = trace.get("start") {
        let start_micros = dt.timestamp_micros();
        trace.insert("start_timestamp_nanos", start_micros);
    }

    if let Some(Value::Integer(raw)) = trace.get("trace_id") {
        let lower = *raw as u64;
        let default_upper = std::borrow::Cow::Borrowed("0000000000000000");
        let upper_hex = trace
            .get("meta._dd.p.tid")
            .and_then(|value| value.as_str())
            .unwrap_or(default_upper);
        let hex_trace_id = format!("{upper_hex}{lower:016x}");
        trace.insert("trace_id", hex_trace_id);
    }

    if let Some(Value::Integer(raw)) = trace.get("span_id") {
        let hex_span_id = format!("{:016x}", *raw as u64);
        trace.insert("span_id", hex_span_id);
    }
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
        let dt = Utc.timestamp_nanos(start_nanos);
        let mut trace = TraceEvent::default();
        trace
            .metadata_mut()
            .set_source_type("datadog_agent".to_string());
        trace.insert("start", dt);
        trace.insert("trace_id", trace_id);
        trace.insert("span_id", span_id);
        Event::Trace(trace)
    }

    #[test]
    fn test_dd_start_timestamp() {
        // 1_724_060_143_000_000_000 ns = 1_724_060_143_000_000 µs
        let events = run_transform(make_dd_trace(1, 1, 1_724_060_143_000_000_000));
        assert_eq!(
            events[0].as_trace().get("start_timestamp_nanos"),
            Some(&Value::Integer(1_724_060_143_000_000)),
        );
    }

    #[test]
    fn test_dd_trace_id_without_upper_bits() {
        // trace_id 0xBC614E = 12345678, no _dd.p.tid → upper 16 chars are zeros.
        let events = run_transform(make_dd_trace(12345678, 1, 0));
        assert_eq!(
            events[0].as_trace().get("trace_id"),
            Some(&Value::from("00000000000000000000000000bc614e")),
        );
    }

    #[test]
    fn test_dd_trace_id_with_upper_bits() {
        let mut event = make_dd_trace(12345678, 1, 0);
        let Event::Trace(ref mut trace) = event else {
            unreachable!();
        };
        trace.insert("meta._dd.p.tid", "463ac35a21414736");

        let events = run_transform(event);
        assert_eq!(
            events[0].as_trace().get("trace_id"),
            Some(&Value::from("463ac35a214147360000000000bc614e")),
        );
    }

    #[test]
    fn test_dd_span_id() {
        // span_id 0xBC614E = 12345678
        let events = run_transform(make_dd_trace(1, 12345678, 0));
        assert_eq!(
            events[0].as_trace().get("span_id"),
            Some(&Value::from("0000000000bc614e")),
        );
    }

    #[test]
    fn test_dd_large_ids_reinterpreted_as_unsigned() {
        // Vector casts u64 to i64, so large u64s become negative i64s.
        // 0xFFFFFFFFFFFFFFFF = u64::MAX = -1 as i64.
        let events = run_transform(make_dd_trace(-1, -1, 0));
        assert_eq!(
            events[0].as_trace().get("trace_id"),
            Some(&Value::from("0000000000000000ffffffffffffffff")),
        );
        assert_eq!(
            events[0].as_trace().get("span_id"),
            Some(&Value::from("ffffffffffffffff")),
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
        assert!(events[0].as_trace().get("start_timestamp_nanos").is_none());
    }

    #[test]
    fn test_unknown_source_type_passes_through() {
        let event = make_trace_with_source_type("unknown");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        assert!(events[0].as_trace().get("start_timestamp_nanos").is_none());
    }
}
