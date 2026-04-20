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

/// Explodes a Datadog agent trace chunk (one event with a `spans` array)
/// into individual `TraceEvent`s — one per span — with each span's fields
/// promoted to the top level. Events that are not from the `datadog_agent`
/// source (e.g. OTLP, which already emits one event per span) are passed
/// through unchanged.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExplodeTraceSpansConfig;

impl vector_lib::configurable::NamedComponent for ExplodeTraceSpansConfig {
    fn get_component_name(&self) -> &'static str {
        "explode_trace_spans"
    }
}

impl GenerateConfig for ExplodeTraceSpansConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "explode_trace_spans")]
impl TransformConfig for ExplodeTraceSpansConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(ExplodeTraceSpans))
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
struct ExplodeTraceSpans;

impl FunctionTransform for ExplodeTraceSpans {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let Event::Trace(trace) = &event else {
            output.push(event);
            return;
        };
        let source_type = trace.metadata().source_type().unwrap_or("unknown");

        if source_type != "datadog_agent" {
            output.push(event);
            return;
        }
        // Destructure into (fields, metadata) so we own both.
        let Event::Trace(trace) = event else {
            unreachable!();
        };
        let (mut fields, metadata) = trace.into_parts();

        let Some(Value::Array(spans)) = fields.remove("spans") else {
            // No spans array — emit the original event as-is.
            let restored = TraceEvent::from_parts(fields, metadata);
            output.push(Event::Trace(restored));
            return;
        };
        for span in spans {
            let Value::Object(span_fields) = span else {
                continue;
            };
            let span_event = TraceEvent::from_parts(span_fields, metadata.clone());
            output.push(Event::Trace(span_event));
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use vector::event::{Event, ObjectMap, TraceEvent, Value};

    use super::*;

    fn run_transform(event: Event) -> Vec<Event> {
        let mut transform = ExplodeTraceSpans;
        let mut output = OutputBuffer::with_capacity(4);
        transform.transform(&mut output, event);
        output.into_events().collect()
    }

    fn make_dd_span(service: &str, start_nanos: i64) -> Value {
        let dt = Utc.timestamp_nanos(start_nanos);
        let mut span = ObjectMap::new();
        span.insert("service".into(), Value::from(service));
        span.insert("name".into(), Value::from("test-span"));
        span.insert("start".into(), Value::Timestamp(dt));
        span.insert("duration".into(), Value::Integer(123));
        span.insert("trace_id".into(), Value::Integer(456));
        span.insert("span_id".into(), Value::Integer(789));
        Value::Object(span)
    }

    #[test]
    fn test_explode_dd_trace_into_individual_spans() {
        let mut trace = TraceEvent::default();
        trace
            .metadata_mut()
            .set_source_type("datadog_agent".to_string());
        trace.insert("env", "prod");
        trace.insert(
            "spans",
            Value::Array(vec![make_dd_span("svc-a", 123), make_dd_span("svc-b", 456)]),
        );

        let events = run_transform(Event::Trace(trace));
        assert_eq!(events.len(), 2);

        // Each output event is a single span with fields at top level.
        let span0 = events[0].as_trace();
        assert_eq!(span0.get("service"), Some(&Value::from("svc-a")),);
        assert!(span0.get("start").is_some());
        assert_eq!(span0.metadata().source_type(), Some("datadog_agent"),);
        // Chunk-level fields like "env" are not carried over.
        assert!(span0.get("env").is_none());
        // The "spans" key must not exist on the output.
        assert!(span0.get("spans").is_none());

        let span1 = events[1].as_trace();
        assert_eq!(span1.get("service"), Some(&Value::from("svc-b")),);
    }
}
