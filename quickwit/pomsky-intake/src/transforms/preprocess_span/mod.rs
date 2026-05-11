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

mod span_to_schema;

use serde::{Deserialize, Serialize};
use vector::config::{
    DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext, TransformOutput,
};
use vector::event::Event;
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

use self::span_to_schema::span_to_schema;

/// Preprocesses span events from the Datadog agent. Operates on the
/// apm-processing-aligned canonical span shape produced by
/// `preprocess_dd_trace` + `explode_trace_spans`; the tail of this transform
/// (`span_to_schema`) lowers the canonical span into the
/// `datadog-spans` index document. OTLP is not supported today.
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
        if let Event::Trace(trace) = &mut event
            && trace.metadata().source_type() == Some("datadog_agent")
        {
            // Future span-level processors run here, operating on the
            // apm-processing-aligned canonical shape. `span_to_schema`
            // is the tail that lowers canonical → index doc.
            span_to_schema(trace);
        }
        output.push(event);
    }
}

#[cfg(test)]
mod tests {
    use vector::event::{Event, TraceEvent};

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

    #[test]
    fn test_unknown_source_type_passes_through() {
        let event = make_trace_with_source_type("unknown");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        let trace = events[0].as_trace();
        // span_to_schema doesn't run — the event is untouched.
        assert!(trace.get("status").is_none());
        assert!(trace.get("single_span").is_none());
    }
}
