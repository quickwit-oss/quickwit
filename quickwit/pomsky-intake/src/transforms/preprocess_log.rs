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
use vector::event::{Event, LogEvent};
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::EstimatedJsonEncodedSizeOf;
use vector_lib::config::clone_input_definitions;

/// Preprocesses log events before indexing. Dispatches to a
/// source-specific handler based on the event's `source_type` metadata.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreprocessLogConfig;

impl vector_lib::configurable::NamedComponent for PreprocessLogConfig {
    fn get_component_name(&self) -> &'static str {
        "preprocess_log"
    }
}

impl GenerateConfig for PreprocessLogConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "preprocess_log")]
impl TransformConfig for PreprocessLogConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(PreprocessLog))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct PreprocessLog;

impl FunctionTransform for PreprocessLog {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        if let Event::Log(ref mut log) = event {
            let source_type = log.metadata().source_type().unwrap_or("unknown");
            match source_type {
                "http_server" => preprocess_http_log(log),
                "datadog_agent" => preprocess_datadog_log(log),
                "opentelemetry" => preprocess_otlp_log(log),
                _ => preprocess_http_log(log),
            }
        }
        output.push(event);
    }
}

fn preprocess_http_log(log: &mut LogEvent) {
    let size_bytes = log.estimated_json_encoded_size_of().get() as i64;
    log.insert("size_bytes", size_bytes);
}

fn preprocess_datadog_log(log: &mut LogEvent) {
    let size_bytes = log.estimated_json_encoded_size_of().get() as i64;
    log.insert("size_bytes", size_bytes);
}

fn preprocess_otlp_log(log: &mut LogEvent) {
    let size_bytes = log.estimated_json_encoded_size_of().get() as i64;
    log.insert("size_bytes", size_bytes);
}

#[cfg(test)]
mod tests {
    use vector::event::{Event, LogEvent, Value};

    use super::*;

    fn run_transform(event: Event) -> Vec<Event> {
        let mut transform = PreprocessLog;
        let mut output = OutputBuffer::with_capacity(1);
        transform.transform(&mut output, event);
        output.into_events().collect()
    }

    fn make_log_event_with_source_type(message: &str, source_type: &str) -> Event {
        let mut log = LogEvent::default();
        log.insert("message", message);
        log.metadata_mut().set_source_type(source_type.to_string());
        Event::Log(log)
    }

    #[test]
    fn test_preprocess_http_log() {
        let event = make_log_event_with_source_type("hello", "http_server");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        match log.get("size_bytes") {
            Some(Value::Integer(n)) => assert!(*n > 0),
            other => panic!("expected positive Integer, got {other:?}"),
        }
    }

    #[test]
    fn test_preprocess_datadog_log() {
        let event = make_log_event_with_source_type("hello", "datadog_agent");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        match log.get("size_bytes") {
            Some(Value::Integer(n)) => assert!(*n > 0),
            other => panic!("expected positive Integer, got {other:?}"),
        }
    }

    #[test]
    fn test_preprocess_otlp_log() {
        let event = make_log_event_with_source_type("hello", "opentelemetry");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        match log.get("size_bytes") {
            Some(Value::Integer(n)) => assert!(*n > 0),
            other => panic!("expected positive Integer, got {other:?}"),
        }
    }

    #[test]
    fn test_unknown_source_falls_back_to_http() {
        let event = make_log_event_with_source_type("hello", "something_else");
        let events = run_transform(event);
        assert_eq!(events.len(), 1);
        assert!(events[0].as_log().get("size_bytes").is_some());
    }
}
