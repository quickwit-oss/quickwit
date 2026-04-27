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
use vector::event::Event;
use vector::schema::Definition;
use vector::transforms::{FunctionTransform, OutputBuffer, Transform};
use vector_lib::config::clone_input_definitions;

/// Preprocesses Datadog agent trace chunks before they are exploded into
/// individual spans. Currently a pass-through; processors will be added
/// here as the trace-level pipeline grows.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreprocessDdTraceConfig;

impl vector_lib::configurable::NamedComponent for PreprocessDdTraceConfig {
    fn get_component_name(&self) -> &'static str {
        "preprocess_dd_trace"
    }
}

impl GenerateConfig for PreprocessDdTraceConfig {
    fn generate_config() -> toml::Value {
        toml::Value::Table(Default::default())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "preprocess_dd_trace")]
impl TransformConfig for PreprocessDdTraceConfig {
    async fn build(&self, _context: &TransformContext) -> vector::Result<Transform> {
        Ok(Transform::function(PreprocessDdTrace))
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
struct PreprocessDdTrace;

impl FunctionTransform for PreprocessDdTrace {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        output.push(event);
    }
}
