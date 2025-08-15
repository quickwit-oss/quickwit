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

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use crate::ProcessedLog;
use crate::pipeline::*;

/// See README.md for more information on how to update the integration processor.
pub fn get_integrations_processor() -> &'static dyn PipelineStep {
    static INTEGRATION_PROCESSOR: OnceLock<Box<dyn PipelineStep>> = OnceLock::new();
    INTEGRATION_PROCESSOR
        .get_or_init(|| {
            let config = include_str!("../../integrations_map.json");
            let pipeline_per_source: BTreeMap<String, PipelineStepConfig> =
                serde_json::from_str(config).expect("Failed to parse integrations_map.json");
            let integration_by_source = pipeline_per_source
                .into_iter()
                .map(|(source, cfg)| {
                    let step = build_step(&cfg).expect("Failed to build pipeline step");
                    (source, step)
                })
                .collect();
            Box::new(IntegrationProcessor {
                integration_by_source,
            })
        })
        .as_ref() // ← turns &Box<dyn PipelineStep> into &dyn PipelineStep
}

#[derive(Debug)]
/// The IntegrationProcessor is a pipeline step that applies a pipeline for predefined sources.
pub struct IntegrationProcessor {
    pub integration_by_source: HashMap<String, Box<dyn PipelineStep>>,
}

impl PipelineStep for IntegrationProcessor {
    fn apply(&self, processed_log: &mut ProcessedLog) -> crate::Result<()> {
        let Some(source) = processed_log.source.as_ref() else {
            // If the source is not set, we cannot apply any integration processing.
            return Ok(());
        };
        let Some(integration_pipeline) = self.integration_by_source.get(source) else {
            return Ok(());
        };
        integration_pipeline.apply(processed_log)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use crate::PipelineStepConfig;
    use crate::transformers::grok_rules::build_grok_rules;

    #[test]
    fn test_integrations_grok_parser() {
        // Enable a logger to print warnings
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
        // We load the integrations file and check that the grok parser rules parse, if not print
        // an error
        let config = include_str!("../../integrations_map.json");
        let pipeline_per_source: BTreeMap<String, PipelineStepConfig> =
            serde_json::from_str(config).expect("Failed to parse integrations_map.json");
        for (source, config) in pipeline_per_source {
            if source != "mongodb" {
                // Skip the datadog source, it has no grok rules
                continue;
            }
            // top level is expected to be a NestedPipeline
            if let PipelineStepConfig::NestedPipeline { processors, .. } = config {
                for processor in processors {
                    if let PipelineStepConfig::Grok { grok, .. } = processor {
                        build_grok_rules(&grok.support_rules, &grok.match_rules).unwrap();
                    }
                }
            } else {
                panic!("Expected NestedPipeline for source '{source}'");
            }
        }
    }
}
