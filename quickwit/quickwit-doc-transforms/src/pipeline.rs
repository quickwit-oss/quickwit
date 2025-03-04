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

use serde::Deserialize;
use vrl::datadog_filter::Matcher;

use crate::error::PipelineError;
use crate::filter::build_vrl_matcher;
use crate::path_access::parse_path;
use crate::processed_log::ProcessedLog;
use crate::transformers::{build_grok_parser_step, RemapStep};

/// Trait for steps in the pipeline. Each step mutates a `ProcessedLog`.
pub trait PipelineStep: Send + Sync + std::fmt::Debug {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError>;
}

#[derive(Debug)]
pub struct Pipeline {
    steps: Vec<Box<dyn PipelineStep>>,
}

impl Pipeline {
    #[cfg(test)]
    pub fn from_steps(steps: Vec<Box<dyn PipelineStep>>) -> Self {
        Self { steps }
    }

    pub fn process_logs(
        &self,
        logs: Vec<ProcessedLog>,
    ) -> Result<Vec<ProcessedLog>, PipelineError> {
        let mut logs = logs;
        for log in &mut logs {
            self.apply(log)?;
        }
        Ok(logs)
    }

    /// Apply the entire pipeline to `value` in place.
    pub fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for step in &self.steps {
            step.apply(value)?;
        }
        Ok(())
    }

    /// Build a Pipeline from a list of typed `StepConfig`.
    pub fn from_configs(configs: &[PipelineStepConfig]) -> Result<Self, PipelineError> {
        let mut steps = Vec::new();
        for cfg in configs {
            let step = build_step(cfg)?;
            steps.push(step);
        }
        Ok(Self { steps })
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineStepConfig {
    /// A nested pipeline step.
    NestedPipeline {
        #[serde(default)]
        filter: String,
        steps: Vec<PipelineStepConfig>,
    },
    /// Grok transform: parse a field with a pattern, optionally add a prefix to captured fields.
    Grok {
        #[serde(default)]
        filter: String,
        patterns: Vec<String>,
    },
    /// Remap one nested path to another (optionally preserve the original).
    Remap {
        #[serde(default)]
        filter: String,
        from: String,
        to: String,
        #[serde(default)]
        preserve_original: bool,
    },
}

/// Convert a `StepConfig` into a boxed pipeline step implementation.
pub fn build_step(cfg: &PipelineStepConfig) -> Result<Box<dyn PipelineStep>, PipelineError> {
    match cfg {
        PipelineStepConfig::NestedPipeline { filter, steps } => {
            let filter = build_vrl_matcher(filter)?;

            let sub_pipeline = Pipeline::from_configs(steps)?;
            Ok(Box::new(NestedPipelineStep {
                filter,
                pipeline: sub_pipeline,
            }))
        }
        PipelineStepConfig::Grok {
            filter,
            patterns: pattern,
        } => {
            let filter = build_vrl_matcher(filter)?;
            let grok = build_grok_parser_step(pattern, filter)?;

            Ok(Box::new(grok))
        }
        PipelineStepConfig::Remap {
            filter,
            from,
            to,
            preserve_original,
        } => {
            let filter = build_vrl_matcher(filter)?;
            let from_path = parse_path(from);
            let to_path = parse_path(to);
            Ok(Box::new(RemapStep {
                filter,
                from_path,
                to_path,
                preserve_original: *preserve_original,
            }))
        }
    }
}

/// A nested pipeline step: if filter matches, apply sub-steps.
#[derive(Debug)]
pub struct NestedPipelineStep {
    filter: Box<dyn Matcher<ProcessedLog>>,
    pipeline: Pipeline,
}

impl PipelineStep for NestedPipelineStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        if self.filter.run(value) {
            self.pipeline.apply(value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TagKV<'a> {
    pub key: &'a str,
    pub value: &'a str,
}
#[allow(dead_code)]
impl TagKV<'_> {
    pub fn parse_tag(tag: &str) -> TagKV {
        let mut parts = tag.splitn(2, ':');
        let key = parts.next().unwrap();
        let value = parts.next().unwrap_or("");
        TagKV { key, value }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::processed_log::tests::make_datadog_log_msg;

    #[test]
    fn test_nested_pipeline() {
        let configs = vec![PipelineStepConfig::NestedPipeline {
            filter: "".into(),
            steps: vec![PipelineStepConfig::Remap {
                filter: "".into(),
                from: "a".to_string(),
                to: "b.c".to_string(),
                preserve_original: false,
            }],
        }];

        let pipeline = Pipeline::from_configs(&configs).unwrap();
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom = [("a".to_string(), json!(42))].into_iter().collect();
        pipeline.apply(&mut log).unwrap();
        assert_eq!(log.custom["b"]["c"], 42);
        assert!(log.custom.get("a").is_none());
    }

    fn get_pipeline() -> Pipeline {
        let step_cfg = PipelineStepConfig::Grok {
            filter: "service:appgate_driver_logs OR service:appgate_app_logs".into(),
            patterns: vec![
                r#"\[%{date("yyyy-MM-dd'T'HH:mm:ss.SSSZ"):date}\] %{data:level} : %{data:message}"#
                    .into(),
            ],
        };
        Pipeline::from_configs(&[step_cfg.clone()]).unwrap()
    }

    #[test]
    fn test_process_logs2() {
        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.service = "appgate_driver_logs".into();
        agent_log.message = "[2025-02-10T19:26:46.419Z] Info : Resolved excluded host: \
                             ec2-54-87-69-251.compute-1.amazonaws.com / 54.87.69.25"
            .into();
        let logs = vec![agent_log];

        let pipeline = get_pipeline();
        let logs = pipeline.process_logs(logs).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].custom.len(), 3);
        assert_eq!(logs[0].custom["level"], "Info");
        assert_eq!(logs[0].custom["date"], 1739215606419u64);

        // Test with service appgate_app_logs
        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.service = "appgate_app_logs".into();
        agent_log.message = "[2025-02-10T19:26:46.419Z] Info : Resolved excluded host: \
                             ec2-54-87-69-251.compute-1.amazonaws.com / 54.87.69.25"
            .into();
        let logs = vec![agent_log];

        let pipeline = get_pipeline();
        let logs = pipeline.process_logs(logs).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].custom.len(), 3);
        assert_eq!(logs[0].custom["level"], "Info");
        assert_eq!(logs[0].custom["date"], 1739215606419u64);
    }
}
