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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::warn;
use vrl::datadog_filter::Matcher;

use crate::error::PipelineError;
use crate::filter::build_vrl_matcher;
use crate::path_access::parse_path;
use crate::processed_log::ProcessedLog;
use crate::transformers::grok_auto_step::build_grok_parser_auto_step;
use crate::transformers::grok_rules::LogsProcessingGrokRules;
use crate::transformers::{
    AttributeRemapStep, CategoryProcessorMapping, CategoryProcessorStep, CompiledTemplateString,
    CoreStringAttr, CoreStringAttrRemapStep, DateRemapStep, FilteredStep, StatusRemapStep,
    StringBuilderStep, build_grok_parser_step,
};

/// Trait for steps in the pipeline. Each step mutates a `ProcessedLog`.
pub trait PipelineStep: Send + Sync + std::fmt::Debug {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError>;
}

#[derive(Debug)]
pub struct Pipeline {
    steps: Vec<Box<dyn PipelineStep>>,
}

impl PipelineStep for Pipeline {
    /// Apply the entire pipeline to `value` in place.
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for step in &self.steps {
            step.apply(value)?;
        }
        Ok(())
    }
}

impl Pipeline {
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

    /// Build a Pipeline from a list of typed `StepConfig`.
    pub fn from_step_configs(configs: &[PipelineStepConfig]) -> Result<Self, PipelineError> {
        let mut steps = Vec::new();
        for cfg in configs {
            let step = build_step(cfg);
            match step {
                Ok(step) => steps.push(step),
                Err(e) => {
                    warn!("failed to build pipeline processor: {:?}", e);
                }
            }
        }
        Ok(Self { steps })
    }

    /// Build a Pipeline from a `PipelineConfig`.
    pub fn try_from_pipeline_config(config: &PipelineConfig) -> Result<Self, PipelineError> {
        Self::from_step_configs(&config.0[..])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq, Default)]
pub struct PipelineConfig(Arc<Vec<PipelineStepConfig>>);

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineStepConfig {
    #[serde(rename = "url-parser")]
    UrlParser {},
    #[serde(rename = "user-agent-parser")]
    UserAgentParser {},
    #[serde(rename = "geo-ip-parser")]
    GeoIpParser {},
    #[serde(rename = "category-processor")]
    CategoryProcessor {
        #[serde(flatten)]
        common: CommonConfig,
        categories: Vec<CategoryConfig>,
        target: String,
    },
    #[serde(rename = "arithmetic-processor")]
    ArithmeticProcessor {},
    #[serde(rename = "lookup-processor")]
    LookupProcessor {},
    /// A nested pipeline step.
    #[serde(rename = "pipeline")]
    NestedPipeline {
        #[serde(flatten)]
        common: CommonConfig,
        #[serde(rename = "processors")]
        processors: Vec<PipelineStepConfig>,
        /// The description of the pipeline.
        #[serde(default)]
        description: String,
        /// A list of tags associated with the pipeline.
        ///
        /// The docs are unclear on the use of this field. Related issue: <https://github.com/DataDog/documentation/issues/28172>
        #[serde(default)]
        tags: Vec<String>,
    },
    /// Grok transforms based on the value of the `source` field from a list of library grok rules.
    #[serde(rename = "auto-grok")]
    AutoGrok {
        #[serde(flatten)]
        common: CommonConfig,
    },
    /// Grok transform based on passed match_rules and support_rules.
    #[serde(rename = "grok-parser")]
    Grok {
        #[serde(flatten)]
        common: CommonConfig,
        grok: LogsProcessingGrokRules,
    },
    /// Remap one nested path to another (optionally preserve the original).
    /// Serde rename to `attribute-remapper` to keep compatible with logs pipelines
    #[serde(rename = "attribute-remapper")]
    /// Example (read path):
    /// {
    ///     "type": "attribute-remapper",
    ///     "id": "1234",
    ///     "name": "Map @dd.env to env tag",
    ///     "enabled": true,
    ///     "meta": {
    ///         "last_update": {
    ///             "timestamp": 1700645013725,
    ///             "user_name": "User123",
    ///             "user_email": "user123@gmail.com"
    ///         },
    ///         "tags": []
    ///     },
    ///     "sources": [
    ///         "dd.env"
    ///     ],
    ///     "source_type": "attribute",
    ///     "target": "env",
    ///     "target_type": "tag",
    ///     "preserve_source": true,
    ///     "override_on_conflict": false
    /// }
    AttributeRemapper {
        #[serde(flatten)]
        common: CommonConfig,
        sources: Vec<String>,
        target: String,
        #[serde(default)]
        #[serde(alias = "preserveSource")]
        preserve_source: bool,
        /// Override or not the target element if already set,
        #[serde(alias = "overrideOnConflict")]
        override_on_conflict: bool,
        #[serde(default)]
        #[serde(alias = "sourceType")]
        source_type: AttrRemapperTargetType,
        #[serde(default)]
        #[serde(alias = "targetType")]
        target_type: AttrRemapperTargetType,
        /// If the target_type of the remapper is attribute, try to cast the value to a new
        /// specific type. Not used for tags.
        #[serde(default)]
        #[serde(alias = "targetFormat")]
        target_format: AttrRemapperTargetFormat,
    },
    /// Status remapper: remap a status field to a new location.
    /// Serde rename to `status-remapper` to keep compatible with logs pipelines
    #[serde(rename = "status-remapper")]
    StatusRemapper {
        #[serde(flatten)]
        common: CommonConfig,
        sources: Vec<String>,
    },
    /// Date remapper
    /// Serde rename to `status-remapper` to keep compatible with logs pipelines
    #[serde(rename = "date-remapper")]
    DateRemapper {
        #[serde(flatten)]
        common: CommonConfig,
        sources: Vec<String>,
    },
    /// A step that constructs a string from a template and writes it to a field.
    #[serde(rename = "string-builder-processor")]
    StringBuilder {
        #[serde(flatten)]
        common: CommonConfig,
        template: String,
        target: String,
        #[serde(default)]
        #[serde(alias = "isReplaceMissing")]
        is_replace_missing: bool,
    },
    /// Copies a string value from `custom` to message field.
    /// Only string values are supported.
    #[serde(rename = "message-remapper")]
    MessageRemapper {
        #[serde(flatten)]
        common: CommonConfig,
        sources: Vec<String>,
    },
    /// Copies a string value from `custom` to the traceid field.
    /// Only string values are supported.
    #[serde(rename = "trace-id-remapper")]
    TraceIdRemapper {
        #[serde(flatten)]
        common: CommonConfig,
        sources: Vec<String>,
    },
    /// Copies a string value from `custom` to the traceid field.
    /// Only string values are supported.
    #[serde(rename = "span-id-remapper")]
    SpanIdRemapper {
        #[serde(flatten)]
        common: CommonConfig,
        sources: Vec<String>,
    },
    /// Copies a string value from `custom` to the service field.
    /// Only string values are supported.
    #[serde(rename = "service-remapper")]
    ServiceRemapper {
        #[serde(flatten)]
        common: CommonConfig,
        sources: Vec<String>,
    },
}

#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum AttrRemapperTargetFormat {
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "string")]
    Str,
    #[serde(rename = "integer")]
    Integer,
    #[serde(rename = "double")]
    Double,
}

#[derive(Default, Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum AttrRemapperTargetType {
    /// A tag target.
    #[serde(rename = "tag")]
    Tag,
    /// An attribute target.
    #[default]
    #[serde(rename = "attribute")]
    Attribute,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
/// Common fields for all pipeline steps.
/// They are flattened into the step config.
pub struct CommonConfig {
    /// The id of the pipeline step. Unclear if this is used.
    #[serde(default)]
    pub id: String,
    /// The name of the pipeline step.
    /// name is not optional for the pipline step type in the logs processing API.
    #[serde(default)]
    pub name: String,
    /// Whether the step is enabled.
    /// The logs processing API uses `is_enabled` but the output uses `enabled`.
    #[serde(alias = "is_enabled")]
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    /// The filter to check before applying the step.
    pub filter: Filter,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct CategoryConfig {
    pub filter: Filter,
    pub name: String,
}

impl Default for CommonConfig {
    fn default() -> Self {
        Self {
            id: Default::default(),
            name: Default::default(),
            // Note: On the logs processing API, the default is `false` and so is the serde
            // default. This is just for convenience in the tests.
            enabled: true,
            filter: Default::default(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub struct Filter {
    #[serde(default)]
    pub query: String,
}

impl From<&str> for Filter {
    fn from(query: &str) -> Self {
        Self {
            query: query.to_string(),
        }
    }
}

fn build_vrl_matcher_from_config(
    common: &CommonConfig,
) -> Result<Box<dyn Matcher<ProcessedLog>>, PipelineError> {
    if !common.enabled {
        return Ok(Box::new(false));
    }
    build_vrl_matcher(&common.filter.query)
}

/// Convert a `StepConfig` into a boxed pipeline step implementation.
pub fn build_step(cfg: &PipelineStepConfig) -> Result<Box<dyn PipelineStep>, PipelineError> {
    match cfg {
        PipelineStepConfig::NestedPipeline {
            common,
            processors: steps,
            ..
        } => {
            let filter = build_vrl_matcher_from_config(common)?;
            let sub_pipeline = Pipeline::from_step_configs(steps)?;
            Ok(Box::new(FilteredStep::new(filter, sub_pipeline)))
        }
        PipelineStepConfig::Grok { common, grok } => {
            let filter = build_vrl_matcher_from_config(common)?;
            let grok = build_grok_parser_step(grok)?;
            Ok(Box::new(FilteredStep::new(filter, grok)))
        }
        PipelineStepConfig::AutoGrok { common: _ } => {
            // TODO: respect common.enabled
            let auto_grok = build_grok_parser_auto_step()?;
            Ok(Box::new(auto_grok))
        }
        PipelineStepConfig::AttributeRemapper {
            common,
            sources,
            target,
            preserve_source: preserve_original,
            override_on_conflict,
            source_type,
            target_type,
            target_format,
        } => {
            let filter = build_vrl_matcher_from_config(common)?;
            let sources = sources.iter().map(AsRef::as_ref).map(parse_path).collect();
            let to_path = parse_path(target);
            let remap = AttributeRemapStep {
                sources,
                to_path,
                preserve_original: *preserve_original,
                source_type: *source_type,
                target_type: *target_type,
                override_if_exists: *override_on_conflict,
                target_format: *target_format,
            };
            Ok(Box::new(FilteredStep::new(filter, remap)))
        }
        PipelineStepConfig::StatusRemapper { common, sources } => {
            let filter = build_vrl_matcher_from_config(common)?;
            let sources = sources.iter().map(AsRef::as_ref).map(parse_path).collect();
            let remap = StatusRemapStep { sources };
            Ok(Box::new(FilteredStep::new(filter, remap)))
        }
        PipelineStepConfig::DateRemapper { common, sources } => {
            let filter = build_vrl_matcher_from_config(common)?;
            let sources = sources.iter().map(AsRef::as_ref).map(parse_path).collect();
            let remap = DateRemapStep { sources };
            Ok(Box::new(FilteredStep::new(filter, remap)))
        }
        PipelineStepConfig::StringBuilder {
            common,
            template,
            target,
            is_replace_missing,
        } => {
            let filter = build_vrl_matcher_from_config(common)?;
            let step = StringBuilderStep {
                template: CompiledTemplateString::compile(template),
                to_path: parse_path(target),
                is_replace_missing: *is_replace_missing,
            };
            Ok(Box::new(FilteredStep::new(filter, step)))
        }
        PipelineStepConfig::CategoryProcessor {
            common,
            categories,
            target,
        } => {
            let filter = build_vrl_matcher_from_config(common)?;
            let step = CategoryProcessorStep {
                mappings: categories
                    .iter()
                    .map(|cat| {
                        let filter = build_vrl_matcher(&cat.filter.query)?;
                        Ok(CategoryProcessorMapping {
                            filter,
                            name: cat.name.clone(),
                        })
                    })
                    .collect::<crate::Result<_>>()?,
                to_path: parse_path(target),
            };
            Ok(Box::new(FilteredStep::new(filter, step)))
        }
        PipelineStepConfig::MessageRemapper { common, sources } => {
            string_core_attr_remapper(common, sources, CoreStringAttr::Message)
        }
        PipelineStepConfig::TraceIdRemapper { common, sources } => {
            string_core_attr_remapper(common, sources, CoreStringAttr::TraceId)
        }
        PipelineStepConfig::SpanIdRemapper { common, sources } => {
            string_core_attr_remapper(common, sources, CoreStringAttr::SpanId)
        }
        PipelineStepConfig::ServiceRemapper { common, sources } => {
            string_core_attr_remapper(common, sources, CoreStringAttr::Service)
        }
        _ => {
            let val = serde_json::to_value(cfg).map_err(|e| PipelineError::Other {
                error: format!("Failed to serialize step config: {e}"),
            })?;
            let step_type = val.get("type").and_then(|t| t.as_str()).unwrap();
            Err(PipelineError::UnsupportedType {
                typ: step_type.to_string(),
            })
        }
    }
}

fn string_core_attr_remapper(
    common: &CommonConfig,
    sources: &[String],
    core_attr: CoreStringAttr,
) -> Result<Box<dyn PipelineStep>, PipelineError> {
    let sources = sources.iter().map(AsRef::as_ref).map(parse_path).collect();
    let filter = build_vrl_matcher_from_config(common)?;
    let step = CoreStringAttrRemapStep { sources, core_attr };
    Ok(Box::new(FilteredStep::new(filter, step)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;
    use crate::processed_log::tests::make_datadog_log_msg;
    use crate::transformers::grok_rules::Rule;

    #[test]
    fn test_nested_pipeline() {
        let configs = vec![PipelineStepConfig::NestedPipeline {
            common: Default::default(),
            description: "Nested pipeline".to_string(),
            tags: vec![],
            processors: vec![PipelineStepConfig::AttributeRemapper {
                common: Default::default(),
                sources: vec!["a".to_string()],
                target: "b.c".to_string(),
                preserve_source: false,
                source_type: AttrRemapperTargetType::Attribute,
                target_type: AttrRemapperTargetType::Attribute,
                override_on_conflict: false,
                target_format: AttrRemapperTargetFormat::Auto,
            }],
        }];

        let pipeline = Pipeline::from_step_configs(&configs).unwrap();
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom = [("a".to_string(), json!(42))].into_iter().collect();
        pipeline.apply(&mut log).unwrap();
        assert_eq!(log.custom["b"]["c"], 42);
        assert!(log.custom.get("a").is_none());
    }

    fn get_grok_pipeline() -> Pipeline {
        let step_cfg = PipelineStepConfig::Grok {
            common: CommonConfig {
                filter: "service:appgate_driver_logs OR service:appgate_app_logs".into(),
                ..Default::default()
            },
            grok: LogsProcessingGrokRules {
                support_rules: vec![],
                match_rules: vec![Rule{ name: "".to_string(), rule:r#"\[%{date("yyyy-MM-dd'T'HH:mm:ss.SSSZ"):date}\] %{data:level} : %{data:message}"#.to_string()  }] ,
            },
        };
        Pipeline::from_step_configs(&[step_cfg]).unwrap()
    }

    #[test]
    fn test_process_logs2() {
        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.service = "appgate_driver_logs".into();
        agent_log.message = "[2025-02-10T19:26:46.419Z] Info : Resolved excluded host: \
                             ec2-54-87-69-251.compute-1.amazonaws.com / 54.87.69.25"
            .into();
        let logs = vec![agent_log];

        let pipeline = get_grok_pipeline();
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

        let pipeline = get_grok_pipeline();
        let logs = pipeline.process_logs(logs).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].custom.len(), 3);
        assert_eq!(logs[0].custom["level"], "Info");
        assert_eq!(logs[0].custom["date"], 1739215606419u64);
    }

    #[test]
    fn test_deserialize_remap_yaml() {
        let yaml = r#"
type: attribute-remapper
id: "3EkBeJhPSMqAprV3LOJv5Q"
name: "Map @dd.env to env tag"
enabled: true
meta:
  last_update:
    timestamp: 1700645013725
    user_name: "User123"
    user_email: "user123@gmail.com"
  tags: []
sources:
  - "dd.env"
sourceType: "attribute"
target: "env"
targetType: "tag"
preserveSource: true
overrideOnConflict: false
"#;

        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");

        // The expected mapping is:
        // - "sources" -> `from` (taking the first element)
        // - "target" -> `to`
        // - "preserveSource" -> `preserve_original`
        match config {
            PipelineStepConfig::AttributeRemapper {
                common,
                sources: from,
                target: to,
                preserve_source: preserve_original,
                override_on_conflict: _,
                source_type: _,
                target_type: _,
                target_format: _,
            } => {
                // The filter is defaulted to an empty string.
                assert_eq!(common.filter.query, "");
                assert_eq!(from, vec!["dd.env"]);
                assert_eq!(to, "env");
                assert!(preserve_original);
            }
            _ => panic!("Expected PipelineStepConfig::Remap variant"),
        }
    }

    #[test]
    fn test_date_remapper_deser() {
        let yaml = r#"
type: date-remapper
id: "123456"
name: "Define `timestamp` as the official date of the log"
enabled: true
sources:
  - "timestamp"
"#;

        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");
        let step = build_step(&config).unwrap();

        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom
            .insert("timestamp".to_string(), json!("2021-01-01T00:00:00Z"));
        step.apply(&mut log).unwrap();
        assert_eq!(
            log.timestamp,
            OffsetDateTime::from_unix_timestamp(1609459200).unwrap()
        );
    }

    #[test]
    fn test_status_remapper_deser() {
        // The format of logs processing
        let yaml = r#"
type: status-remapper
id: "123456"
name: "Define `http.status_category`, `level` as the official status of the log"
enabled: true
sources:
  - "http.status_category"
  - "level"
"#;

        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");
        let step = build_step(&config).unwrap();

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log
            .custom
            .insert("http".to_string(), json!({"status_category": "warn"}));
        step.apply(&mut agent_log).unwrap();
        assert_eq!(agent_log.status, "warn");

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.custom.insert("level".to_string(), "warn".into());
        step.apply(&mut agent_log).unwrap();
        assert_eq!(agent_log.status, "warn");

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        step.apply(&mut agent_log).unwrap();
        assert_eq!(agent_log.status, "info"); // default
    }

    #[test]
    fn test_grok_parser_deser() {
        // The format of logs processing
        let yaml = r#"
type: grok-parser
id: "123456"
name: "grok-parser test"
enabled: true
grok:
  matchRules: "default_format %{_pid}:%{_role} %{_date} %{_severity} %{data:message}"
  supportRules: "_date (%{date(\"dd MMM HH:mm:ss.SSS\"):date}|%{date(\"dd MMM yyyy HH:mm:ss.SSS\"):date})\n_pid %{integer:pid}\n_severity %{notSpace:severity}\n_role %{word:role}\n"
"#;

        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");
        let step = build_step(&config).unwrap();

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.message = "2115:M 08 Jan 17:55:41.572 # WARNING: The TCP backlog setting of 511 \
                             cannot be enforced because /proc/sys/net/core/somaxconn is set to \
                             the lower value of 128."
            .into();

        step.apply(&mut agent_log).unwrap();
        assert_eq!(agent_log.custom.len(), 5);
        assert_eq!(agent_log.custom["pid"], 2115);
    }

    #[test]
    fn test_message_remapper_deser() {
        // The format of logs processing
        let yaml = r#"
type: message-remapper
id: "123456"
name: ""
enabled: true
sources:
  - "http.status_category"
  - "msg"
"#;

        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");
        let step = build_step(&config).unwrap();

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.custom.insert("msg".to_string(), "blub".into());
        step.apply(&mut agent_log).unwrap();
        assert_eq!(agent_log.message, "blub");
    }
}
