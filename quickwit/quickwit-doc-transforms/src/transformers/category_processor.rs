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

use serde_json::Value;
use vrl::datadog_filter::Matcher;

use crate::error::PipelineError;
use crate::path_access::*;
use crate::pipeline::*;
use crate::ProcessedLog;

/// A step that copies a value from `custom` to a new location, optionally removing the original.
#[derive(Debug)]
pub struct CategoryProcessorStep {
    pub mappings: Vec<CategoryProcessorMapping>,
    pub to_path: ParsedPath,
}

#[derive(Debug)]
pub struct CategoryProcessorMapping {
    /// The filter to be checked against the log
    pub filter: Box<dyn Matcher<ProcessedLog>>,
    /// The value to be used if the filter matches
    pub name: String,
}

impl PipelineStep for CategoryProcessorStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        for mapping in &self.mappings {
            if mapping.filter.run(value) {
                set_value_at_path_on_map(
                    &mut value.custom,
                    &self.to_path.segments,
                    Value::String(mapping.name.clone()),
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::processed_log::tests::make_datadog_log_msg;

    #[test]
    fn test_category_processor_deser() {
        let yaml = r#"
type: category-processor
id: "123456"
name: ""
enabled: true
target: http.status_category
categories:
  - name: "OK"
    filter:
      query: "@http.status_code:[200 TO 299]"
  - name: "notice"
    filter:
      query: "@http.status_code:[300 TO 399]"
"#;

        let config: PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("Deserialization failed");
        let step = build_step(&config).unwrap();

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log
            .custom
            .insert("http".to_string(), json!({"status_code": 204}));
        step.apply(&mut agent_log).unwrap();
        assert_eq!(agent_log.custom["http"]["status_category"], json!("OK"));
        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log
            .custom
            .insert("http".to_string(), json!({"status_code": 904}));
        step.apply(&mut agent_log).unwrap();
        assert_eq!(
            agent_log.custom["http"]["status_category"],
            serde_json::Value::Null
        );
    }
}
