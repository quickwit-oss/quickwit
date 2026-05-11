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

use crate::ProcessedLog;
use crate::error::PipelineError;
use crate::path_access::*;
use crate::percolate::Percolator;
use crate::pipeline::*;

#[derive(Debug)]
pub struct CategoryProcessorStep {
    pub percolator: Percolator,
    pub category_names: Vec<String>,
    pub to_path: ParsedPath,
}

impl PipelineStep for CategoryProcessorStep {
    fn apply(&self, value: &mut ProcessedLog) -> Result<(), PipelineError> {
        let first_match =
            self.percolator
                .matcher(value)
                .next()
                .map_err(|error| PipelineError::Other {
                    error: error.to_string(),
                })?;

        if let Some(idx) = first_match {
            set_value_at_path_on_map(
                &mut value.custom,
                &self.to_path.segments,
                Value::String(self.category_names[idx].clone()),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::path_access::parse_path;
    use crate::percolate::default_percolator_config;
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

    #[test]
    fn test_category_processor_complex_wildcard_or() {
        // Regression test: complex multi-wildcard OR queries caused a
        // "Bytecode and closure evaluation mismatch" panic in event-percolation.
        let yaml = r#"
type: category-processor
name: activity_id
enabled: true
categories:
  - filter:
      query: "@ocsf.metadata.event_code:*unknown*"
    name: "0"
  - filter:
      query: "@ocsf.metadata.event_code:(*create* OR *generate*)"
    name: "1"
  - filter:
      query: "@ocsf.metadata.event_code:(*read* OR *get* OR *list*)"
    name: "2"
  - filter:
      query: "@ocsf.metadata.event_code:(*update* OR *patch* OR *attach* OR *detach* OR *enable* OR *disable* OR *remove* OR *insert* OR *add* OR *set*)"
    name: "3"
  - filter:
      query: "@ocsf.metadata.event_code:*delete*"
    name: "4"
  - filter:
      query: "@ocsf.metadata.event_code:*"
    name: "99"
target: ocsf.activity_id
"#;
        let config: crate::pipeline::PipelineStepConfig =
            serde_yaml::from_str(yaml).expect("YAML deserialization failed");
        let step = crate::pipeline::build_step(&config).expect("build_step failed");

        // Same config via JSON — should behave identically
        let json = r#"{
            "type": "category-processor",
            "name": "activity_id",
            "enabled": true,
            "categories": [
                {"filter": {"query": "@ocsf.metadata.event_code:*unknown*"}, "name": "0"},
                {"filter": {"query": "@ocsf.metadata.event_code:(*create* OR *generate*)"}, "name": "1"},
                {"filter": {"query": "@ocsf.metadata.event_code:(*read* OR *get* OR *list*)"}, "name": "2"},
                {"filter": {"query": "@ocsf.metadata.event_code:(*update* OR *patch* OR *attach* OR *detach* OR *enable* OR *disable* OR *remove* OR *insert* OR *add* OR *set*)"}, "name": "3"},
                {"filter": {"query": "@ocsf.metadata.event_code:*delete*"}, "name": "4"},
                {"filter": {"query": "@ocsf.metadata.event_code:*"}, "name": "99"}
            ],
            "target": "ocsf.activity_id"
        }"#;
        let config_json: crate::pipeline::PipelineStepConfig =
            serde_json::from_str(json).expect("JSON deserialization failed");
        let step_json =
            crate::pipeline::build_step(&config_json).expect("build_step failed (json)");

        // "create_bucket" should match category "1" (*create*)
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom.insert(
            "ocsf".to_string(),
            json!({"metadata": {"event_code": "create_bucket"}}),
        );
        step.apply(&mut log).unwrap();
        assert_eq!(
            log.custom["ocsf"]["activity_id"],
            json!("1"),
            "yaml: create_bucket"
        );

        let mut log_json = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log_json.custom.insert(
            "ocsf".to_string(),
            json!({"metadata": {"event_code": "create_bucket"}}),
        );
        step_json.apply(&mut log_json).unwrap();
        assert_eq!(
            log_json.custom["ocsf"]["activity_id"],
            json!("1"),
            "json: create_bucket"
        );

        // "update_policy" should match category "3" (*update*)
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom.insert(
            "ocsf".to_string(),
            json!({"metadata": {"event_code": "update_policy"}}),
        );
        step.apply(&mut log).unwrap();
        assert_eq!(
            log.custom["ocsf"]["activity_id"],
            json!("3"),
            "yaml: update_policy"
        );

        // "foobar" should fall through to the wildcard catch-all "99"
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom.insert(
            "ocsf".to_string(),
            json!({"metadata": {"event_code": "foobar"}}),
        );
        step.apply(&mut log).unwrap();
        assert_eq!(
            log.custom["ocsf"]["activity_id"],
            json!("99"),
            "yaml: foobar catch-all"
        );

        // "google.longrunning.operations.getoperation" — real value from integration test
        // Should match category "2" (*get*)
        let mut log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log.custom.insert(
            "ocsf".to_string(),
            json!({"metadata": {"event_code": "google.longrunning.operations.getoperation"}}),
        );
        step.apply(&mut log).unwrap();
        assert_eq!(
            log.custom["ocsf"]["activity_id"],
            json!("2"),
            "yaml: getoperation"
        );

        let mut log_json = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        log_json.custom.insert(
            "ocsf".to_string(),
            json!({"metadata": {"event_code": "google.longrunning.operations.getoperation"}}),
        );
        step_json.apply(&mut log_json).unwrap();
        assert_eq!(
            log_json.custom["ocsf"]["activity_id"],
            json!("2"),
            "json: getoperation"
        );
    }

    #[test]
    fn test_first_matching_category_wins() {
        let queries = vec!["@level:error AND @service:api", "@level:error"];
        let percolator = Percolator::new(queries, &default_percolator_config()).unwrap();
        let step = CategoryProcessorStep {
            percolator,
            category_names: vec!["critical".to_string(), "warning".to_string()],
            to_path: parse_path("category"),
        };

        let mut agent_log = ProcessedLog::from_datadog_log_msg(make_datadog_log_msg());
        agent_log.custom.insert("level".to_string(), json!("error"));
        agent_log.custom.insert("service".to_string(), json!("api"));

        step.apply(&mut agent_log).unwrap();
        assert_eq!(agent_log.custom["category"], json!("critical"));
    }
}
