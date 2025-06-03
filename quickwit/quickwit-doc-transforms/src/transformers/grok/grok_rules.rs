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

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tracing::warn;
use vrl::datadog_grok::parse_grok_rules::{self, GrokRule};
use vrl::value::KeyString;

use crate::error::PipelineError;

static OP_GROK_RULES: OnceLock<SourceToGrokPatterns> = OnceLock::new();
pub(super) type SourceToGrokPatterns = BTreeMap<String, Vec<GrokRule>>;

pub fn get_grok_rules_by_source() -> &'static SourceToGrokPatterns {
    OP_GROK_RULES.get_or_init(|| {
        let json_str = include_str!("rules.json");
        let mut op_grok_rules: Vec<OPGrokRules> =
            serde_json::from_str(json_str).expect("Failed to parse JSON");
        for op_grok_rules in &mut op_grok_rules {
            op_grok_rules.normalize();
        }
        build_grok_rules_with_source(&op_grok_rules)
    })
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OPGrokRules {
    pub source: String,
    pub support_rules: Vec<Rule>,
    pub match_rules: Vec<Rule>,
    pub samples: Vec<Sample>,
}
impl OPGrokRules {
    pub fn normalize(&mut self) {
        // Normalize the rules by removing leading and trailing whitespace.
        for rule in &mut self.support_rules {
            rule.name = rule.name.trim().to_string();
            rule.rule = rule.rule.trim().to_string();
        }
        for rule in &mut self.match_rules {
            rule.name = rule.name.trim().to_string();
            rule.rule = rule.rule.trim().to_string();
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct Rule {
    pub name: String,
    pub rule: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sample {
    pub sample: String,
    pub result: Value,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct LogsProcessingGrokRules {
    // Use a custom deserializer to transform the multiline string into Vec<Rule>
    #[serde(alias = "supportRules")]
    #[serde(deserialize_with = "parse_rules_from_str")]
    #[serde(default)]
    pub support_rules: Vec<Rule>,
    #[serde(alias = "matchRules")]
    #[serde(deserialize_with = "parse_rules_from_str")]
    pub match_rules: Vec<Rule>,
}

/// Custom deserializer for fields that are multiline rules strings.
pub fn parse_rules_from_str<'de, D>(deserializer: D) -> Result<Vec<Rule>, D::Error>
where D: Deserializer<'de> {
    let s: String = Deserialize::deserialize(deserializer).unwrap_or("".to_string());
    Ok(parse_rules(&s))
}

/// This parses the rules from the logs processing format, which is
/// {RULE_NAME} {RULE}\n
pub fn parse_rules(rules_str: &str) -> Vec<Rule> {
    rules_str
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                None
            } else {
                // Split each line at the first space.
                trimmed.split_once(' ').map(|(name, rule)| Rule {
                    name: name.trim().to_string(),
                    rule: rule.trim().to_string(),
                })
            }
        })
        .collect()
}

pub(crate) fn build_grok_rules(
    support_rules: &[Rule],
    match_rules: &[Rule],
) -> crate::Result<Vec<GrokRule>> {
    let all_rules = support_rules.iter().chain(match_rules.iter());
    let aliases: BTreeMap<KeyString, String> = all_rules
        .map(|rule| (rule.name.clone().into(), rule.rule.clone()))
        .collect();

    // We call every rule separate to filter out the ones that are not valid.
    let mut rules = Vec::new();
    for rule in match_rules.iter() {
        let parsed_rule =
            parse_grok_rules::parse_grok_rules(&[rule.rule.to_string()], aliases.clone());
        match parsed_rule {
            Ok(parsed) => rules.extend_from_slice(&parsed),
            Err(err) => {
                warn!("failed to parse grok rule {}: {}", rule.rule, err);
            }
        }
    }

    if rules.is_empty() {
        return Err(PipelineError::GrokParse {
            message: "No valid grok rules found".to_string(),
        });
    }
    Ok(rules)
}

fn build_grok_rules_with_source(rules: &[OPGrokRules]) -> SourceToGrokPatterns {
    rules
        .iter()
        .filter_map(|rule| {
            build_grok_rules(&rule.support_rules, &rule.match_rules)
                .map(|parsed_rules| (rule.source.to_owned(), parsed_rules))
                .ok()
        })
        .collect()
}

#[cfg(test)]
pub mod tests {

    use vrl::datadog_grok::parse_grok::parse_grok;

    use super::*;

    #[test]
    fn test_simple_grok_rules_deserialization() {
        let json_data = r#"
        {
            "supportRules": "_timestamp %{date(\"yyyy-MM-dd\")}\n_context %{notSpace}\n",
            "matchRules": "mongo.test1 %{_timestamp}\nmongo.test2 %{_context}\n"
        }
        "#;

        let rules: LogsProcessingGrokRules =
            serde_json::from_str(json_data).expect("Failed to parse GrokRules JSON");

        // Validate support rules.
        assert_eq!(rules.support_rules.len(), 2);
        assert_eq!(rules.support_rules[0].name, "_timestamp");
        assert_eq!(rules.support_rules[0].rule, "%{date(\"yyyy-MM-dd\")}");
        assert_eq!(rules.support_rules[1].name, "_context");
        assert_eq!(rules.support_rules[1].rule, "%{notSpace}");

        // Validate match rules.
        assert_eq!(rules.match_rules.len(), 2);
        assert_eq!(rules.match_rules[0].name, "mongo.test1");
        assert_eq!(rules.match_rules[0].rule, "%{_timestamp}");
        assert_eq!(rules.match_rules[1].name, "mongo.test2");
        assert_eq!(rules.match_rules[1].rule, "%{_context}");
    }

    #[test]
    fn test_simple_grok_rules_no_support_rules() {
        let json_data = r#"
        {
            "matchRules": "mongo.test1 %{_timestamp}\nmongo.test2 %{_context}\n"
        }
        "#;

        let rules: LogsProcessingGrokRules =
            serde_json::from_str(json_data).expect("Failed to parse GrokRules JSON");

        // Validate support rules.
        assert_eq!(rules.support_rules.len(), 0);

        // Validate match rules.
        assert_eq!(rules.match_rules.len(), 2);
        assert_eq!(rules.match_rules[0].name, "mongo.test1");
        assert_eq!(rules.match_rules[0].rule, "%{_timestamp}");
        assert_eq!(rules.match_rules[1].name, "mongo.test2");
        assert_eq!(rules.match_rules[1].rule, "%{_context}");
    }

    #[test]
    fn test_simple_grok_rules_support_rules_null() {
        let json_data = r#"
        {
            "supportRules": null,
            "matchRules": "mongo.test1 %{_timestamp}\nmongo.test2 %{_context}\n"
        }
        "#;

        let rules: LogsProcessingGrokRules =
            serde_json::from_str(json_data).expect("Failed to parse GrokRules JSON");

        // Validate support rules.
        assert_eq!(rules.support_rules.len(), 0);

        // Validate match rules.
        assert_eq!(rules.match_rules.len(), 2);
        assert_eq!(rules.match_rules[0].name, "mongo.test1");
        assert_eq!(rules.match_rules[0].rule, "%{_timestamp}");
        assert_eq!(rules.match_rules[1].name, "mongo.test2");
        assert_eq!(rules.match_rules[1].rule, "%{_context}");
    }

    #[test]
    fn test_vrl_grok_parser_datadog_agent() {
        let results = r#"[   
            {
                "sample" : "2020-07-01 09:48:14 UTC | CORE | INFO | (pkg/collector/runner/runner.go:327 in work) | check:network,type:core | Done running check",
                "result" : {
                    "agent" : "CORE",
                    "process" : "work",
                    "filename" : "pkg/collector/runner/runner.go",
                    "lineno" : 327.0,
                    "level" : "INFO",
                    "check" : "network",
                    "type" : "core",
                    "timestamp" : 1593596894000
                }
            }, {
                "sample" : "2020-09-15 10:00:07 UTC | CORE | INFO | (pkg/collector/python/datadog_agent.go:120 in LogMessage) | kafka_cluster_status:8ca7b736f0aa43e5 | (kafka_cluster_status.py:213) | Checking for out of sync partition replicas",
                "result" : {
                    "agent" : "CORE",
                    "process" : "LogMessage",
                    "filename" : "pkg/collector/python/datadog_agent.go",
                    "lineno" : 120.0,
                    "level" : "INFO",
                    "pyFilename" : "kafka_cluster_status.py",
                    "kafka_cluster_status" : "8ca7b736f0aa43e5",
                    "pyLineno" : 213.0,
                    "timestamp" : 1600164007000
                }
            }, {
                "sample" : "2019-04-08 13:53:48 UTC | TRACE | INFO | (pkg/trace/agent/agent.go:145 in loop) | exiting",
                "result" : {
                    "agent" : "TRACE",
                    "process" : "loop",
                    "filename" : "pkg/trace/agent/agent.go",
                    "lineno" : 145.0,
                    "level" : "INFO",
                    "timestamp" : 1554731628000
                }
            }, {
                "sample" : "2019-02-01 16:59:41 UTC | INFO | (connection_manager.go:124 in CloseConnection) | Connection closed",
                "result" : {
                    "process" : "CloseConnection",
                    "filename" : "connection_manager.go",
                    "lineno" : 124.0,
                    "level" : "INFO",
                    "timestamp" : 1549040381000
                }
            }, {
                "sample" : "2020-11-18 10:31:13 UTC | JMX | INFO  | App | Successfully initialized instance: cassandra-localhost-7199",
                "result" : {
                    "agent" : "JMX",
                    "level" : "INFO",
                    "class" : "App",
                    "timestamp" : 1605695473000
                }
        }]"#;
        let results: Vec<serde_json::Value> = serde_json::from_str(results).unwrap();

        #[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
        struct LogsProcessingGrokRulesDeser {
            // Use a custom deserializer to transform the multiline string into Vec<Rule>
            #[serde(alias = "supportRules")]
            #[serde(default)]
            pub support_rules: Vec<Rule>,
            #[serde(alias = "matchRules")]
            pub match_rules: Vec<Rule>,
        }
        let grok_rules = r#"{
                "supportRules": [],
                "matchRules": [
                    {
                        "name": "agent_rule",
                        "rule": "%{date(\"yyyy-MM-dd HH:mm:ss z\"):timestamp} \\| %{notSpace:agent} \\| %{word:level} \\| \\(%{notSpace:filename}:%{number:lineno} in %{word:process}\\) \\|( %{data::keyvalue(\":\")} \\|)?( - \\|)?( \\(%{notSpace:pyFilename}:%{number:pyLineno}\\) \\|)?%{data}"
                    },
                    {
                        "name": "agent_rule_pre_611",
                        "rule": "%{date(\"yyyy-MM-dd HH:mm:ss z\"):timestamp} \\| %{word:level} \\| \\(%{notSpace:filename}:%{number:lineno} in %{word:process}\\)%{data}"
                    },
                    {
                        "name": "jmxfetch_rule",
                        "rule": "%{date(\"yyyy-MM-dd HH:mm:ss z\"):timestamp} \\| %{notSpace:agent} \\| %{word:level}\\s+\\| %{word:class} \\| %{data}"
                    }
                ]
        }"#;
        let grok_rules_tmp: LogsProcessingGrokRulesDeser =
            serde_json::from_str(grok_rules).unwrap();
        let grok_rules = LogsProcessingGrokRules {
            support_rules: grok_rules_tmp.support_rules,
            match_rules: grok_rules_tmp.match_rules,
        };

        let parsed_rules = build_grok_rules(&grok_rules.support_rules, &grok_rules.match_rules)
            .expect("Failed to parse grok rules");

        // Validate the parsed rules
        assert_eq!(parsed_rules.len(), 3);

        // Validate the parsed results
        for (i, result) in results.iter().enumerate() {
            let sample = result["sample"].as_str().unwrap();
            let mut expected_result = result["result"].clone();

            let result = parse_grok(sample, &parsed_rules).unwrap();
            let mut json_val =
                serde_json::to_value(result.parsed).expect("Failed to convert to JSON");

            normalize_numbers(&mut expected_result);
            normalize_numbers(&mut json_val);

            assert!(json_val.is_object(), "Grok parser should return an object");
            assert_eq!(
                json_val, expected_result,
                "Sample {}: Expected {:?}, got {:?}",
                i, expected_result, json_val
            );
        }
    }
    /// Convert numbers to f64 recursively
    fn normalize_numbers_in_obj(value: &mut serde_json::Map<String, serde_json::Value>) {
        for val in value.values_mut() {
            normalize_numbers(val);
        }
    }
    /// Convert numbers to f64 recursively
    pub fn normalize_numbers(value: &mut Value) {
        match value {
            Value::Number(n) => {
                if let Some(val) = n.as_u64() {
                    *value = Value::Number(serde_json::Number::from_f64(val as f64).unwrap());
                } else if let Some(val) = n.as_i64() {
                    *value = Value::Number(serde_json::Number::from_f64(val as f64).unwrap());
                }
            }
            Value::Array(arr) => {
                for item in arr {
                    normalize_numbers(item);
                }
            }
            Value::Object(map) => {
                normalize_numbers_in_obj(map);
            }
            Value::Null | Value::Bool(_) | Value::String(_) => {}
        }
    }
}
