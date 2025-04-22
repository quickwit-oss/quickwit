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
use vrl::datadog_grok::parse_grok_rules::{self, GrokRule};
use vrl::value::KeyString;

use crate::error::PipelineError;

static OP_GROK_RULES: OnceLock<SourceToGrokPatterns> = OnceLock::new();
pub(super) type SourceToGrokPatterns = BTreeMap<String, Vec<GrokRule>>;

pub fn get_grok_rules_by_source() -> &'static SourceToGrokPatterns {
    OP_GROK_RULES.get_or_init(|| {
        // The path is relative to this file.
        let json_str = include_str!("rules.json");
        let op_grok_rules: Vec<OPGrokRules> =
            serde_json::from_str(json_str).expect("Failed to parse JSON");
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
#[serde(rename_all = "camelCase")]
pub struct LogsProcessingGrokRules {
    // Use a custom deserializer to transform the multiline string into Vec<Rule>
    #[serde(deserialize_with = "parse_rules_from_str")]
    pub support_rules: Vec<Rule>,
    #[serde(deserialize_with = "parse_rules_from_str")]
    pub match_rules: Vec<Rule>,
}

/// Custom deserializer for fields that are multiline rules strings.
pub fn parse_rules_from_str<'de, D>(deserializer: D) -> Result<Vec<Rule>, D::Error>
where D: Deserializer<'de> {
    let s: String = Deserialize::deserialize(deserializer)?;
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
                    name: name.to_string(),
                    rule: rule.to_string(),
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

    let patterns: Vec<String> = match_rules
        .iter()
        .map(|match_rule| match_rule.rule.to_owned())
        .collect();

    parse_grok_rules::parse_grok_rules(&patterns, aliases)
        .map_err(|source| PipelineError::GrokCompile { source })
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
mod tests {
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
}
