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

use core::fmt;
use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::{Deserialize, Deserializer, Serialize, de};
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

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct Rule {
    pub name: String,
    pub rule: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Sample {
    pub sample: String,
    pub result: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct LogsProcessingGrokRules {
    // Use a custom deserializer to transform the multiline string into Vec<Rule>
    #[serde(alias = "supportRules")]
    #[serde(deserialize_with = "parse_rules")]
    #[serde(default)]
    pub support_rules: Vec<Rule>,
    #[serde(alias = "matchRules")]
    #[serde(deserialize_with = "parse_rules")]
    pub match_rules: Vec<Rule>,
}

/// Custom deserializer for fields that are multiline rules strings, null, or arrays of Rule
/// objects.
pub fn parse_rules<'de, D>(deserializer: D) -> Result<Vec<Rule>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(RulesVisitor)
}

/// This parses the rules from the logs processing format, which is
/// {RULE_NAME} {RULE}\n
pub fn parse_rules_from_str(rules_str: &str) -> Vec<Rule> {
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

struct RulesVisitor;

impl<'de> serde::de::Visitor<'de> for RulesVisitor {
    type Value = Vec<Rule>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a multiline string or a Rule object or an array of Rule objects")
    }

    // For multiline string
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(parse_rules_from_str(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(parse_rules_from_str(&v))
    }

    // Single object deserialization
    fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
    where
        M: de::MapAccess<'de>,
    {
        let rule = Rule::deserialize(de::value::MapAccessDeserializer::new(map))?;
        Ok(vec![rule])
    }

    // Array of objects deserialization
    fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
    where
        S: de::SeqAccess<'de>,
    {
        let mut rules = Vec::new();
        while let Some(rule) = seq.next_element()? {
            rules.push(rule);
        }
        Ok(rules)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Vec::new())
    }
}

pub(crate) fn build_grok_rules(
    support_rules: &[Rule],
    match_rules: &[Rule],
) -> crate::Result<Vec<GrokRule>> {
    let mut support_rules: Vec<Rule> = support_rules.to_vec();
    let mut match_rules: Vec<Rule> = match_rules.to_vec();
    normalize_grok_rules(&mut support_rules);
    normalize_grok_rules(&mut match_rules);

    let all_rules = support_rules
        .iter()
        .chain(match_rules.iter())
        .cloned()
        .collect::<Vec<_>>();
    let aliases: BTreeMap<KeyString, String> = all_rules
        .iter()
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

/// Legacy grok rules fix:
/// Normalize grok rules by replacing: %{?> with %{
/// This is necessary to ensure the rule can be parsed by the grok parser.
///
/// The syntax does not allow regex after %{, it needs to start with IDENTIFIER
/// This kind of grammar should not be used in the grok rules, but it is.
///
/// The Java Backend ignores errors in the lexer to handle this kind of rules.
/// <https://github.com/DataDog/logs-backend/blob/9b494d3875c917607bdfd460409096673fcef49d/domains/event-platform/libs/processing/processing-parsing/src/main/java/com/fsmatic/shared/parse/grok/GrokInterpreter.java#L51>
///
/// OP doesn't support this syntax.
///
/// NOTE: This is a targeted workaround. Since the grok rules are not validated when being added to
/// the integrations repositories, other invalid rules may be introduced in the future.
pub fn normalize_grok_rules(rules: &mut [Rule]) {
    for rule in rules.iter_mut() {
        // Replace %{?> with %{ to fix the invalid legacy grok rules.
        rule.rule = rule.rule.replace("%{?>", "%{");
    }
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

    use serde_json::json;
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
    fn test_simple_grok_rules_support_rules_as_json() {
        let json_data = r#"
        {
            "supportRules": null,
            "matchRules": [{
                "name":"mongo.test1", 
                "rule": "%{_timestamp}"
            }]
        }
        "#;

        let rules: LogsProcessingGrokRules =
            serde_json::from_str(json_data).expect("Failed to parse GrokRules JSON");

        // Validate support rules.
        assert_eq!(rules.support_rules.len(), 0);

        // Validate match rules.
        assert_eq!(rules.match_rules.len(), 1);
        assert_eq!(rules.match_rules[0].name, "mongo.test1");
        assert_eq!(rules.match_rules[0].rule, "%{_timestamp}");
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
        let grok_rules: LogsProcessingGrokRules = serde_json::from_str(grok_rules).unwrap();

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
                "Sample {i}: Expected {expected_result:?}, got {json_val:?}"
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

    #[test]
    // Make sure we can handle the invalid grok syntax in this rule:
    // %{?>notSpace:db.severity}
    // (Used in mongodb and some other rules)
    fn test_legacy_rule_fix() {
        let grok_rules = json!({
          "support_rules": [
            { "name": "_timestamp", "rule": "%{date(\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"):db.date}" },
            { "name": "_severity", "rule": "%{?>notSpace:db.severity}" },
          ],
          "match_rules": [
            {
              "name": "mongo.accepted_connections",
              "rule": "%{_timestamp} %{_severity}"
            }
          ]
        });

        let grok_rules: LogsProcessingGrokRules = serde_json::from_value(grok_rules).unwrap();
        let parsed_rules = build_grok_rules(&grok_rules.support_rules, &grok_rules.match_rules)
            .expect("Failed to parse grok rules");

        let sample = "2016-11-29T16:19:27.663+0000 INFO";
        let parsed = parse_grok(sample, &parsed_rules).expect("Failed to parse grok");
        let actual = serde_json::to_value(parsed.parsed).expect("Failed to convert to JSON");
        assert!(actual.is_object(), "Grok parser should return an object");
        assert_eq!(actual["db"]["date"], 1480436367663i64);
        assert_eq!(actual["db"]["severity"], "INFO");
    }

    #[test]
    // Make sure the grok parser can parse MongoDB logs (only partially currently)
    //
    fn integration_test_vrl_grok_parser_mongodb() {
        let results = r#"[   
        {
            "sample": "2016-11-29T16:19:27.663+0000 [conn118457] command logs.$cmd command: findAndModify { findandmodify: \"alert_events\", query: { date: new Date(1480430820000), scope: \"prod_102\", alertId: ObjectId('5624ca7de8e3f50a009a1a3a'), type: \"open\", breached: \"/cart/select-payment-method.html\" }, sort: { _id: 1 }, new: 1, remove: 0, upsert: 1, update: { $set: { date: new Date(1480430820000), scope: \"prod_102\", alertId: ObjectId('5624ca7de8e3f50a009a1a3a'), type: \"open\", breached: \"/cart/select-payment-method.html\", value: 5, computeDate: new Date(1480436348272) } } } keyUpdates:0 numYields:0 locks(micros) w:245033 reslen:340 245ms",
            "result": {
                "db": {
                    "date": 1480436367663,
                    "instance": "logs",
                    "operation": "command",
                    "statement": "{ findandmodify: \"alert_events\", query: { date: new Date(1480430820000), scope: \"prod_102\", alertId: ObjectId('5624ca7de8e3f50a009a1a3a'), type: \"open\", breached: \"/cart/select-payment-method.html\" }, sort: { _id: 1 }, new: 1, remove: 0, upsert: 1, update: { $set: { date: new Date(1480430820000), scope: \"prod_102\", alertId: ObjectId('5624ca7de8e3f50a009a1a3a'), type: \"open\", breached: \"/cart/select-payment-method.html\", value: 5, computeDate: new Date(1480436348272) } } }"
                },
                "duration": 245000000,
                "mongo": {
                    "context": "conn118457",
                    "counters": {
                        "keyUpdates": 0,
                        "numYields": 0,
                        "reslen": 340,
                        "w": 245033
                    },
                    "query": {
                        "type": "findAndModify"
                    }
                }
            }
        }]"#;

        let results: Vec<serde_json::Value> = serde_json::from_str(results).unwrap();

        let grok_rules = json!({
          "support_rules": [
            { "name": "_timestamp", "rule": "%{date(\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"):db.date}" },
            { "name": "_severity", "rule": "%{?>notSpace:db.severity}" },
            { "name": "_context", "rule": "\\[%{notSpace:mongo.context}\\]" },
            { "name": "_client_ip", "rule": "%{ipOrHost:network.client.ip}" },
            { "name": "_client_port", "rule": "%{integer:network.client.port}" },
            { "name": "_connection_id", "rule": "\\#%{integer:mongo.connectionId}" },
            { "name": "_operation", "rule": "%{notSpace:mongo.operation}" },
            { "name": "_namespace", "rule": "%{notSpace:mongo.namespace}" },
            { "name": "_database", "rule": "%{regex(\"[^/\\\\\\\\.\\\\s\\\"$]+\"):db.instance}" },
            { "name": "_collection", "rule": "%{notSpace:mongo.collection}" },
            { "name": "_query_type", "rule": "%{data:mongo.query.type}" },
            { "name": "_counters", "rule": "%{data:mongo.counters:keyvalue(\":\")}" },
            { "name": "_duration", "rule": "%{integer:duration:scale(1000000)}" },
            { "name": "_raw_query", "rule": "%{regex(\"\\\\{.*\\\\}\"):db.statement}" },
            { "name": "_raw_update", "rule": "%{regex(\"\\\\{.*\\\\}\"):mongo.update.raw}" },
            { "name": "_raw_query_in_db", "rule": "%{regex(\"\\\\{.*db: \\\\S+ \\\\}\"):db.statement}" },
            { "name": "_plan_summary", "rule": "%{notSpace:mongo.planSummary.type}( %{regex(\"\\\\{.*?\\\\}\"):mongo.planSummary.params})?" },
            { "name": "_app_name", "rule": "%{regex(\"[\\\\w\\\\s-_.]*\"):mongo.appName}" }
          ],
          "match_rules": [
            {
              "name": "mongo.accepted_connections",
              "rule": "%{_timestamp} %{_severity}\\s+%{notSpace}\\s+%{_context} connection accepted from %{_client_ip}:%{_client_port} .*"
            },
            {
              "name": "mongo.end_connections",
              "rule": "%{_timestamp} %{_severity}\\s+%{notSpace}\\s+%{_context} end connection %{_client_ip}:%{_client_port} .*"
            },
            {
              "name": "mongo.query",
              "rule": "%{_timestamp} %{_severity}\\s+%{regex(\"QUERY\"):db.operation}\\s+%{_context} %{data} query: %{_raw_query} %{_counters}"
            },
            {
              "name": "mongo.update",
              "rule": "%{_timestamp} %{_severity}\\s+%{notSpace}\\s+%{_context} %{regex(\"update\"):db.operation} %{_database}\\.%{_collection} query: %{_raw_query} planSummary: %{_plan_summary} update: %{_raw_update} keysExamined:%{number} %{_counters} %{_duration}ms"
            },
            {
              "name": "mongo.command.with_plan",
              "rule": "%{_timestamp} %{_severity}\\s+%{notSpace}\\s+%{_context} %{word:db.operation} %{_database}(\\.)?(\\$cmd|%{_collection})? command: %{_query_type} %{_raw_query} planSummary: %{_plan_summary} %{_counters} %{_duration}ms"
            },
            {
              "name": "mongo.command.in_db",
              "rule": "%{_timestamp} %{_severity}\\s+%{notSpace}\\s+%{_context} %{word:db.operation} %{_database}(\\.)?(\\$cmd|%{_collection})? command: %{_query_type} %{_raw_query_in_db} %{_counters} %{_duration}ms"
            },
            {
              "name": "mongo.command",
              "rule": "%{_timestamp} %{_severity}\\s+%{notSpace}\\s+%{_context} %{word:db.operation} %{_database}(\\.)?(\\$cmd|%{_collection})?( appName: \"%{_app_name}\")? command: %{_query_type} %{_raw_query} %{_counters} %{_duration}ms"
            },
            {
              "name": "rule_default",
              "rule": "%{_timestamp}\\s*(%{_severity}\\s+%{word:db.operation})?\\s*%{_context}\\s*%{data}"
            },
            {
              "name": "#Extra",
              "rule": "samples:"
            },
            {
              "name": "#2019-05-09T13:18:48.741+0000",
              "rule": "I COMMAND  [conn26] command db.collection command: find { find: \"collection\", filter: { scope: \"1\" }, sort: { discoveryHour: -1 }, limit: 1, singleBatch: true, $readPreference: { mode: \"secondaryPreferred\" }, $db: \"logs\" } planSummary: IXSCAN { scope: 1, discoveryHour: 1 } keysExamined:1 docsExamined:1 cursorExhausted:1 numYields:1 nreturned:1 reslen:188 locks:{ Global: { acquireCount: { r: 4 } }, Database: { acquireCount: { r: 2 } }, Collection: { acquireCount: { r: 2 } } } protocol:op_query 284ms"
            }
          ]
        });

        let grok_rules: LogsProcessingGrokRules = serde_json::from_value(grok_rules).unwrap();

        let parsed_rules = build_grok_rules(&grok_rules.support_rules, &grok_rules.match_rules)
            .expect("Failed to parse grok rules");

        assert_eq!(parsed_rules.len(), 9);

        let mut num_match = 0;
        let mut partial_match = 0;
        for result in results.iter() {
            let sample = result["sample"].as_str().unwrap();
            let mut expected = result["result"].clone();

            let parsed = parse_grok(sample, &parsed_rules).expect("Failed to parse grok");
            let mut actual =
                serde_json::to_value(parsed.parsed).expect("Failed to convert to JSON");

            normalize_numbers(&mut expected);
            normalize_numbers(&mut actual);

            assert!(actual.is_object(), "Grok parser should return an object");
            if actual != expected {
                // Track if we get an object, but not exactly the expected one
                if !actual.as_object().unwrap().is_empty() {
                    partial_match += 1;
                }
                println!(
                    "Sample {}\nExpected {}\ngot {}",
                    sample,
                    serde_json::to_string(&expected).unwrap(),
                    serde_json::to_string(&actual).unwrap()
                );
            } else {
                num_match += 1;
            }
        }
        assert_eq!(num_match, 0);
        assert_eq!(partial_match, 1);
    }
}
